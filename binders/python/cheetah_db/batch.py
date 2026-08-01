"""The ``BATCH`` command: run one command N times in one round trip.

Cheetah's bottleneck under bulk work is the round trip, not the disk. ``BATCH``
is the server's general answer to it — one command name, any target command::

    BATCH <COMMAND> items=<base64 JSON> [continue_on_error=] [results=] [async=]

An item is one of three shapes, and this module builds all three::

    "ctx:BERLIN 42"          a raw argument line
    ["ctx:BERLIN", 42]       positional arguments
    {"from": "a", "to": "b"} the key=value dialect, over the shared modifiers

It is **not** a transaction: items apply in order and independently, so the
response always carries ``applied``/``failed`` rather than collapsing to one
ERROR. With ``results=1`` (the default) every item's own response line comes
back in ``payload=``.

Two ways to run one:

  - **inline** — :func:`run_batch`, one line out, one line back.
  - **detached** — :func:`run_batch_async` submits it as a job and streams the
    item responses back with ``JOB results`` *while it runs*, then closes with
    the aggregate.

And a third that asks nothing of the caller: :class:`BatchCollector` queues
commands and sends them as one ``BATCH``, handing each caller a
:class:`DeferredResponse` that resolves on first use. :class:`CheetahClient`
drives one automatically once a command looks like bulk work — see
:class:`AutoBatchPolicy` and ``CheetahClient(auto_batch=…)``.

A note on why Python's automatic mode is not Node's. The Node binder can
coalesce transparently because its calls are promises: several are outstanding
at once and folding them changes nothing observable. A synchronous client has
exactly one command in flight per thread, so the only way to coalesce is to
*defer* the response — which moves the moment an error surfaces. That is a real
trade, so the default policy here only reports the opportunity
(``mode="advise"``); ``mode="deferred"`` opts into taking it.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Mapping, Sequence

from .protocol import Response, parse_response

__all__ = [
    "AUTO_BATCH_EXCLUDED",
    "BATCH_MAX_ITEMS",
    "JOB_RESULTS_MAX_PAGE",
    "NON_BATCHABLE",
    "AutoBatchPolicy",
    "BatchCollector",
    "BatchResult",
    "CheetahBatchError",
    "DeferredResponse",
    "batch",
    "build_batch",
    "decode_result_lines",
    "parse_batch_response",
    "run_batch",
    "run_batch_async",
    "run_batch_chunked",
    "split_command_line",
]

logger = logging.getLogger(__name__)

#: Server-side cap on the items of one request (``src/batch.go``).
BATCH_MAX_ITEMS = 10000

#: Server-side cap on one ``JOB results`` page (``src/micro_job.go``).
JOB_RESULTS_MAX_PAGE = 1000

#: Commands ``BATCH`` refuses as a target, mirrored so the client fails before
#: the round trip. ``BATCH``/``JOB`` would recurse; the other three are
#: connection-scoped and never reach the dispatcher at all.
NON_BATCHABLE = ("BATCH", "JOB", "DATABASE", "RESET_DB", "EXIT", "QUIT")

#: Commands automatic batching leaves alone on top of :data:`NON_BATCHABLE`.
#: Not *wrong* to batch — simply never the shape auto-batching exists for.
AUTO_BATCH_EXCLUDED = NON_BATCHABLE + (
    "LOG_FLUSH",
    "SYSTEM_STATS",
    "FILE_CHECKPOINT",
    "CLUSTER_UPDATE",
    "CLUSTER_GOSSIP",
    "CLUSTER_MOVE",
    "FORK_ASSIGN",
)


class CheetahBatchError(RuntimeError):
    """Raised when a ``BATCH`` request is malformed or the server refused it."""

    def __init__(self, message: str, *, command: str | None = None, response: Response | None = None) -> None:
        super().__init__(message)
        self.command = command
        self.response = response


# --------------------------------------------------------------------------- #
# Building
# --------------------------------------------------------------------------- #
def split_command_line(line: str) -> tuple[str, str]:
    """Split a raw command line into ``(COMMAND, argument line)``."""
    head, sep, rest = str(line).partition(" ")
    return head.upper(), rest if sep else ""


def _assert_batchable(command: str) -> str:
    name = str(command or "").strip().upper()
    if not name:
        raise CheetahBatchError("cheetah BATCH requires a target command")
    base = name.split(":", 1)[0]
    if base in NON_BATCHABLE:
        raise CheetahBatchError(f"cheetah BATCH cannot target {base}")
    return name


def _encode_items(items: Iterable[Any]) -> str:
    listed = list(items or [])
    if not listed:
        raise CheetahBatchError("cheetah BATCH requires at least one item")
    if len(listed) > BATCH_MAX_ITEMS:
        raise CheetahBatchError(
            f"cheetah BATCH accepts at most {BATCH_MAX_ITEMS} items (got {len(listed)})"
        )
    encoded = json.dumps(listed, separators=(",", ":"), ensure_ascii=False)
    return base64.b64encode(encoded.encode("utf-8")).decode("ascii")


def build_batch(
    command: str,
    items: Iterable[Any],
    *,
    continue_on_error: bool = True,
    results: bool = True,
    detached: bool = False,
    shared: Mapping[str, Any] | None = None,
) -> str:
    """Build one ``BATCH`` line.

    ``shared`` are modifiers every *object* item inherits — ``{"type": "knows"}``
    writes the edge type once instead of ten thousand times. Raw-string and
    array items carry their own arguments and ignore it, which is the only rule
    that lets one command serve both dialects.
    """
    parts = [f"BATCH {_assert_batchable(command)}", f"items={_encode_items(items)}"]
    # The server defaults to stop-on-error, while this binder deliberately
    # defaults to continuing. Always spell the flag so either caller choice
    # survives the wire unchanged.
    parts.append(f"continue_on_error={1 if continue_on_error else 0}")
    if not results:
        parts.append("results=0")
    if detached:
        parts.append("async=1")
    for key, value in (shared or {}).items():
        if value is None or value == "":
            continue
        rendered = ("1" if value else "0") if isinstance(value, bool) else str(value)
        if any(char.isspace() for char in rendered):
            raise CheetahBatchError(
                f"cheetah BATCH shared modifier {key}= must not contain whitespace"
            )
        parts.append(f"{key}={rendered}")
    return " ".join(parts)


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def decode_result_lines(fields: Mapping[str, str] | None) -> list[str | None]:
    """Decode the ``payload=`` of a ``BATCH`` (or ``JOB results``) response.

    The lines are JSON strings, so they must be valid UTF-8. When a command
    answers with bytes that are not — ``READ`` of a binary payload is the only
    real case — the server switches the whole array to base64 and says so in
    ``results_encoding=``; decoding it as latin-1 then gives back the exact
    bytes that were on the wire.
    """
    payload = (fields or {}).get("payload")
    if not payload:
        return []
    try:
        decoded = json.loads(base64.b64decode(payload).decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise CheetahBatchError(f"cheetah BATCH payload is not decodable: {exc}") from exc
    if not isinstance(decoded, list):
        return []
    base64_lines = (fields or {}).get("results_encoding") == "base64"
    lines: list[str | None] = []
    for entry in decoded:
        if entry is None:
            lines.append(None)
        elif base64_lines:
            lines.append(base64.b64decode(str(entry)).decode("latin1"))
        else:
            lines.append(str(entry))
    return lines


@dataclass
class BatchResult:
    """The aggregate of one ``BATCH``, plus each item's own response."""

    target: str
    requested: int
    applied: int
    failed: int
    first_error: str | None
    lines: list[str | None] = field(default_factory=list)
    results: list[Response | None] = field(default_factory=list)
    response: Response | None = None

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def raise_for_failures(self) -> "BatchResult":
        """Raise unless every item applied. A half-written batch is a hole."""
        if self.failed:
            raise CheetahBatchError(
                f"cheetah BATCH {self.target} applied {self.applied}/{self.requested} "
                f"(failed={self.failed}): {self.first_error or 'no reason reported'}",
                response=self.response,
            )
        return self


def parse_batch_response(response: Response, *, target: str | None = None) -> BatchResult:
    lines = decode_result_lines(response.fields)
    return BatchResult(
        target=response.field_value("target", target) or (target or ""),
        requested=int(response.int_field("requested", 0) or 0),
        applied=int(response.int_field("applied", 0) or 0),
        failed=int(response.int_field("failed", 0) or 0),
        first_error=response.field_value("first_error"),
        lines=lines,
        results=[None if line is None else parse_response(line) for line in lines],
        response=response,
    )


# --------------------------------------------------------------------------- #
# Inline
# --------------------------------------------------------------------------- #
def run_batch(conn: Any, command: str, items: Iterable[Any], **options: Any) -> BatchResult:
    """Send one ``BATCH`` and return its parsed aggregate."""
    line = build_batch(command, items, **options)
    response = conn.send(line)
    if not response.ok:
        raise CheetahBatchError(
            f"cheetah BATCH {command} failed: {response.reason}", command=line, response=response
        )
    return parse_batch_response(response, target=command)


def run_batch_chunked(
    conn: Any,
    command: str,
    items: Sequence[Any],
    *,
    chunk_size: int = 1000,
    **options: Any,
) -> BatchResult:
    """:func:`run_batch` split into ``chunk_size`` requests, counts merged.

    The server caps one request at :data:`BATCH_MAX_ITEMS`; above that the split
    is mandatory. Below it, it is a choice about failure granularity — the
    command is not a transaction, so two chunks can leave a half-applied list
    either way.
    """
    listed = list(items or [])
    size = max(1, min(int(chunk_size), BATCH_MAX_ITEMS))
    totals = BatchResult(target=command, requested=0, applied=0, failed=0, first_error=None)
    for start in range(0, len(listed), size):
        page = run_batch(conn, command, listed[start : start + size], **options)
        totals.requested += page.requested
        totals.applied += page.applied
        totals.failed += page.failed
        totals.first_error = totals.first_error or page.first_error
        totals.lines.extend(page.lines)
        totals.results.extend(page.results)
    return totals


# --------------------------------------------------------------------------- #
# Detached
# --------------------------------------------------------------------------- #
def run_batch_async(
    conn: Any,
    command: str,
    items: Iterable[Any],
    *,
    on_result: Callable[[Response, int], None] | None = None,
    on_progress: Callable[[Any], None] | None = None,
    poll_interval: float = 0.25,
    timeout: float | None = None,
    page_size: int = JOB_RESULTS_MAX_PAGE,
    sleep: Callable[[float], None] = time.sleep,
    **options: Any,
) -> BatchResult:
    """Submit a ``BATCH`` as a job and follow it to the end.

    ``on_result(response, index)`` — when given — receives every item's response
    as soon as the server has it, not at the end: that is the whole point of the
    detached form, and it is why the poll loop reads ``JOB results`` before it
    decides whether the job is done.
    """
    from . import jobs as jobs_module

    line = build_batch(command, items, detached=True, **options)
    submitted = conn.send(line)
    if not submitted.ok:
        raise CheetahBatchError(
            f"cheetah BATCH {command} submit failed: {submitted.reason}",
            command=line,
            response=submitted,
        )
    job_id = submitted.field_value("job")
    if not job_id:
        raise CheetahBatchError(
            f"cheetah BATCH returned no job id: {submitted.raw}", response=submitted
        )

    deadline = None if timeout is None else time.monotonic() + timeout
    consumed = 0

    def drain() -> int:
        nonlocal consumed
        if on_result is None:
            return consumed
        while True:
            page = jobs_module.results(conn, job_id, from_index=consumed, limit=page_size)
            if not page.lines:
                return consumed
            for offset, text in enumerate(page.lines):
                if text is not None:
                    on_result(parse_response(text), consumed + offset)
            consumed = page.next

    while True:
        drain()
        snapshot = jobs_module.status(conn, job_id)
        if on_progress is not None:
            on_progress(snapshot)
        if snapshot.finished:
            # One last sweep: the items produced between the page read above and
            # the job's own completion would otherwise never be delivered.
            drain()
            result = jobs_module.fetch(conn, job_id)
            if result is not None:
                return parse_batch_response(result, target=command)
        if deadline is not None and time.monotonic() >= deadline:
            raise CheetahBatchError(f"cheetah BATCH job {job_id} did not finish within {timeout}s")
        sleep(max(0.01, poll_interval))


def batch(
    conn: Any,
    command: str,
    items: Sequence[Any],
    *,
    detached: bool | None = None,
    detach_threshold: int = 2000,
    **options: Any,
) -> BatchResult:
    """:func:`run_batch` or :func:`run_batch_async`, chosen by size.

    ``detached=None`` (the default) detaches above ``detach_threshold`` items,
    because past that the batch is long enough that holding the socket open for
    it costs more than the extra round trips of polling.
    """
    listed = list(items or [])
    if detached is True or (detached is None and len(listed) >= detach_threshold):
        return run_batch_async(conn, command, listed, **options)
    if len(listed) > BATCH_MAX_ITEMS:
        return run_batch_chunked(conn, command, listed, **options)
    return run_batch(conn, command, listed, **options)


# --------------------------------------------------------------------------- #
# Automatic
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AutoBatchPolicy:
    """When and how a client coalesces commands on its own.

    ``mode``:

      ``"off"``       never coalesce. The pre-batching wire behavior, exactly.
      ``"advise"``    (default) watch, and log once per command when it goes hot
                      enough to be worth batching. Nothing about the wire, the
                      ordering or the moment an error surfaces changes.
      ``"deferred"``  coalesce for real: a hot command is queued and the caller
                      gets a :class:`DeferredResponse` that resolves on first
                      use. Bulk loops that ignore the response get the full
                      saving; the cost is that an item's error surfaces at that
                      first use — or, for a response nobody ever reads, only in
                      ``on_error``.

    ``threshold`` calls inside ``window`` seconds make a command hot;
    ``idle`` seconds without a call cool it back down.
    """

    mode: str = "advise"
    threshold: int = 8
    window: float = 0.2
    idle: float = 2.0
    max_size: int = 256
    commands: tuple[str, ...] | None = None
    exclude: tuple[str, ...] = AUTO_BATCH_EXCLUDED
    continue_on_error: bool = True
    #: ``(command, exception) -> None`` for a failure nobody was waiting on.
    on_error: Callable[[str, Exception], None] | None = None

    def with_overrides(self, overrides: Mapping[str, Any] | None) -> "AutoBatchPolicy":
        if not overrides:
            return self
        known = {key: value for key, value in overrides.items() if key in self.__dataclass_fields__}
        unknown = set(overrides) - set(known)
        if unknown:
            raise ValueError(f"unknown auto_batch settings: {', '.join(sorted(unknown))}")
        return replace(self, **known)


class DeferredResponse:
    """A :class:`Response` that has not been sent yet.

    Touching any attribute flushes the collector it belongs to and resolves.
    A caller that never touches it never forces the flush, which is exactly the
    bulk-write loop this exists for.
    """

    __slots__ = ("_collector", "_resolved", "_error", "_line")

    def __init__(self, collector: "BatchCollector", line: str) -> None:
        self._collector = collector
        self._line = line
        self._resolved: Response | None = None
        self._error: Exception | None = None

    # -- resolution --------------------------------------------------------- #
    def resolve(self) -> Response:
        """Flush if needed and return the real response."""
        if self._resolved is None and self._error is None:
            self._collector.flush()
        if self._error is not None:
            raise self._error
        if self._resolved is None:
            raise CheetahBatchError(f"cheetah BATCH produced no response for: {self._line}")
        return self._resolved

    def _settle(self, response: Response | None, error: Exception | None) -> None:
        self._resolved = response
        self._error = error

    @property
    def pending(self) -> bool:
        return self._resolved is None and self._error is None

    # -- Response surface --------------------------------------------------- #
    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.resolve(), name)

    def __repr__(self) -> str:
        if self.pending:
            return f"DeferredResponse(pending, line={self._line!r})"
        return f"DeferredResponse({self._resolved!r})"

    def __bool__(self) -> bool:
        return bool(self.resolve().ok)


class BatchCollector:
    """Queues command lines and sends them as one ``BATCH``.

    One collector holds one command at a time: a queue that receives a different
    command flushes first, which is also what keeps ordering intact — the server
    applies a batch's items in order, and nothing that cannot join the queue is
    written before it.
    """

    def __init__(
        self,
        send: Callable[[str], Response],
        *,
        max_size: int = 256,
        continue_on_error: bool = True,
        on_error: Callable[[str, Exception], None] | None = None,
        stats: dict[str, int] | None = None,
    ) -> None:
        self._send = send
        self._max_size = max(1, int(max_size))
        self._continue_on_error = continue_on_error
        self._on_error = on_error
        self._command: str | None = None
        self._queue: list[tuple[str, DeferredResponse]] = []
        # A caller may pass its own counters in so they outlive the collector:
        # a `with client.batching():` block builds and discards one, and the
        # totals are more useful than the block's own share of them.
        self.stats = {"batched": 0, "batches": 0, "direct": 0} if stats is None else stats

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def command(self) -> str | None:
        return self._command

    def add(self, line: str) -> DeferredResponse:
        """Queue one command line; returns the handle to its future response."""
        command, args = split_command_line(line)
        if self._command is not None and self._command != command:
            self.flush()
        self._command = command
        handle = DeferredResponse(self, line)
        self._queue.append((args, handle))
        if len(self._queue) >= self._max_size:
            self.flush()
        return handle

    def flush(self) -> None:
        """Send whatever is queued. Safe to call when nothing is."""
        pending = self._queue
        command = self._command
        self._queue = []
        self._command = None
        if not pending or command is None:
            return

        if len(pending) == 1:
            # Nothing to gain: one item in a BATCH is one command plus a wrapper.
            args, handle = pending[0]
            self.stats["direct"] += 1
            self._settle_one(command, args, handle)
            return

        self.stats["batches"] += 1
        self.stats["batched"] += len(pending)
        try:
            line = build_batch(
                command,
                [args for args, _ in pending],
                continue_on_error=self._continue_on_error,
            )
            response = self._send(line)
            if not response.ok:
                raise CheetahBatchError(
                    f"cheetah BATCH {command} failed: {response.reason}",
                    command=line,
                    response=response,
                )
            parsed = parse_batch_response(response, target=command)
        except Exception as exc:  # noqa: BLE001 - every waiter must learn of it
            self._fail_all(command, pending, exc)
            return

        for index, (_, handle) in enumerate(pending):
            item = parsed.results[index] if index < len(parsed.results) else None
            if item is None:
                handle._settle(
                    None,
                    CheetahBatchError(
                        f"cheetah BATCH {command} returned no result for item {index}",
                        command=line,
                        response=response,
                    ),
                )
            else:
                handle._settle(item, None)

    def _settle_one(self, command: str, args: str, handle: DeferredResponse) -> None:
        line = f"{command} {args}" if args else command
        try:
            handle._settle(self._send(line), None)
        except Exception as exc:  # noqa: BLE001
            handle._settle(None, exc)
            self._report(command, exc)

    def _fail_all(
        self, command: str, pending: list[tuple[str, DeferredResponse]], error: Exception
    ) -> None:
        for _, handle in pending:
            handle._settle(None, error)
        self._report(command, error)

    def _report(self, command: str, error: Exception) -> None:
        """A deferred failure nobody may ever look at still has to be visible."""
        if self._on_error is not None:
            self._on_error(command, error)
        else:
            logger.warning("cheetah batched %s failed: %s", command, error)

    def __enter__(self) -> "BatchCollector":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.flush()


class HotCommandWindow:
    """Per-command sliding window: is this command being issued in bulk?

    A counter and a start stamp rather than a list of timestamps — this runs on
    every single command, and an exact sliding window would allocate per call to
    answer a question whose only use is a yes/no.
    """

    def __init__(self, policy: AutoBatchPolicy) -> None:
        self._policy = policy
        self._windows: dict[str, dict[str, Any]] = {}
        self._advised: set[str] = set()

    def batchable(self, command: str) -> bool:
        base = command.split(":", 1)[0]
        if base in self._policy.exclude:
            return False
        if self._policy.commands is not None and base not in self._policy.commands:
            return False
        return True

    def observe(self, command: str, *, now: float | None = None) -> bool:
        """Record one call and return whether the command is hot."""
        stamp = time.monotonic() if now is None else now
        window = self._windows.get(command)
        if window is None or stamp - window["last"] > self._policy.idle:
            window = {"start": stamp, "count": 0, "last": stamp, "hot": False}
            self._windows[command] = window
        if stamp - window["start"] > self._policy.window:
            window["hot"] = window["count"] >= self._policy.threshold
            window["start"] = stamp
            window["count"] = 0
        window["count"] += 1
        window["last"] = stamp
        if window["count"] >= self._policy.threshold:
            window["hot"] = True
        return bool(window["hot"])

    def advise_once(self, command: str) -> None:
        """Say it once. A per-call log line about volume is itself the volume."""
        if command in self._advised:
            return
        self._advised.add(command)
        logger.info(
            "cheetah: %s is being issued in bulk; batch it with cheetah_db.batch.run_batch(), "
            "with `with client.batching():`, or set auto_batch={'mode': 'deferred'}",
            command,
        )

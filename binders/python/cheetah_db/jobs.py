"""The ``JOB`` micro-command: submit a bounded command, poll it, fetch it once.

A sweep that takes minutes cannot hold a connection hostage, so Cheetah detaches
it: ``JOB submit`` answers a ``job=<id>`` immediately, ``JOB status`` reports
progress, and ``JOB fetch`` returns the submitted command's own response — once,
because fetching consumes the job.

Three things a caller has to know, all of them learned the hard way:

  - **Only registered commands are submittable** (today ``PAIR_REDUCE``,
    ``PREDICT_INHERIT_BATCH`` and ``BATCH``); anything else answers
    ``ERROR,command_not_submittable``.
  - **The command line travels base64** in ``command=``. The micro dialect
    splits on whitespace and a command line is full of it.
  - **Jobs are in memory.** A server restart loses them, so a poll loop must
    treat ``job_not_found`` as a terminal failure rather than retrying forever.

The historical names (``PAIR_REDUCE_ASYNC``/``_STATUS``/``_FETCH``,
``PREDICT_INHERIT_ASYNC``/…) are aliases over exactly this and still work; use
them only against a server too old to know ``JOB``, which answers
``ERROR,unknown_command``.
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Callable

from .client import CheetahError
from .protocol import Response, build_key_value_command

__all__ = [
    "JobResultsPage",
    "JobSnapshot",
    "await_job",
    "fetch",
    "results",
    "status",
    "submit",
    "supports_job_api",
]


@dataclass(frozen=True)
class JobSnapshot:
    job_id: str
    state: str
    progress: float
    completed: int
    total: int
    error: str | None
    response: Response

    @property
    def finished(self) -> bool:
        return self.state in {"completed", "failed", "cancelled"}

    @property
    def failed(self) -> bool:
        return self.state == "failed" or bool(self.error)


def submit(conn: Any, command_line: str) -> str:
    """Submit a bounded command; returns the job id."""
    encoded = base64.b64encode(command_line.strip().encode("utf-8")).decode("ascii")
    response = conn.send(f"JOB submit command={encoded}")
    if not response.ok:
        raise CheetahError(
            f"cheetah JOB submit failed: {response.reason}", command=command_line, response=response
        )
    job_id = response.field_value("job")
    if not job_id:
        raise CheetahError(f"cheetah JOB submit returned no job id: {response.raw}", response=response)
    return job_id


def status(conn: Any, job_id: str) -> JobSnapshot:
    return _snapshot(conn.send(build_key_value_command("JOB status", {"id": job_id})), job_id)


def fetch(conn: Any, job_id: str) -> Response | None:
    """The submitted command's own response, or ``None`` while it still runs.

    Consumes the job when it returns a response: there is no second fetch.
    """
    response = conn.send(build_key_value_command("JOB fetch", {"id": job_id}))
    if response.pending:
        return None
    if not response.ok:
        raise CheetahError(f"cheetah JOB fetch {job_id} failed: {response.reason}", response=response)
    return response


@dataclass(frozen=True)
class JobResultsPage:
    """One page of the results a job has produced so far."""

    job_id: str
    state: str
    from_index: int
    count: int
    next: int
    available: int
    total: int
    lines: list[str | None]
    response: Response


def results(
    conn: Any, job_id: str, *, from_index: int = 0, limit: int = 0
) -> JobResultsPage:
    """Read the results a job has produced **so far**, without consuming it.

    The counterpart to :func:`fetch`: fetch is terminal and answers with the
    command's aggregate, ``results`` is repeatable and answers with the
    individual response lines, so a caller can consume a long ``BATCH`` page by
    page while it is still running. ``from_index`` is where to resume — feed it
    the previous page's ``next`` — and a job whose command produces no per-item
    results simply always answers ``count=0``.
    """
    # Imported here: batch.py reaches for this module to stream a detached
    # BATCH, and a module-level import in both directions would not resolve.
    from .batch import decode_result_lines

    fields = {"id": job_id, "from": from_index}
    if limit:
        fields["limit"] = limit
    response = conn.send(build_key_value_command("JOB results", fields))
    if not response.ok:
        raise CheetahError(f"cheetah JOB results {job_id} failed: {response.reason}", response=response)
    return JobResultsPage(
        job_id=response.field_value("job", job_id) or job_id,
        state=(response.field_value("state", "") or "").strip(),
        from_index=int(response.int_field("from", from_index) or 0),
        count=int(response.int_field("count", 0) or 0),
        next=int(response.int_field("next", from_index) or 0),
        available=int(response.int_field("available", 0) or 0),
        total=int(response.int_field("total", 0) or 0),
        lines=decode_result_lines(response.fields),
        response=response,
    )


def _snapshot(response: Response, job_id: str) -> JobSnapshot:
    if not (response.ok or response.pending):
        raise CheetahError(f"cheetah JOB status {job_id} failed: {response.reason}", response=response)
    return JobSnapshot(
        job_id=response.field_value("job", job_id) or job_id,
        state=(response.field_value("state", "") or "").strip(),
        progress=float(response.float_field("progress", 0.0) or 0.0),
        completed=int(response.int_field("completed", 0) or 0),
        total=int(response.int_field("total", 0) or 0),
        error=response.field_value("error"),
        response=response,
    )


def await_job(
    conn: Any,
    job_id: str,
    *,
    poll_interval: float = 5.0,
    timeout: float | None = None,
    on_progress: Callable[[JobSnapshot], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Response:
    """Poll ``job_id`` until it finishes and return its result.

    The interval is deliberately coarse: polling is a round trip that competes
    with the job it is watching, and a sweep long enough to detach is never
    milliseconds away from done.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    interval = max(0.1, float(poll_interval))
    while True:
        snapshot = status(conn, job_id)
        if on_progress is not None:
            on_progress(snapshot)
        if snapshot.failed:
            raise CheetahError(
                f"cheetah job {job_id} failed: {snapshot.error or snapshot.state}",
                response=snapshot.response,
            )
        if snapshot.finished:
            result = fetch(conn, job_id)
            if result is not None:
                return result
        if deadline is not None and time.monotonic() >= deadline:
            raise CheetahError(f"cheetah job {job_id} did not finish within {timeout}s")
        sleep(interval)


def supports_job_api(conn: Any) -> bool:
    """Whether this server knows ``JOB`` at all.

    Probed rather than assumed: the micro-commands landed after the aliases, and
    a client that pins the answer once per connection pays for the probe only
    on the first detached command.
    """
    response = conn.send("JOB")
    return not (response.error or "").startswith("unknown_command")

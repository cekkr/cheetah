"""Cheetah TCP client.

The protocol is newline-delimited text with **no request ids**: the server
answers one line per command, in the order the commands arrived on that socket
(one goroutine per connection, see ``src/server.go``). This client is
synchronous and serializes send+receive under a lock, which is the shape a
threaded Python program wants: a command and its response are one indivisible
step, so two threads sharing a client can never read each other's line.

That serialization is also the reason a *busy* program should not share one
client between threads: every command waits for the previous one's response.
:class:`ThreadLocalClientPool` gives each thread its own socket instead, which
is how a pooled Cheetah client is normally built in Python.

The client also watches for **bulk work**: when the same command is issued often
enough to look like an ingest loop, it can fold those commands into a single
``BATCH`` line (:mod:`cheetah_db.batch`) instead of paying a round trip each.
Because a synchronous client has one command in flight at a time, coalescing
means deferring the response, which moves the moment an error surfaces — so the
default policy only *reports* the opportunity and ``auto_batch={"mode":
"deferred"}`` opts into taking it. ``with client.batching():`` does the same
thing explicitly, for a block where it is obviously the right trade.

Two more behaviors here exist because of how Cheetah is deployed rather than how
it is coded:

  - **Several destinations.** ``0.0.0.0`` is a listen address, not a
    destination, and a WSL client reaching a Windows-side server is not on its
    own loopback. :mod:`cheetah_db.hosts` turns the configured host into an
    ordered candidate list, tried on connect.
  - **A long silence is not a dead socket.** A reducer sweep or a purge can
    take minutes to answer, so the read loop uses a short socket timeout to stay
    interruptible and gives up only after an *inactivity* grace window. A
    fixed socket timeout would abandon a healthy long-running command.

No dependency is used: the protocol is a socket and newlines.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Mapping

from . import binary as binary_codec
from .batch import AutoBatchPolicy, BatchCollector, HotCommandWindow, split_command_line
from .binary import BinarySession
from .hosts import candidate_hosts
from .protocol import Response, build_command, build_key_value_command, parse_response

__all__ = [
    "CheetahClient",
    "CheetahConnectionError",
    "CheetahError",
    "ThreadLocalClientPool",
    "DEFAULT_IDLE_GRACE_SECONDS",
    "DEFAULT_PORT",
    "DEFAULT_TIMEOUT_SECONDS",
]

logger = logging.getLogger(__name__)

DEFAULT_PORT = 4455
DEFAULT_TIMEOUT_SECONDS = 1.0
DEFAULT_IDLE_GRACE_SECONDS = 30.0


class CheetahError(RuntimeError):
    """Raised when a Cheetah command fails or the bridge becomes unusable."""

    def __init__(
        self,
        message: str,
        *,
        command: str | None = None,
        response: Response | None = None,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.response = response


class CheetahConnectionError(CheetahError):
    """Raised when no configured destination accepted the connection."""


class CheetahClient:
    """One socket to one cheetah-server, one database selection, one lock."""

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        *,
        database: str = "default",
        database_options: Mapping[str, Any] | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        idle_grace: float | None = None,
        auto_batch: AutoBatchPolicy | Mapping[str, Any] | None = None,
        binary: bool | Mapping[str, int] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.database_options = dict(database_options or {})
        self.timeout = timeout
        if idle_grace is not None and idle_grace > 0:
            self._readline_idle_grace = idle_grace
        else:
            self._readline_idle_grace = max(timeout * 30.0, DEFAULT_IDLE_GRACE_SECONDS)
        self._host_candidates = self._build_host_candidates(host)
        self._active_host: str | None = None
        self._last_errors: list[str] = []
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()
        # Binary mode (:mod:`cheetah_db.binary`). ``binary=True`` takes the
        # server's default widths; a mapping asks for its own. Everything above
        # this client keeps producing command lines: they are transcoded on the
        # way out and rebuilt from the response frame on the way in, so no
        # command layer notices which transport it is running over.
        self.binary_requested = (
            {} if binary is True else dict(binary) if isinstance(binary, Mapping) else None
        )
        self.binary: BinarySession | None = None
        self._binary_buffer = b""
        self.auto_batch = (
            auto_batch
            if isinstance(auto_batch, AutoBatchPolicy)
            else AutoBatchPolicy().with_overrides(auto_batch)
        )
        self._hot = HotCommandWindow(self.auto_batch)
        self._batch_stats = {"batched": 0, "batches": 0, "direct": 0}
        # The collector is per *thread*: a CheetahClient serializes its socket,
        # but two threads sharing one client must not end up appending to each
        # other's queue and receiving each other's responses.
        self._batch_local = threading.local()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def healthy(self) -> bool:
        return self._sock is not None

    @property
    def active_host(self) -> str | None:
        return self._active_host

    def set_idle_grace(self, seconds: float) -> None:
        """Override the inactivity grace period used while reading responses."""
        if seconds > 0:
            self._readline_idle_grace = seconds

    def describe_targets(self) -> str:
        if not self._host_candidates:
            return "<none>"
        return ", ".join(f"{host}:{self.port}" for host in self._host_candidates)

    def describe_failures(self) -> str:
        if not self._last_errors:
            return "no connection errors recorded"
        return "; ".join(self._last_errors)

    def describe(self) -> str:
        host = self._active_host or (self._host_candidates[0] if self._host_candidates else "<unknown>")
        return f"cheetah-db://{host}:{self.port}/{self.database}"

    def connect(self) -> bool:
        with self._lock:
            return self._ensure_connection()

    def close(self) -> None:
        # Ahead of the socket: a queued batch that is never written leaves every
        # DeferredResponse it holds unresolvable.
        self.flush()
        with self._lock:
            self._close_socket()

    def __enter__(self) -> "CheetahClient":
        self.connect()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Generic command surface
    # ------------------------------------------------------------------ #
    def command(self, text: str) -> str | None:
        """Send one raw command line, return its raw response line."""
        return self._command(text)

    def send(self, text: str) -> Response:
        """Send one raw command line, return its parsed response.

        The line may be queued into a ``BATCH`` instead of written immediately —
        see :class:`~cheetah_db.batch.AutoBatchPolicy`. When it is, the return
        value is a :class:`~cheetah_db.batch.DeferredResponse`, which behaves
        like a :class:`Response` and resolves on first use.
        """
        command, _ = split_command_line(text)
        collector = getattr(self._batch_local, "collector", None)
        if collector is not None:
            if self._hot.batchable(command):
                return collector.add(text)  # type: ignore[return-value]
            # Anything that cannot join the queue must not overtake it.
            collector.flush()
            return self.send_direct(text)

        if self.auto_batch.mode == "off" or not self._hot.batchable(command):
            return self.send_direct(text)
        if not self._hot.observe(command):
            return self.send_direct(text)
        if self.auto_batch.mode == "deferred":
            return self._open_collector().add(text)  # type: ignore[return-value]
        self._hot.advise_once(command)
        return self.send_direct(text)

    def send_direct(self, text: str) -> Response:
        """Send one raw command line now, bypassing any batching."""
        return parse_response(self._command(text))

    # ------------------------------------------------------------------ #
    # Batching
    # ------------------------------------------------------------------ #
    def _open_collector(
        self, *, max_size: int | None = None, continue_on_error: bool | None = None
    ) -> BatchCollector:
        collector = BatchCollector(
            self.send_direct,
            max_size=self.auto_batch.max_size if max_size is None else max_size,
            continue_on_error=(
                self.auto_batch.continue_on_error if continue_on_error is None else continue_on_error
            ),
            on_error=self.auto_batch.on_error,
            stats=self._batch_stats,
        )
        self._batch_local.collector = collector
        return collector

    @contextmanager
    def batching(
        self, *, max_size: int | None = None, continue_on_error: bool | None = None
    ) -> Iterator[BatchCollector]:
        """Collect the commands issued in this block into ``BATCH`` requests.

        Every ``send``/``execute`` inside answers with a
        :class:`~cheetah_db.batch.DeferredResponse` rather than a
        :class:`Response`. The queue is written when the block ends, when it
        reaches ``max_size``, and whenever a command arrives that cannot join it
        — so ordering against the rest of the connection is preserved::

            with client.batching():
                for key, abs_key in rows:
                    client.execute("PAIR_SET", key, abs_key)

        Nested blocks reuse the outermost collector and leave the flush to it:
        writing an inner queue early would put its items ahead of the outer
        one's.
        """
        existing = getattr(self._batch_local, "collector", None)
        if existing is not None:
            yield existing
            return
        collector = self._open_collector(max_size=max_size, continue_on_error=continue_on_error)
        try:
            yield collector
        finally:
            self._batch_local.collector = None
            collector.flush()

    def flush(self) -> None:
        """Write this thread's queued batch, if there is one.

        In ``deferred`` mode a loop that simply ends leaves its tail queued —
        nothing signals "the bulk work is over". Call this (or close the client)
        to be sure the last page landed.
        """
        collector = getattr(self._batch_local, "collector", None)
        if collector is not None:
            collector.flush()

    @property
    def batch_stats(self) -> dict[str, int]:
        """Coalescing counters since this client was built.

        ``{batched, batches, direct}`` — how many commands were folded, into how
        many requests, and how many went out on their own because a queue of one
        is not worth a wrapper.
        """
        return dict(self._batch_stats)

    def execute(self, name: str, *args: Any) -> Response:
        """Send a positional-dialect command (``PAIR_*``, KV, ``LOG_FLUSH``, …)."""
        return self.send(build_command(name, *args))

    def execute_kv(self, name: str, fields: Mapping[str, Any] | None = None) -> Response:
        """Send a ``key=value`` dialect command (``GRAPH_*``, ``PREDICT_*``, …)."""
        return self.send(build_key_value_command(name, fields))

    def execute_or_raise(self, name: str, *args: Any) -> Response:
        response = self.execute(name, *args)
        if not response.ok:
            raise CheetahError(
                f"cheetah {name} failed: {response.reason}",
                command=name,
                response=response,
            )
        return response

    def execute_kv_or_raise(
        self, name: str, fields: Mapping[str, Any] | None = None
    ) -> Response:
        response = self.execute_kv(name, fields)
        if not response.ok:
            raise CheetahError(
                f"cheetah {name} failed: {response.reason}",
                command=name,
                response=response,
            )
        return response

    def select_database(self, name: str, **options: Any) -> Response:
        """Point **this connection** at another logical database.

        Handled by the front-end rather than the dispatcher, and it changes what
        the socket points at — so it is always the first line on a fresh
        connection, ahead of any queued work.
        """
        self.database = name
        if options:
            self.database_options.update(options)
        return self.send(self._database_command(name))

    def reset_database(self, name: str | None = None) -> Response:
        """Close, delete and reopen a database. Destructive and immediate."""
        target = name or self.database
        return self.send(f"RESET_DB {target}" if target else "RESET_DB")

    def _database_command(self, name: str) -> str:
        overrides = " ".join(f"{key}={value}" for key, value in self.database_options.items())
        return f"DATABASE {name} {overrides}".strip()

    # ------------------------------------------------------------------ #
    # Low-level protocol management
    # ------------------------------------------------------------------ #
    def _command(self, text: str) -> str | None:
        with self._lock:
            if not self._ensure_connection():
                return None
            assert self._sock is not None
            try:
                # Encoded *after* the connection is up: in binary mode the frame
                # depends on the session this socket negotiated.
                self._sock.sendall(self._encode_command(text))
                return self._read_response()
            except OSError as exc:
                logger.debug("cheetah command failed (%s), reconnecting...", exc)
                self._close_socket()
                if not self._ensure_connection():
                    return None
                assert self._sock is not None
                self._sock.sendall(self._encode_command(text))
                return self._read_response()

    def _ensure_connection(self) -> bool:
        if self._sock:
            return True
        errors: list[str] = []
        for host in self._host_candidates:
            try:
                sock = socket.create_connection((host, self.port), self.timeout)
                sock.settimeout(self.timeout)
            except OSError as exc:
                logger.debug("Unable to reach cheetah-db at %s:%s (%s)", host, self.port, exc)
                errors.append(f"{host}:{self.port} -> {exc.__class__.__name__}: {exc}")
                continue
            self._sock = sock
            self._active_host = host
            if self.binary_requested is not None and not self._binary_handshake():
                # Before anything else: the server picks the mode from the first
                # byte, and the widths it fixes apply to every later frame.
                self._close_socket()
                errors.append(f"{host}:{self.port} -> binary handshake failed")
                continue
            if self.database and self.database != "default":
                response = self._command_unlocked(self._database_command(self.database))
                if not response or not response.startswith("SUCCESS"):
                    logger.debug(
                        "Failed to switch cheetah database on %s:%s: %s", host, self.port, response
                    )
                    self._close_socket()
                    errors.append(
                        f"{host}:{self.port} -> DATABASE {self.database} failed ({response})"
                    )
                    continue
            self._last_errors = []
            return True
        logger.debug("Unable to reach cheetah-db hosts (%s)", self.describe_targets())
        self._last_errors = errors or ["no cheetah hosts configured"]
        return False

    def _command_unlocked(self, text: str) -> str | None:
        """Send a line with the lock already held (connection handshake only)."""
        if not self._sock:
            return None
        try:
            self._sock.sendall(self._encode_command(text))
            return self._read_response()
        except OSError as exc:
            logger.debug("cheetah command failed (%s) before lock acquisition", exc)
            self._close_socket()
            return None

    # ------------------------------------------------------------------ #
    # Transport: one line of text, or one frame
    # ------------------------------------------------------------------ #
    def _encode_command(self, text: str) -> bytes:
        if self.binary_requested is None:
            return (text.strip() + "\n").encode("utf-8")
        return binary_codec.encode_command_line(text.strip(), self.binary)

    def _read_response(self) -> str | None:
        if self.binary_requested is None:
            return self._readline()
        read = self._read_frame()
        if read is None:
            return None
        frame_type, body = read
        if frame_type != binary_codec.FrameType.RESPONSE:
            logger.warning("cheetah sent frame type %s where a response was due", frame_type)
            self._close_socket()
            return None
        widths = self.binary.widths if self.binary else binary_codec.DEFAULT_WIDTHS
        try:
            # Back to the canonical line: parse_response and every layer above
            # it are untouched by which transport carried the answer.
            return binary_codec.decode_response(body, self.binary, widths).line
        except binary_codec.BinaryProtocolError as exc:
            logger.warning("cheetah binary response could not be decoded: %s", exc)
            self._close_socket()
            return None

    def _read_frame(self) -> tuple[int, bytes] | None:
        """One whole frame, or None when the socket went quiet or away."""
        while True:
            try:
                taken = binary_codec.read_frame(self._binary_buffer)
            except binary_codec.BinaryProtocolError as exc:
                # A frame we cannot even delimit means we no longer know where
                # the next one starts: the connection goes down with it.
                logger.warning("cheetah binary stream desynchronised: %s", exc)
                self._close_socket()
                return None
            if taken is not None:
                frame_type, body, rest = taken
                self._binary_buffer = rest
                return frame_type, body
            if not self._fill_binary_buffer():
                return None

    def _fill_binary_buffer(self) -> bool:
        """Read once, tolerating silence the same way _readline does."""
        if not self._sock:
            return False
        idle_deadline = time.monotonic() + self._readline_idle_grace
        while True:
            try:
                data = self._sock.recv(65536)
            except socket.timeout:
                if time.monotonic() >= idle_deadline:
                    logger.warning(
                        "cheetah response timed out after %.1fs of inactivity",
                        self._readline_idle_grace,
                    )
                    return False
                continue
            except OSError:
                self._close_socket()
                return False
            if not data:
                self._close_socket()
                return False
            self._binary_buffer += data
            return True

    def _binary_handshake(self) -> bool:
        """Negotiate the widths and take the tables the ack delivers.

        The ack carries the command index and the argument-key dictionary in
        full, which is not an optimisation: a response frame names its fields by
        index, so without the dictionary this client could not decode even the
        answer to ``ALIAS keys``. The ack is the one message that can break that
        circle.
        """
        if not self._sock:
            return False
        requested = self.binary_requested or {}
        self._binary_buffer = b""
        try:
            self._sock.sendall(
                binary_codec.encode_handshake(
                    uint=int(requested.get("uint", 0)),
                    int_=int(requested.get("int", 0)),
                    float_=int(requested.get("float", 0)),
                )
            )
        except OSError as exc:
            logger.debug("cheetah binary handshake failed to send (%s)", exc)
            return False
        read = self._read_frame()
        if read is None:
            return False
        frame_type, body = read
        if frame_type != binary_codec.FrameType.HANDSHAKE_ACK:
            logger.debug("cheetah answered frame type %s to a handshake", frame_type)
            return False
        try:
            ack = binary_codec.decode_handshake_ack(body)
        except (binary_codec.BinaryProtocolError, IndexError, struct.error) as exc:
            logger.debug("cheetah handshake ack could not be decoded: %s", exc)
            return False
        if ack.version != binary_codec.PROTOCOL_VERSION:
            logger.debug("cheetah speaks binary protocol v%s, this binder v%s",
                         ack.version, binary_codec.PROTOCOL_VERSION)
            return False
        self.binary = BinarySession().adopt(ack)
        return True

    def _readline(self) -> str | None:
        if not self._sock:
            return None
        chunks: list[bytes] = []
        idle_deadline = time.monotonic() + self._readline_idle_grace
        while True:
            try:
                data = self._sock.recv(1)
            except socket.timeout:
                if time.monotonic() >= idle_deadline:
                    logger.warning(
                        "cheetah response timed out after %.1fs of inactivity",
                        self._readline_idle_grace,
                    )
                    return None
                continue
            if not data:
                self._close_socket()
                return None
            if data == b"\n":
                break
            if data != b"\r":
                chunks.append(data)
            idle_deadline = time.monotonic() + self._readline_idle_grace
        return b"".join(chunks).decode("utf-8", "replace")

    def _close_socket(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._active_host = None
        # The negotiated session belongs to the socket that is going away: a
        # reconnection re-handshakes, and the index it gets back may differ.
        self.binary = None
        self._binary_buffer = b""

    def _build_host_candidates(self, host: str) -> list[str]:
        return candidate_hosts(host)


class ThreadLocalClientPool:
    """Creates or reuses one client per thread.

    A :class:`CheetahClient` serializes its commands, so sharing one across
    worker threads turns concurrent work into a queue. Handing each thread its
    own socket is both the simplest and the fastest arrangement, and it keeps
    the server's per-connection model (database selection, response ordering)
    intact.
    """

    def __init__(
        self,
        factory: Callable[[], CheetahClient],
        *,
        warm_client: CheetahClient | None = None,
        description: str | None = None,
    ) -> None:
        self._factory = factory
        self._local = threading.local()
        self._lock = threading.Lock()
        self._clients: list[CheetahClient] = []
        self._description = description
        if warm_client is not None:
            self._local.client = warm_client
            self._register_client(warm_client)

    def acquire(self) -> CheetahClient:
        client = getattr(self._local, "client", None)
        if client is not None:
            return client
        client = self._factory()
        connect_fn = getattr(client, "connect", None)
        if callable(connect_fn):
            if not connect_fn():
                target = getattr(client, "describe_targets", lambda: "<unknown>")()
                raise CheetahConnectionError(f"cheetah connection failed ({target})")
        self._local.client = client
        self._register_client(client)
        return client

    def describe(self) -> str:
        if self._description:
            return self._description
        with self._lock:
            if self._clients:
                return self._describe_client(self._clients[0])
        return "<unknown>"

    def close_all(self) -> None:
        with self._lock:
            clients = list(self._clients)
            # Thread-local slots keep their client objects. Keep those same
            # objects registered too: a later command can reconnect one after
            # RESET_DB, and a subsequent close_all must still find and close
            # that new socket. Clearing only this shared list leaked the
            # reconnected handle at final shutdown.
        for client in clients:
            try:
                client.close()
            except Exception:  # pragma: no cover - close must never raise here
                continue

    def _register_client(self, client: CheetahClient) -> None:
        with self._lock:
            self._clients.append(client)
            if not self._description:
                self._description = self._describe_client(client)

    @staticmethod
    def _describe_client(client: CheetahClient) -> str:
        describe = getattr(client, "describe", None)
        if callable(describe):
            return describe()
        host = getattr(client, "host", "<unknown>")
        port = getattr(client, "port", "<unknown>")
        database = getattr(client, "database", "<unknown>")
        return f"cheetah-db://{host}:{port}/{database}"

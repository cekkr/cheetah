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

Two behaviors here exist because of how Cheetah is deployed rather than how it
is coded:

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
import threading
import time
from typing import Any, Callable, Mapping

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
        """Send one raw command line, return its parsed response."""
        return parse_response(self._command(text))

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
        line = (text.strip() + "\n").encode("utf-8")
        with self._lock:
            if not self._ensure_connection():
                return None
            assert self._sock is not None
            try:
                self._sock.sendall(line)
                return self._readline()
            except OSError as exc:
                logger.debug("cheetah command failed (%s), reconnecting...", exc)
                self._close_socket()
                if not self._ensure_connection():
                    return None
                assert self._sock is not None
                self._sock.sendall(line)
                return self._readline()

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
        line = (text.strip() + "\n").encode("utf-8")
        if not self._sock:
            return None
        try:
            self._sock.sendall(line)
            return self._readline()
        except OSError as exc:
            logger.debug("cheetah command failed (%s) before lock acquisition", exc)
            self._close_socket()
            return None

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
            self._clients.clear()
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

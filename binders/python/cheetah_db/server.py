"""cheetah-server lifecycle helper.

Spawns the Go binary headless on a chosen port and data dir, waits for the
listener to accept, and shuts it down cleanly. This is a development and test
convenience — a deployment runs the server itself.

The Go module is this binder's own repository, three levels up from
``binders/python/cheetah_db``, so a checkout can build its own binary::

    go build -o cheetah-server ./src

A host project that keeps the binary somewhere else passes ``binary_path``.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Mapping

__all__ = [
    "CheetahServerProcess",
    "DEFAULT_BINARY",
    "MODULE_ROOT",
    "ensure_server_binary",
    "server_binary_name",
    "start_server",
    "wait_for_listener",
]

#: The cheetah repository root: ``<root>/binders/python/cheetah_db`` → ``<root>``.
MODULE_ROOT = Path(__file__).resolve().parents[3]


def server_binary_name(platform: str = sys.platform) -> str:
    """Return the executable file name Go and the host launcher agree on."""
    return "cheetah-server.exe" if platform == "win32" else "cheetah-server"


DEFAULT_BINARY = MODULE_ROOT / server_binary_name()


def ensure_server_binary(
    *,
    binary_path: Path | str = DEFAULT_BINARY,
    source_dir: Path | str = MODULE_ROOT,
    force: bool = False,
) -> Path:
    """Build ``cheetah-server`` from source if it is missing (or ``force``)."""
    binary = Path(binary_path)
    source = Path(source_dir)
    if not force and binary.is_file():
        return binary
    if not (source / "go.mod").exists():
        raise FileNotFoundError(
            f"cheetah source is not present at {source} — if it is a git submodule, "
            "run: git submodule update --init"
        )
    result = subprocess.run(
        ["go", "build", "-o", str(binary), "./src"],
        cwd=str(source),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cheetah-server build failed:\n{result.stderr or result.stdout}")
    return binary


def _try_connect(host: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout):
            return True
    except OSError:
        return False


def wait_for_listener(
    host: str, port: int, *, timeout: float = 10.0, interval: float = 0.05
) -> bool:
    """Poll until the listener accepts, or raise after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        if _try_connect(host, port, min(0.5, interval * 4)):
            return True
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"cheetah-server did not start listening on {host}:{port} within {timeout}s"
            )
        time.sleep(interval)


class CheetahServerProcess:
    def __init__(self, child: subprocess.Popen, host: str, port: int, data_dir: Path, logs: list[str]):
        self.child = child
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.logs = logs
        self.stopped = False

    def stop(self, *, grace: float = 3.0) -> None:
        """SIGTERM, then SIGKILL if it does not exit within ``grace``."""
        if self.stopped or self.child.poll() is not None:
            self.stopped = True
            return
        self.stopped = True
        self.child.terminate()
        try:
            self.child.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait(timeout=grace)
        if self.child.stdout is not None:
            self.child.stdout.close()

    def __enter__(self) -> "CheetahServerProcess":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()


def start_server(
    *,
    host: str = "127.0.0.1",
    port: int = 4455,
    cwd: Path | str | None = None,
    data_dir: Path | str | None = None,
    binary_path: Path | str = DEFAULT_BINARY,
    source_dir: Path | str = MODULE_ROOT,
    build: bool = True,
    graph_term_index: bool | None = None,
    pair_index_bytes: int | None = None,
    env: Mapping[str, str] | None = None,
    ready_timeout: float = 10.0,
) -> CheetahServerProcess:
    """Start a headless cheetah-server.

    ``graph_term_index`` and ``pair_index_bytes`` are left **unset** by default,
    so the server's own configuration decides. Pass them only to override it —
    and note that ``pair_index_bytes`` is adopted when a database directory is
    *created* and pinned from then on, so setting it here does nothing to an
    existing one.
    """
    working_dir = Path(cwd or Path.cwd())
    data_path = Path(data_dir or working_dir / "cheetah_data")
    binary = ensure_server_binary(binary_path=binary_path, source_dir=source_dir) if build else Path(binary_path)
    if not binary.is_file():
        raise FileNotFoundError(f"cheetah-server binary not found at {binary}")
    data_path.mkdir(parents=True, exist_ok=True)

    child_env = dict(os.environ)
    child_env.update(
        {
            "CHEETAH_HEADLESS": "1",
            "CHEETAH_LISTEN_ADDR": f"{host}:{port}",
            "CHEETAH_DATA_DIR": str(data_path),
        }
    )
    if graph_term_index is not None:
        child_env["CHEETAH_GRAPH_TERM_INDEX"] = "1" if graph_term_index else "0"
    if pair_index_bytes is not None:
        child_env["CHEETAH_PAIR_INDEX_BYTES"] = str(pair_index_bytes)
    child_env.update(env or {})

    child = subprocess.Popen(
        [str(binary)],
        cwd=str(working_dir),
        env=child_env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    logs: list[str] = []

    def drain() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            if line.strip():
                logs.append(line.rstrip())

    threading.Thread(target=drain, daemon=True).start()

    server = CheetahServerProcess(child, host, port, data_path, logs)
    try:
        wait_for_listener(host, port, timeout=ready_timeout)
    except TimeoutError as exc:
        server.stop()
        detail = "" if child.poll() is None else f" (process exited code={child.returncode})"
        raise TimeoutError(f"{exc}{detail}\n" + "\n".join(logs[-20:])) from exc
    return server

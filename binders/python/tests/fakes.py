"""An in-memory stand-in that speaks the same line protocol as cheetah-server.

It is deliberately small and deliberately *literal*: it reproduces the response
lines the real server emits for the commands the binder spells, including the
awkward ones (``value=`` running to end of line, ``x<hex>`` arguments, cursors,
``ERROR,not_found``). It proves the binder's codec and call shapes; it proves
nothing about the server, which is what ``go test ./src`` and the gated
integration test are for.
"""

from __future__ import annotations

import base64
import json
import socket
import threading
from typing import Any

from cheetah_db.protocol import parse_response


def _parse_value(token: str) -> bytes:
    if token.startswith("x"):
        return bytes.fromhex(token[1:])
    return token.encode("latin1")


def _kv_args(rest: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in rest.split():
        key, sep, value = token.partition("=")
        if sep:
            fields[key] = value
    return fields


class FakeCheetahServer:
    """The subset of the command surface the binder's own tests exercise."""

    def __init__(self) -> None:
        self.values: dict[int, bytes] = {}
        self.pairs: dict[bytes, int] = {}
        self.hidden: set[bytes] = set()
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: list[dict[str, Any]] = []
        self.jobs: dict[str, str] = {}
        self.commands: list[str] = []
        self.database = "default"
        self._next_key = 1

    # -- dispatch ------------------------------------------------------- #
    def execute(self, line: str) -> str:
        self.commands.append(line)
        name, _, rest = line.strip().partition(" ")
        upper = name.upper()
        handler = getattr(self, f"_do_{upper.lower().replace(':', '_')}", None)
        if upper.startswith("INSERT"):
            return self._do_insert(upper, rest)
        if handler is None:
            return "ERROR,unknown_command"
        return handler(rest)

    # -- values --------------------------------------------------------- #
    def _do_insert(self, command: str, rest: str) -> str:
        if not rest:
            return "ERROR,missing_value"
        if ":" in command:
            declared = int(command.split(":", 1)[1])
            if declared != len(rest.encode("utf-8")):
                return f"ERROR,value_size_mismatch (expected {declared}, got {len(rest)})"
        key = self._next_key
        self._next_key += 1
        self.values[key] = rest.encode("utf-8")
        return f"SUCCESS,key={key}"

    def _do_read(self, rest: str) -> str:
        key = int(rest.strip())
        payload = self.values.get(key)
        if payload is None:
            return "ERROR,key_not_found"
        text = payload.decode("utf-8")
        return f"SUCCESS,size={len(payload)},value={text}"

    def _do_edit(self, rest: str) -> str:
        raw_key, _, payload = rest.partition(" ")
        key = int(raw_key)
        if key not in self.values:
            return "ERROR,key_not_found"
        self.values[key] = payload.encode("utf-8")
        return "SUCCESS,edited"

    # -- pairs ---------------------------------------------------------- #
    def _do_pair_set(self, rest: str, hidden: bool = False) -> str:
        value, _, raw_key = rest.rpartition(" ")
        key_bytes = _parse_value(value)
        self.pairs[key_bytes] = int(raw_key)
        if hidden:
            self.hidden.add(key_bytes)
        return "SUCCESS,pair_set"

    def _do_pair_set_hidden(self, rest: str) -> str:
        return self._do_pair_set(rest, hidden=True)

    def _do_pair_get(self, rest: str) -> str:
        key = self.pairs.get(_parse_value(rest.strip()))
        if key is None:
            return "ERROR,not_found"
        return f"SUCCESS,key={key}"

    def _do_pair_put_batch(self, rest: str) -> str:
        fields = _kv_args(rest)
        items = json.loads(base64.b64decode(fields["items"]).decode("utf-8"))
        assigned: list[int | None] = []
        applied = 0
        failed = 0
        first_error = ""
        for index, item in enumerate(items):
            key_bytes = _parse_value(item["k"])
            value_bytes = _parse_value(item["v"])
            if not key_bytes or not value_bytes:
                failed += 1
                assigned.append(None)
                first_error = first_error or f"item_{index}:_pair_value_cannot_be_empty"
                if not fields.get("continue_on_error"):
                    break
                continue
            key = self._next_key
            self._next_key += 1
            self.values[key] = value_bytes
            self.pairs[key_bytes] = key
            if fields.get("hidden"):
                self.hidden.add(key_bytes)
            assigned.append(key)
            applied += 1
        line = (
            f"SUCCESS,command=PAIR_PUT_BATCH,requested={len(items)},"
            f"applied={applied},failed={failed}"
        )
        if first_error:
            line += f",first_error={first_error}"
        if fields.get("keys"):
            payload = base64.b64encode(json.dumps(assigned).encode("utf-8")).decode("ascii")
            line += f",payload={payload}"
        return line

    def _do_pair_scan(self, rest: str) -> str:
        parts = rest.split()
        prefix = b"" if not parts or parts[0] == "*" else _parse_value(parts[0])
        limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 500
        cursor = b""
        for token in parts[2:]:
            if token.startswith("x") or token == "*":
                cursor = _parse_value(token) if token != "*" else b""
        include_hidden = "include_hidden=1" in parts
        keys = sorted(
            key
            for key in self.pairs
            if key.startswith(prefix)
            and key > cursor
            and (include_hidden or key not in self.hidden)
        )
        page = keys[:limit]
        items = ";".join(f"{key.hex()}:{self.pairs[key]}" for key in page)
        next_cursor = page[-1].hex() if len(keys) > limit else None
        line = f"SUCCESS,count={len(page)}"
        if next_cursor:
            line += f",next_cursor=x{next_cursor}"
        if items:
            line += f",items={items}"
        return line

    def _do_pair_reduce(self, rest: str) -> str:
        parts = rest.split()
        reducer = parts[0]
        scanned = parse_response(self._do_pair_scan(" ".join(parts[1:])))
        items = []
        for item in scanned.items():
            payload = base64.b64encode(self.values[item.abs_key]).decode("ascii")
            items.append(f"{item.key_hex}:{item.abs_key}:{payload}")
        line = f"SUCCESS,reducer={reducer},count={len(items)}"
        if scanned.cursor():
            line += f",next_cursor={scanned.cursor()}"
        if items:
            line += ",items=" + ";".join(items)
        return line

    def _do_pair_summary(self, rest: str) -> str:
        parts = rest.split()
        prefix = b"" if not parts or parts[0] == "*" else _parse_value(parts[0])
        keys = [key for key in self.pairs if key.startswith(prefix)]
        total = sum(len(self.values.get(self.pairs[key], b"")) for key in keys)
        return f"SUCCESS,count={len(keys)},total_payload_bytes={total},max_depth=1"

    def _do_del(self, rest: str) -> str:
        target, _, options = rest.partition(" ")
        fields = _kv_args(options)
        if target in {"values", "value"}:
            key = int(fields.get("key", "0"))
            if self.values.pop(key, None) is None:
                return "ERROR,key_not_found"
            return f"SUCCESS,deleted=1,key={key}"
        if target in {"pairs", "pair"}:
            if "prefix" in fields:
                raw = fields["prefix"]
                prefix = b"" if raw == "*" else _parse_value(raw)
                doomed = [key for key in self.pairs if key.startswith(prefix)]
                for key in doomed:
                    absolute = self.pairs.pop(key)
                    if fields.get("payloads") != "0":
                        self.values.pop(absolute, None)
                return f"SUCCESS,deleted={len(doomed)}"
            key_bytes = _parse_value(fields.get("key", ""))
            if self.pairs.pop(key_bytes, None) is None:
                return "ERROR,not_found"
            return "SUCCESS,deleted=1"
        if target == "graph":
            node = fields.get("node")
            if node is not None:
                if self.nodes.pop(node, None) is None:
                    return "ERROR,node_not_found"
                if fields.get("cascade") == "1":
                    self.edges = [
                        edge for edge in self.edges if node not in (edge["from"], edge["to"])
                    ]
                return f"SUCCESS,deleted=1,node={node}"
            return "SUCCESS,deleted=1,edge=e1"
        return "ERROR,unknown_del_target"

    # -- graph ---------------------------------------------------------- #
    def _do_graph_node_set(self, rest: str) -> str:
        fields = _kv_args(rest)
        node = self.nodes.setdefault(fields["id"], {"id": fields["id"]})
        if "labels" in fields:
            node["labels"] = fields["labels"].split(",")
        if "props" in fields:
            node["props"] = json.loads(base64.b64decode(fields["props"]).decode("utf-8"))
        if "references" in fields:
            node["references"] = (
                []
                if fields["references"] == "-"
                else json.loads(base64.b64decode(fields["references"]).decode("utf-8"))
            )
        return f"SUCCESS,node_set,id={fields['id']}"

    def _do_graph_node_get(self, rest: str) -> str:
        fields = _kv_args(rest)
        node = self.nodes.get(fields.get("id", ""))
        if node is None:
            return "ERROR,node_not_found"
        payload = base64.b64encode(json.dumps(node).encode("utf-8")).decode("ascii")
        return f"SUCCESS,id={node['id']},payload={payload}"

    def _do_graph_edge_set_batch(self, rest: str) -> str:
        fields = _kv_args(rest)
        items = json.loads(base64.b64decode(fields["items"]).decode("utf-8"))
        for item in items:
            self.edges.append(
                {
                    "from": item.get("from"),
                    "to": item.get("to"),
                    "type": item.get("type") or fields.get("type") or "",
                }
            )
        return (
            f"SUCCESS,requested={len(items)},applied={len(items)},"
            f"created={len(items)},updated=0,failed=0"
        )

    def _do_graph_degree(self, rest: str) -> str:
        fields = _kv_args(rest)
        node = fields.get("id", "")
        if node not in self.nodes:
            return "ERROR,node_not_found"
        degree = sum(1 for edge in self.edges if edge["from"] == node)
        return f"SUCCESS,id={node},direction=out,type=*,degree={degree},weighted_degree=0.000000"

    def _do_graph_recall(self, rest: str) -> str:
        fields = _kv_args(rest)
        raw = fields.get("seeds", "")
        if raw.startswith("base64:"):
            raw = base64.b64decode(raw[len("base64:") :]).decode("utf-8")
        seeds = [seed for seed in raw.split(",") if seed]
        associations = [
            {
                "id": f"hit:{seed}",
                "score": 0.5,
                "source_count": 1,
                "sources": [{"seed": seed, "activation": 0.5}],
            }
            for seed in seeds
        ]
        payload = base64.b64encode(
            json.dumps({"seeds": seeds, "associations": associations}).encode("utf-8")
        ).decode("ascii")
        return f"SUCCESS,command=GRAPH_RECALL,count={len(associations)},payload={payload}"

    # -- jobs ----------------------------------------------------------- #
    def _do_job(self, rest: str) -> str:
        action, _, options = rest.partition(" ")
        fields = _kv_args(options)
        if action == "":
            return "ERROR,job_requires_action"
        if action == "submit":
            encoded = fields.get("command", "")
            line = base64.b64decode(encoded).decode("utf-8") if encoded else ""
            name = line.split(" ", 1)[0].upper()
            if name not in {"PAIR_REDUCE", "PREDICT_INHERIT_BATCH"}:
                return "ERROR,command_not_submittable"
            job_id = f"job_{len(self.jobs) + 1}"
            self.jobs[job_id] = line
            return f"SUCCESS,job={job_id},kind=reduce,command={name},state=queued,total=0"
        job_id = fields.get("id", "")
        if job_id not in self.jobs:
            return "ERROR,job_not_found"
        if action == "status":
            return f"SUCCESS,job={job_id},state=completed,progress=100.00,completed=1,total=1"
        if action == "fetch":
            # A fetch consumes the job, exactly as the server's does.
            line = self.jobs.pop(job_id)
            inner = self.execute(line)
            return inner.replace("SUCCESS,", f"SUCCESS,job={job_id},", 1)
        return "ERROR,unknown_job_action"

    # -- session -------------------------------------------------------- #
    def _do_database(self, rest: str) -> str:
        self.database = rest.split()[0] if rest.strip() else self.database
        return f"SUCCESS,database={self.database}"

    def _do_reset_db(self, rest: str) -> str:
        self.values.clear()
        self.pairs.clear()
        self.hidden.clear()
        self.nodes.clear()
        self.edges.clear()
        return "SUCCESS,database_reset"

    def _do_system_stats(self, rest: str) -> str:
        return (
            "SUCCESS,command=SYSTEM_STATS,logical_cores=8,gomaxprocs=8,goroutines=12,"
            "process_cpu_pct=1.50,system_cpu_pct=NA,payload_cache_enabled=1,"
            "payload_cache_entries=3,payload_cache_bytes=64,payload_cache_hits=9,"
            "payload_cache_misses=1"
        )

    def _do_log_flush(self, rest: str) -> str:
        entries = ["one", "two"]
        payload = base64.b64encode(json.dumps(entries).encode("utf-8")).decode("ascii")
        return f"SUCCESS,count={len(entries)},payload={payload}"

    def _do_file_checkpoint(self, rest: str) -> str:
        return "SUCCESS,file_checkpoint_flushed=4"


class FakeConnection:
    """A ``conn`` for the free-function layers: no socket, same responses."""

    def __init__(self, server: FakeCheetahServer | None = None) -> None:
        self.server = server or FakeCheetahServer()

    def send(self, line: str):
        return parse_response(self.server.execute(line))

    @property
    def commands(self) -> list[str]:
        return self.server.commands


class FakeServerSocket:
    """The same stand-in behind a real TCP listener, for client-level tests."""

    def __init__(self, server: FakeCheetahServer | None = None) -> None:
        self.server = server or FakeCheetahServer()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self.host, self.port = self._listener.getsockname()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            buffer = b""
            while not self._stop.is_set():
                try:
                    chunk = conn.recv(4096)
                except OSError:
                    return
                if not chunk:
                    return
                buffer += chunk
                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    response = self.server.execute(line.decode("utf-8"))
                    try:
                        conn.sendall(response.encode("utf-8") + b"\n")
                    except OSError:
                        return

    def close(self) -> None:
        self._stop.set()
        try:
            self._listener.close()
        except OSError:
            pass

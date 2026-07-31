"""Server operations and cluster placement — the commands about the server.

Everything here answers a question about the *process* rather than the data:
how loaded it is, what it has been logging, when it last flushed, and which
node owns which fork. They are cheap enough to poll between phases of a long
ingest, which is what they exist for.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .client import CheetahError
from .protocol import Response, build_command, build_key_value_command, numeric_field

__all__ = [
    "SystemStats",
    "cluster_gossip",
    "cluster_move",
    "cluster_status",
    "cluster_update",
    "file_checkpoint",
    "fork_assign",
    "log_flush",
    "system_stats",
]


@dataclass(frozen=True)
class SystemStats:
    """Live gauges — a cheap heartbeat between ingest and reduce loops.

    Only the fields every build reports are named; ``fields`` keeps the whole
    line so a newer server's additions are not lost on the way through.
    """

    logical_cores: int | None
    gomaxprocs: int | None
    goroutines: int | None
    process_cpu_pct: float | None
    system_cpu_pct: float | None
    payload_cache_entries: int | None
    payload_cache_bytes: int | None
    payload_cache_hits: int | None
    payload_cache_misses: int | None
    fields: dict[str, str]

    @property
    def cache_hit_ratio(self) -> float | None:
        hits, misses = self.payload_cache_hits, self.payload_cache_misses
        if hits is None or misses is None or (hits + misses) == 0:
            return None
        return hits / (hits + misses)


def system_stats(conn: Any) -> SystemStats:
    response = conn.send("SYSTEM_STATS")
    if not response.ok:
        raise CheetahError(f"cheetah SYSTEM_STATS failed: {response.reason}", response=response)
    fields = dict(response.fields)
    return SystemStats(
        logical_cores=response.int_field("logical_cores"),
        gomaxprocs=response.int_field("gomaxprocs"),
        goroutines=response.int_field("goroutines"),
        # `NA` where the platform cannot measure it — not zero, and not an error.
        process_cpu_pct=response.float_field("process_cpu_pct"),
        system_cpu_pct=response.float_field("system_cpu_pct"),
        payload_cache_entries=response.int_field("payload_cache_entries"),
        payload_cache_bytes=response.int_field("payload_cache_bytes"),
        payload_cache_hits=response.int_field("payload_cache_hits"),
        payload_cache_misses=response.int_field("payload_cache_misses"),
        fields=fields,
    )


def log_flush(conn: Any, limit: int = 0) -> list[str]:
    """Dump **and clear** the in-memory log ring (default depth 256).

    Clearing is the point and the trap: two readers of the same ring each see
    half the history. Keep one flusher.
    """
    response = conn.send(build_command("LOG_FLUSH", limit if limit > 0 else None))
    if not response.ok:
        raise CheetahError(f"cheetah LOG_FLUSH failed: {response.reason}", response=response)
    payload = response.payload()
    return [str(entry) for entry in payload] if payload else []


def file_checkpoint(
    conn: Any,
    *,
    idle: str | None = None,
    drop_cache: bool = False,
    close_handles: bool = False,
) -> int:
    """Force the managed-file layer to act now: flush, optionally drop and close.

    The manual form of what shutdown does. Its own small dialect — bare
    uppercase flags — is spelled here so callers do not have to remember it.
    """
    parts = ["FILE_CHECKPOINT"]
    if idle:
        parts.append(f"IDLE={idle}")
    if drop_cache:
        parts.append("DROP_CACHE")
    if close_handles:
        parts.append("CLOSE_HANDLES")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah FILE_CHECKPOINT failed: {response.reason}", response=response)
    for flag in response.flags:
        if flag.startswith("file_checkpoint_flushed="):
            _, _, value = flag.partition("=")
            try:
                return int(value)
            except ValueError:
                return 0
    return int(numeric_field(response.fields, "file_checkpoint_flushed", 0) or 0)


# --------------------------------------------------------------------------- #
# Cluster placement
# --------------------------------------------------------------------------- #
def cluster_update(
    conn: Any, *, replication: int, nodes: Mapping[str, str] | None = None
) -> Response:
    """Register the topology — who exists, where, and how many replicas a fork wants.

    ``nodes`` maps a node id to ``host:port/weight``. Persisted to
    ``cluster_topology.json``; the placement *overrides* made with
    :func:`cluster_move` are not.
    """
    parts = ["CLUSTER_UPDATE", f"replication={int(replication)}"]
    for node_id, address in (nodes or {}).items():
        parts.append(f"{node_id}={address}")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah CLUSTER_UPDATE failed: {response.reason}", response=response)
    return response


def cluster_status(conn: Any) -> dict[str, Any]:
    response = conn.send("CLUSTER_STATUS")
    if not response.ok:
        raise CheetahError(f"cheetah CLUSTER_STATUS failed: {response.reason}", response=response)
    return {"fields": dict(response.fields), "payload": response.payload(), "response": response}


def fork_assign(conn: Any, prefix: str | bytes | None = None) -> dict[str, Any]:
    """Which fork a prefix hashes to, and which nodes own it."""
    target = "*" if prefix in (None, b"", "") else prefix
    response = conn.send(build_command("FORK_ASSIGN", target))
    if not response.ok:
        raise CheetahError(f"cheetah FORK_ASSIGN failed: {response.reason}", response=response)
    nodes: Sequence[str] = (response.field_value("nodes", "") or "").split("|")
    return {
        "fork_id": response.field_value("fork_id"),
        "nodes": [node for node in nodes if node],
        "response": response,
    }


def cluster_move(
    conn: Any, *, node: str, prefix: str | bytes | None = None, fork: str | None = None
) -> Response:
    """Force a fork onto a node and gossip the transfer to peers.

    The override lives in memory: it does **not** survive a restart.
    """
    if prefix in (None, b"", "") and not fork:
        raise CheetahError("cheetah CLUSTER_MOVE requires a prefix or a fork id")
    fields: dict[str, Any] = {"node": node}
    if fork:
        fields["fork"] = fork
    else:
        fields["prefix"] = prefix
    response = conn.send(build_key_value_command("CLUSTER_MOVE", fields))
    if not response.ok:
        raise CheetahError(f"cheetah CLUSTER_MOVE failed: {response.reason}", response=response)
    return response


def cluster_gossip(conn: Any, message: Any) -> Response:
    """The inbound peer channel. Machine-to-machine; do not drive it by hand.

    Clustering is unauthenticated: enable it only among trusted nodes.
    """
    from .protocol import encode_json_argument

    payload = message if isinstance(message, str) else encode_json_argument(message)
    response = conn.send(f"CLUSTER_GOSSIP json={payload}")
    if not response.ok:
        raise CheetahError(f"cheetah CLUSTER_GOSSIP failed: {response.reason}", response=response)
    return response

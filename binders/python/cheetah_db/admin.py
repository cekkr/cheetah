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
    "DATABASE_SETTINGS",
    "DatabaseConfigChange",
    "DatabaseInfo",
    "SystemStats",
    "cluster_gossip",
    "cluster_move",
    "cluster_status",
    "cluster_update",
    "create_database",
    "configure_database",
    "file_checkpoint",
    "fork_assign",
    "list_databases",
    "log_flush",
    "reset_database",
    "system_stats",
    "use_database",
]

#: The per-database settings ``DB_CONFIG``/``DB_CREATE``/``DATABASE``/``RESET_DB`` accept.
#: They override the server's own ``[database]`` section for that database
#: alone and are persisted next to its data (``<db>/settings.ini``), so they
#: survive a restart. The trie-geometry ones only bite when the directory is
#: *created*: ``pairs/format.dat`` wins on every ordinary open, which is why
#: adopting a new stride means :func:`reset_database`.
DATABASE_SETTINGS = (
    "pair_bytes",
    "pair_index_bytes",
    "adaptive_pair_index",
    "pair_list_max_bytes",
    "pair_list_max_fill_percent",
    "payload_cache_entries",
    "payload_cache_mb",
    "payload_cache_bytes",
    "graph_cache_enabled",
    "graph_cache_sample",
    "graph_cache_capacity",
    "graph_cache_half_life",
    "graph_cache_min_utility",
    "graph_cache_budget",
    "graph_cache_interval",
    "graph_cache_page",
    "graph_cache_page_size",
)


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


# --------------------------------------------------------------------------- #
# Databases — the engine, not one database
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DatabaseInfo:
    """One row of ``DB_LIST``.

    ``ad_hoc`` says the database carries a ``settings.ini`` of its own, which is
    the difference between "these are the server's settings" and "these are
    this database's settings".
    """

    name: str
    path: str
    loaded: bool
    ad_hoc: bool
    settings: dict[str, Any]


@dataclass(frozen=True)
class DatabaseConfigChange:
    """Result of :func:`configure_database`, including deferred actions."""

    name: str
    loaded: bool
    applied: tuple[str, ...]
    on_open: tuple[str, ...]
    reopen: tuple[str, ...]
    reset: tuple[str, ...]
    settings: dict[str, str]
    response: Response


def _settings_fields(settings: Mapping[str, Any] | None, **kwargs: Any) -> dict[str, Any]:
    merged: dict[str, Any] = dict(settings or {})
    merged.update({key: value for key, value in kwargs.items() if value is not None})
    unknown = [key for key in merged if key not in DATABASE_SETTINGS]
    if unknown:
        raise CheetahError(
            f"cheetah database settings not understood: {', '.join(sorted(unknown))}; "
            f"expected one of {', '.join(DATABASE_SETTINGS)}"
        )
    rendered: dict[str, Any] = {}
    for key, value in merged.items():
        rendered[key] = (1 if value else 0) if isinstance(value, bool) else value
    return rendered


def create_database(
    conn: Any, name: str, settings: Mapping[str, Any] | None = None, **kwargs: Any
) -> dict[str, Any]:
    """``DB_CREATE`` — a **new** database, optionally with settings of its own.

    Unlike :func:`use_database`, which opens-or-creates, this refuses a name
    that already exists (``ERROR,database_exists:<name>``): a creation that
    silently adopted a populated directory would also silently ignore the
    settings you passed, since trie geometry is decided when the directory is
    made. It does not point the connection at the new database — call
    :func:`use_database` for that.

    Returns the settings the database was actually created with.
    """
    parts = ["DB_CREATE", str(name)]
    for key, value in _settings_fields(settings, **kwargs).items():
        parts.append(f"{key}={value}")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah DB_CREATE {name} failed: {response.reason}", response=response)
    created = dict(response.fields)
    created.pop("database_created", None)
    return {
        "name": response.field_value("database_created", name),
        "settings": created,
        "response": response,
    }


def configure_database(
    conn: Any, name: str, settings: Mapping[str, Any] | None = None, **kwargs: Any
) -> DatabaseConfigChange:
    """Persist and hot-apply settings for an existing database.

    Payload and graph-cache fields apply immediately when the database is
    loaded. Trie geometry remains pinned and is returned in ``reset``.
    """
    parts = ["DB_CONFIG", str(name)]
    for key, value in _settings_fields(settings, **kwargs).items():
        parts.append(f"{key}={value}")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah DB_CONFIG {name} failed: {response.reason}", response=response)

    def actions(field: str) -> tuple[str, ...]:
        value = response.field_value(field, "") or ""
        return tuple(item for item in value.split(";") if item and item != "-")

    metadata = {"database_configured", "loaded", "applied", "on_open", "reopen", "reset"}
    return DatabaseConfigChange(
        name=response.field_value("database_configured", name) or name,
        loaded=response.field_value("loaded", "0") == "1",
        applied=actions("applied"),
        on_open=actions("on_open"),
        reopen=actions("reopen"),
        reset=actions("reset"),
        settings={key: value for key, value in response.fields.items() if key not in metadata},
        response=response,
    )


def list_databases(conn: Any) -> list[DatabaseInfo]:
    """``DB_LIST`` — every database under ``data_dir`` and how it would open.

    Reads the disk rather than the registry, so a database never opened in this
    process is still listed.
    """
    response = conn.send("DB_LIST")
    if not response.ok:
        raise CheetahError(f"cheetah DB_LIST failed: {response.reason}", response=response)
    payload = response.payload() or []
    return [
        DatabaseInfo(
            name=str(entry.get("name", "")),
            path=str(entry.get("path", "")),
            loaded=bool(entry.get("loaded")),
            ad_hoc=bool(entry.get("ad_hoc_settings")),
            settings=dict(entry.get("settings") or {}),
        )
        for entry in payload
    ]


def use_database(
    conn: Any, name: str, settings: Mapping[str, Any] | None = None, **kwargs: Any
) -> Response:
    """``DATABASE`` — point **this connection** at a database, creating it if new.

    Connection-scoped: it changes what this socket is talking to and nothing
    else, so a pool must be told database by database (which is what the
    client's own ``database=`` option does at connect time). Settings given here
    are recorded and persisted for that name exactly as with
    :func:`create_database`.
    """
    parts = ["DATABASE", str(name)]
    for key, value in _settings_fields(settings, **kwargs).items():
        parts.append(f"{key}={value}")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah DATABASE {name} failed: {response.reason}", response=response)
    return response


def reset_database(
    conn: Any, name: str | None = None, settings: Mapping[str, Any] | None = None, **kwargs: Any
) -> Response:
    """``RESET_DB`` — delete the directory and reopen it empty.

    The only way to adopt a new trie geometry, since ``pairs/format.dat`` is
    authoritative on every ordinary open. Destructive and not confirmable:
    everything in that database is gone.
    """
    parts = ["RESET_DB"]
    if name:
        parts.append(str(name))
    for key, value in _settings_fields(settings, **kwargs).items():
        if not name:
            raise CheetahError("cheetah RESET_DB needs an explicit database name to carry settings")
        parts.append(f"{key}={value}")
    response = conn.send(" ".join(parts))
    if not response.ok:
        raise CheetahError(f"cheetah RESET_DB failed: {response.reason}", response=response)
    return response


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

"""``CheetahDatabase`` — a connected, version-guarded handle on one database.

The layers below this one are deliberately free functions over a connection:
:mod:`~cheetah_db.kv`, :mod:`~cheetah_db.graph` and :mod:`~cheetah_db.protocol`
know nothing about who is calling them. That is the right shape for a protocol
binding and the wrong shape for an application, which ends up rewriting the same
handful of things around them every time — client/pool construction, a connect
that refuses a database written by an incompatible codec, a close that only
closes what it owns, a read-modify-write that cannot interleave with itself,
collision-checked id allocation, and payload accounting per namespace.

So this class holds exactly those, and nothing about any particular schema. It
is meant to be **subclassed**: a store extends it, adds the methods that know
its own namespaces, and inherits the plumbing. Everything it does is still
reachable as a free function for callers who would rather compose.

The commit protocol worth copying, since Cheetah has no transaction spanning
several writes: write the record incomplete, write its parts, then flip a
completion marker, and have every reader ignore records without it. This class
does not impose it — a marker means nothing without a reader that respects it —
but :meth:`mutate_json` and :meth:`allocate_random_id` are the pieces such a
protocol needs.
"""

from __future__ import annotations

import json
import secrets
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from . import graph as graph_ops
from . import kv
from .client import CheetahClient, CheetahError, ThreadLocalClientPool
from .protocol import ScanItem, decode_item_payload

__all__ = ["CheetahDatabase", "DEFAULT_SCAN_LIMIT", "DEFAULT_WRITE_BATCH_SIZE", "hydrate_json"]

DEFAULT_SCAN_LIMIT = 500
DEFAULT_WRITE_BATCH_SIZE = 256


def hydrate_json(item: ScanItem) -> Any:
    """Decode a payload hydrated by the ``continuations`` reducer during a scan."""
    payload = decode_item_payload(item)
    if payload is None:
        raise CheetahError(f"cheetah scan did not hydrate {item.text or '(unknown key)'}")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheetahError(f"cheetah payload at {item.text} is not JSON: {exc}") from exc


class CheetahDatabase:
    """Subclass this; add the methods that know your namespaces."""

    def __init__(
        self,
        *,
        client: CheetahClient | None = None,
        pool: ThreadLocalClientPool | None = None,
        host: str = "127.0.0.1",
        port: int = 4455,
        database: str | None = None,
        database_options: Mapping[str, Any] | None = None,
        timeout: float = 1.0,
        idle_grace: float | None = None,
        layout: Mapping[str, Any] | None = None,
        write_batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
        scan_limit: int = DEFAULT_SCAN_LIMIT,
        now: Callable[[], datetime] | None = None,
        random_int: Callable[[int], int] | None = None,
    ) -> None:
        self.database_name = database
        self._owns_pool = pool is None
        if pool is not None:
            self._pool = pool
        else:
            def factory() -> CheetahClient:
                return CheetahClient(
                    host,
                    port,
                    database=database or "default",
                    database_options=database_options,
                    timeout=timeout,
                    idle_grace=idle_grace,
                )

            self._pool = ThreadLocalClientPool(factory, warm_client=client)
        self.layout = dict(layout) if layout else None
        self.write_batch_size = max(1, int(write_batch_size))
        self.scan_limit = max(1, int(scan_limit))
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._random_int = random_int or (lambda maximum: secrets.randbelow(maximum - 1) + 1)
        self.connected = False
        # One lock per key, so two read-modify-writes of the same record cannot
        # interleave within this process. See `mutate_json`.
        self._mutation_locks: "defaultdict[str, threading.Lock]" = defaultdict(threading.Lock)
        self._connect_lock = threading.Lock()

    # -- connection ----------------------------------------------------- #
    @property
    def conn(self) -> CheetahClient:
        """This thread's connection. Acquiring it connects on first use."""
        return self._pool.acquire()

    def connect(self) -> "CheetahDatabase":
        with self._connect_lock:
            if self.connected:
                return self
            conn = self._pool.acquire()
            self.assert_layout(conn)
            self.on_connect(conn)
            self.connected = True
        return self

    def assert_layout(self, conn: Any) -> None:
        """Write the layout marker on an empty database, or refuse an incompatible one.

        Failing loudly here is the point. A trie key layout is not
        self-describing: a codec that changed its segment widths reads an older
        database as a set of keys that simply do not match anything, which looks
        like an empty result rather than an error.
        """
        if not self.layout:
            return
        key = self.layout["key"]
        version = int(self.layout["version"])
        label = self.layout.get("label", "key layout")
        stored = kv.get_value(conn, key)
        if stored is None:
            kv.put_value(conn, key, str(version), upsert=True)
            return
        try:
            stored_version = int(stored)
        except ValueError:
            stored_version = -1
        if stored_version != version:
            raise CheetahError(
                f"cheetah {label} {stored!r} is incompatible with codec {version}; "
                "re-ingest into a fresh database"
            )

    def on_connect(self, conn: Any) -> None:
        """Subclass hook: runs on this thread's connection after the layout check."""

    def clear_caches(self) -> None:
        """Subclass hook: drop every in-memory cache of database content."""

    def close(self) -> None:
        if not self._owns_pool:
            return
        self._pool.close_all()
        self.connected = False

    def reset(self) -> "CheetahDatabase":
        """Drop and recreate this store's database.

        Destructive and immediate: ``RESET_DB`` recreates the directory, so
        everything in it is gone — which is what a run establishing a corpus
        from scratch wants, and never what a run adding to one wants.

        Every pooled connection is dropped afterwards, not just the one that
        issued the reset. That follows from the server's own model: database
        selection is per-connection, and resetting closes the database and drops
        it from the engine registry while only the issuing socket is re-pointed
        at the fresh one — so every other socket holds a pointer to a closed
        database. Reconnecting removes the question.
        """
        name = self.database_name
        if not name:
            raise CheetahError("cannot reset a store with no database name")
        response = self.conn.send(f"RESET_DB {name}")
        if not response.ok:
            raise CheetahError(f"cheetah RESET_DB {name} failed: {response.reason}", response=response)
        # Everything cached described the database that just ceased to exist.
        self.clear_caches()
        self._pool.close_all()
        self.connected = False
        return self.connect()

    # -- values --------------------------------------------------------- #
    def get_value(self, key: str) -> str | None:
        return kv.get_value(self.conn, key)

    def put_value(self, key: str, payload: str, *, upsert: bool = False) -> int:
        return kv.put_value(self.conn, key, payload, upsert=upsert)

    def get_json(self, key: str) -> Any:
        return kv.get_json(self.conn, key)

    def put_json(self, key: str, value: Any, *, upsert: bool = False) -> int:
        return kv.put_json(self.conn, key, value, upsert=upsert)

    def put_json_batched(self, entries: Sequence[Any], **options: Any) -> int:
        """``entries`` as ``(key, payload)``, written in ``write_batch_size`` pages."""
        written = 0
        entries = list(entries)
        for start in range(0, len(entries), self.write_batch_size):
            page = entries[start : start + self.write_batch_size]
            kv.put_json_batch(self.conn, page, **options)
            written += len(page)
        return written

    def delete_pair(self, key: str) -> int:
        return kv.delete_pair(self.conn, key)

    def scan(self, prefix: str, **options: Any) -> Iterator[ScanItem]:
        options.setdefault("limit", self.scan_limit)
        return kv.scan_prefix(self.conn, prefix, **options)

    def scan_all(self, prefix: str, **options: Any) -> list[ScanItem]:
        options.setdefault("limit", self.scan_limit)
        return kv.scan_all(self.conn, prefix, **options)

    def scan_json(self, prefix: str, **options: Any) -> Iterator[tuple[ScanItem, Any]]:
        """:meth:`scan` with the ``continuations`` reducer, yielding ``(item, value)``.

        One request per page instead of one ``READ`` per row, which is the whole
        reason the reducer exists.
        """
        options["reducer"] = "continuations"
        for item in self.scan(prefix, **options):
            yield item, hydrate_json(item)

    def mutate_json(self, key: str, fallback: Any, mutate: Callable[[Any], Any]) -> Any:
        """Read-modify-write one JSON record, serialized per key.

        Cheetah has no compare-and-swap, so two concurrent increments of the
        same counter would both read the old value and one would be lost. The
        lock makes that impossible within a process; across processes it is
        still a race, and a record written by several writers needs a different
        design.
        """
        with self._mutation_locks[key]:
            conn = self.conn
            current = kv.get_json(conn, key)
            if current is None:
                current = json.loads(json.dumps(fallback))
            updated = mutate(current)
            kv.put_json(conn, key, updated, upsert=True)
            return updated

    # -- ids ------------------------------------------------------------ #
    def allocate_random_id(
        self, key_for: Callable[[int], str], *, maximum: int = 0x100000000, attempts: int = 64
    ) -> int:
        """A random id no record uses yet.

        Random rather than a ``cfg:`` counter because allocation must not depend
        on a single process: separate workers have isolated state, so a
        process-local counter hands the same id to two writers.
        """
        conn = self.conn
        for _ in range(attempts):
            candidate = self._random_int(maximum)
            if kv.pair_get(conn, key_for(candidate)) is None:
                return candidate
        raise CheetahError(f"unable to allocate a collision-free id below {maximum}")

    def timestamp(self) -> str:
        """ISO-8601 stamp from the injected clock."""
        value = self._now()
        if not isinstance(value, datetime):
            value = datetime.fromtimestamp(float(value), tz=timezone.utc)
        return value.isoformat()

    # -- accounting ----------------------------------------------------- #
    def pair_summary(self, prefix: str, depth: int = 1) -> kv.PairSummary:
        return kv.pair_summary(self.conn, prefix, depth=depth)

    def namespace_summary(self, prefixes: Iterable[str]) -> dict[str, Any]:
        """:meth:`pair_summary` over several namespaces, plus their totals."""
        unique = list(dict.fromkeys(prefixes))
        entries = [self.pair_summary(prefix, 1) for prefix in unique]
        return {
            "total_records": sum(entry.count for entry in entries),
            "total_payload_bytes": sum(entry.payload_bytes for entry in entries),
            "namespaces": {
                entry.prefix: {
                    "prefix": entry.prefix,
                    "count": entry.count,
                    "payload_bytes": entry.payload_bytes,
                }
                for entry in entries
            },
        }

    # -- graph ---------------------------------------------------------- #
    def set_node(self, node_id: str, **options: Any) -> Any:
        return graph_ops.set_node(self.conn, node_id, **options)

    def get_node(self, node_id: str) -> Any:
        return graph_ops.get_node(self.conn, node_id)

    def set_edge_batch(self, items: Sequence[Mapping[str, Any]], **options: Any) -> dict[str, int]:
        return graph_ops.edge_set_batch(self.conn, items, **options)

    def degree(self, node_id: str, **options: Any) -> dict[str, float]:
        return graph_ops.degree(self.conn, node_id, **options)

    def recall(self, seeds: Sequence[str], **options: Any) -> list[dict[str, Any]]:
        return graph_ops.recall_batched(self.conn, seeds, **options)

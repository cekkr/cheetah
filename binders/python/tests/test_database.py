"""`CheetahDatabase`: layout guard, serialized mutation, id allocation, accounting."""

from __future__ import annotations

import threading
import unittest
from datetime import datetime, timezone

from cheetah_db import kv
from cheetah_db.client import CheetahError
from cheetah_db.database import CheetahDatabase

from .fakes import FakeCheetahServer, FakeConnection


class _StubPool:
    """A pool over one in-memory connection, so no socket is involved."""

    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.closed = 0

    def acquire(self) -> FakeConnection:
        return self.conn

    def close_all(self) -> None:
        self.closed += 1

    def describe(self) -> str:
        return "fake://memory"


def build(**options) -> tuple[CheetahDatabase, FakeConnection]:
    conn = FakeConnection(options.pop("server", None) or FakeCheetahServer())
    database = CheetahDatabase(pool=_StubPool(conn), database="test", **options)
    return database, conn


class LayoutTests(unittest.TestCase):
    def test_marker_is_written_on_an_empty_database(self) -> None:
        database, conn = build(layout={"key": "cfg:layout", "version": 3})
        database.connect()
        self.assertEqual(kv.get_value(conn, "cfg:layout"), "3")

    def test_an_incompatible_layout_fails_loudly_instead_of_reading_as_empty(self) -> None:
        server = FakeCheetahServer()
        first, _ = build(server=server, layout={"key": "cfg:layout", "version": 1})
        first.connect()
        second, _ = build(server=server, layout={"key": "cfg:layout", "version": 2})
        with self.assertRaises(CheetahError) as raised:
            second.connect()
        self.assertIn("re-ingest into a fresh database", str(raised.exception))

    def test_on_connect_hook_runs_once(self) -> None:
        calls: list[int] = []

        class Store(CheetahDatabase):
            def on_connect(self, conn) -> None:
                calls.append(1)

        conn = FakeConnection()
        store = Store(pool=_StubPool(conn), database="test")
        store.connect()
        store.connect()
        self.assertEqual(len(calls), 1)


class MutationTests(unittest.TestCase):
    def test_concurrent_increments_of_one_record_do_not_lose_a_write(self) -> None:
        database, conn = build()
        database.connect()

        def bump() -> None:
            for _ in range(20):
                database.mutate_json("counter", {"n": 0}, lambda doc: {"n": doc["n"] + 1})

        threads = [threading.Thread(target=bump) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(kv.get_json(conn, "counter")["n"], 80)

    def test_fallback_is_copied_not_shared_between_records(self) -> None:
        database, _ = build()
        database.connect()
        database.mutate_json("a", {"items": []}, lambda doc: {**doc, "items": doc["items"] + [1]})
        second = database.mutate_json(
            "b", {"items": []}, lambda doc: {**doc, "items": doc["items"] + [2]}
        )
        self.assertEqual(second["items"], [2])


class IdAndAccountingTests(unittest.TestCase):
    def test_allocation_skips_ids_already_in_use(self) -> None:
        candidates = iter([7, 7, 9])
        database, conn = build(random_int=lambda _maximum: next(candidates))
        database.connect()
        kv.put_value(conn, "a:7", "taken")
        self.assertEqual(database.allocate_random_id(lambda candidate: f"a:{candidate}"), 9)

    def test_allocation_gives_up_rather_than_looping_forever(self) -> None:
        database, conn = build(random_int=lambda maximum: 1)
        database.connect()
        kv.put_value(conn, "a:1", "taken")
        with self.assertRaises(CheetahError):
            database.allocate_random_id(lambda candidate: f"a:{candidate}", attempts=3)

    def test_namespace_summary_totals_every_prefix_once(self) -> None:
        database, conn = build()
        database.connect()
        kv.put_values_batch(conn, [("a:1", "one"), ("a:2", "two"), ("b:1", "three")])
        summary = database.namespace_summary(["a:", "b:", "a:"])
        self.assertEqual(summary["total_records"], 3)
        self.assertEqual(summary["namespaces"]["a:"]["count"], 2)

    def test_timestamp_uses_the_injected_clock(self) -> None:
        moment = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
        database, _ = build(now=lambda: moment)
        self.assertEqual(database.timestamp(), moment.isoformat())


class ResetTests(unittest.TestCase):
    def test_reset_drops_the_data_the_caches_and_the_pooled_connections(self) -> None:
        cleared: list[int] = []

        class Store(CheetahDatabase):
            def clear_caches(self) -> None:
                cleared.append(1)

        conn = FakeConnection()
        pool = _StubPool(conn)
        store = Store(pool=pool, database="test")
        store.connect()
        kv.put_value(conn, "a:1", "one")
        store.reset()
        self.assertIsNone(kv.get_value(conn, "a:1"))
        self.assertEqual(cleared, [1])
        self.assertEqual(pool.closed, 1)


class ScanJsonTests(unittest.TestCase):
    def test_scan_json_hydrates_through_the_reducer(self) -> None:
        database, conn = build()
        database.connect()
        kv.put_json_batch(conn, [("d:1", {"n": 1}), ("d:2", {"n": 2})])
        hydrated = [value for _item, value in database.scan_json("d:")]
        self.assertEqual(hydrated, [{"n": 1}, {"n": 2}])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

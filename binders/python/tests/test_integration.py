"""The binder against a live cheetah-server. Gated: needs Go and a spare port.

    CHEETAH_INTEGRATION=1 python3 -m unittest discover -s tests -t .

Everything else in this suite proves the binder's own encodings against a
stand-in. Only this file proves the server answers the way the binder assumes,
which is the half a codec test can never cover.
"""

from __future__ import annotations

import os
import socket
import tempfile
import unittest

from cheetah_db import admin, graph, jobs, kv, records
from cheetah_db.client import CheetahClient
from cheetah_db.database import CheetahDatabase
from cheetah_db.server import start_server
from cheetah_db.vocabulary import TokenVocabulary

INTEGRATION = os.environ.get("CHEETAH_INTEGRATION", "").strip() not in {"", "0", "false", "no"}


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@unittest.skipUnless(INTEGRATION, "set CHEETAH_INTEGRATION=1 to run against a live server")
class LiveServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory(prefix="cheetah-python-binder-")
        cls.server = start_server(
            port=free_port(), cwd=cls._tmp.name, data_dir=os.path.join(cls._tmp.name, "data")
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()
        cls._tmp.cleanup()

    def setUp(self) -> None:
        self.conn = CheetahClient(
            self.server.host, self.server.port, database="binder_test", timeout=5.0
        )
        self.addCleanup(self.conn.close)
        self.assertTrue(self.conn.connect())

    def test_value_round_trip_including_a_comma_and_non_ascii(self) -> None:
        kv.put_json(self.conn, "u:1", {"name": "Ada", "note": "one, two — três"}, upsert=True)
        self.assertEqual(
            kv.get_json(self.conn, "u:1"), {"name": "Ada", "note": "one, two — três"}
        )

    def test_binary_round_trip(self) -> None:
        payload = bytes(range(0, 256))
        kv.put_bytes(self.conn, "b:1", payload, upsert=True)
        self.assertEqual(kv.get_bytes(self.conn, "b:1"), payload)

    def test_batch_write_then_paged_scan(self) -> None:
        entries = [(f"s:{index:04x}", f"value-{index}") for index in range(120)]
        kv.put_values_batch(self.conn, entries)
        items = kv.scan_all(self.conn, "s:", limit=25)
        self.assertEqual(len(items), 120)
        hydrated = kv.scan_all(self.conn, "s:", limit=25, reducer="continuations")
        self.assertEqual(hydrated[0].payload, b"value-0")

    def test_hidden_pairs_are_skipped_unless_asked_for(self) -> None:
        kv.put_values_batch(self.conn, [("h:1", "one")], hidden=True)
        self.assertEqual(kv.scan_all(self.conn, "h:"), [])
        self.assertEqual(len(kv.scan_all(self.conn, "h:", include_hidden=True)), 1)

    def test_graph_write_and_recall(self) -> None:
        graph.set_node(self.conn, "term:rain", labels=["term"], props={"gloss": "water falling"})
        graph.set_node(self.conn, "ctx:weather", labels=["ctx"])
        graph.edge_set_batch(
            self.conn, [{"from": "ctx:weather", "to": "term:rain"}], type="evokes", weight=1.0
        )
        self.assertEqual(graph.get_node(self.conn, "term:rain")["id"], "term:rain")
        self.assertGreaterEqual(graph.degree(self.conn, "ctx:weather")["degree"], 1)
        result = graph.recall(self.conn, ["ctx:weather"], hops=1)
        self.assertTrue(any(a["id"] == "term:rain" for a in result["associations"]))

    def test_detached_reduce_round_trip(self) -> None:
        kv.put_values_batch(self.conn, [(f"j:{index}", f"v{index}") for index in range(5)])
        self.assertTrue(jobs.supports_job_api(self.conn))
        job_id = jobs.submit(self.conn, "PAIR_REDUCE continuations j: 256")
        result = jobs.await_job(self.conn, job_id, poll_interval=0.2, timeout=30)
        self.assertEqual(len(result.items()), 5)

    def test_record_table_round_trip_and_schema_evolution(self) -> None:
        records.define(
            self.conn, "ngram", "cnt:uint:4,prob:float:8,label:string:12", if_not_exists=True
        )
        records.set_row(self.conn, "ngram", "berlin", {"cnt": 42, "prob": 0.25, "label": "city"})
        records.set_row(self.conn, "ngram", "lisbon", {"cnt": 7})
        self.assertEqual(
            records.get_row(self.conn, "ngram", "berlin"),
            {"cnt": 42, "prob": 0.25, "label": "city"},
        )
        # A partial write leaves the other fields alone.
        records.set_row(self.conn, "ngram", "berlin", {"cnt": 43})
        self.assertEqual(records.get_row(self.conn, "ngram", "berlin")["label"], "city")

        # A field added later reads null on the rows that predate it.
        records.alter(self.conn, "ngram", add="novelty:float:8", drop="label")
        self.assertIsNone(records.get_row(self.conn, "ngram", "berlin")["novelty"])
        schema = records.schema(self.conn, "ngram", rows=True)
        self.assertEqual(schema.dead_bytes, 12)
        self.assertEqual(schema.rows, 2)

        compacted, rewritten = records.compact(self.conn, "ngram")
        self.assertEqual(rewritten, 2)
        self.assertEqual(compacted.dead_bytes, 0)
        self.assertEqual(records.get_row(self.conn, "ngram", "berlin")["cnt"], 43)

        rows = list(records.iter_rows(self.conn, "ngram", limit=1))
        self.assertEqual({row.text for row in rows}, {"berlin", "lisbon"})
        self.assertTrue(records.delete_row(self.conn, "ngram", "berlin"))
        self.assertEqual(records.drop_table(self.conn, "ngram"), 1)
        self.assertIsNone(records.schema(self.conn, "ngram"))

    def test_database_creation_carries_its_own_settings(self) -> None:
        name = "binder_adhoc"
        created = admin.create_database(self.conn, name, payload_cache_mb=8)
        self.assertEqual(created["name"], name)
        self.assertEqual(created["settings"]["payload_cache_bytes"], str(8 << 20))
        with self.assertRaises(Exception):
            admin.create_database(self.conn, name)
        listed = {info.name: info for info in admin.list_databases(self.conn)}
        self.assertIn(name, listed)
        self.assertTrue(listed[name].ad_hoc)
        self.assertEqual(listed[name].settings["payload_cache_bytes"], 8 << 20)

    def test_server_gauges_are_readable(self) -> None:
        stats = admin.system_stats(self.conn)
        self.assertGreaterEqual(stats.logical_cores or 0, 1)
        admin.file_checkpoint(self.conn)

    def test_vocabulary_allocates_each_name_once(self) -> None:
        vocabulary = TokenVocabulary(self.conn, counter_key="cfg:vocab_next")
        tokens = vocabulary.tokens_for(["alpha", "beta", "alpha", "gamma"])
        self.assertEqual(tokens[0], tokens[2])
        self.assertEqual(len(set(tokens)), 3)
        self.assertEqual(vocabulary.name_for(tokens[1]), "beta")

    def test_database_layout_guard_and_reset(self) -> None:
        store = CheetahDatabase(
            host=self.server.host,
            port=self.server.port,
            database="binder_layout",
            timeout=5.0,
            layout={"key": "cfg:layout", "version": 1},
        )
        self.addCleanup(store.close)
        store.connect()
        store.put_json("a:1", {"n": 1}, upsert=True)
        self.assertEqual(store.get_json("a:1"), {"n": 1})
        store.reset()
        self.assertIsNone(store.get_json("a:1"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

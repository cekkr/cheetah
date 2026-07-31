"""The two-step write, the batch write, and paged sweeps."""

from __future__ import annotations

import base64
import json
import unittest

from cheetah_db import kv
from cheetah_db.client import CheetahError

from .fakes import FakeConnection


class ValueAndNameTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_put_value_inserts_then_binds(self) -> None:
        abs_key = kv.put_value(self.conn, "a:1", "hello")
        self.assertEqual(kv.get_value(self.conn, "a:1"), "hello")
        self.assertEqual(kv.pair_get(self.conn, "a:1"), abs_key)
        self.assertEqual(
            [command.split(" ", 1)[0] for command in self.conn.commands[:4]],
            ["INSERT:5", "PAIR_SET", "PAIR_GET", "READ"],
        )

    def test_upsert_edits_in_place_and_keeps_the_absolute_key(self) -> None:
        first = kv.put_value(self.conn, "a:1", "hello", upsert=True)
        second = kv.put_value(self.conn, "a:1", "goodbye", upsert=True)
        self.assertEqual(first, second)
        self.assertEqual(kv.get_value(self.conn, "a:1"), "goodbye")

    def test_unbound_name_reads_as_none_rather_than_raising(self) -> None:
        self.assertIsNone(kv.get_value(self.conn, "missing:1"))
        self.assertIsNone(kv.pair_get(self.conn, "missing:1"))

    def test_json_round_trip_survives_commas_inside_the_payload(self) -> None:
        kv.put_json(self.conn, "a:1", {"name": "Ada", "note": "one, two"}, upsert=True)
        self.assertEqual(kv.get_json(self.conn, "a:1"), {"name": "Ada", "note": "one, two"})

    def test_bytes_round_trip_through_the_base64_transport(self) -> None:
        payload = bytes(range(0, 32))
        kv.put_bytes(self.conn, "b:1", payload)
        self.assertEqual(kv.get_bytes(self.conn, "b:1"), payload)

    def test_empty_and_multiline_payloads_are_refused_before_the_wire(self) -> None:
        with self.assertRaises(CheetahError):
            kv.put_value(self.conn, "a:1", "")
        with self.assertRaises(CheetahError):
            kv.put_value(self.conn, "a:1", "two\nlines")

    def test_delete_pair_uses_the_micro_dialect_and_escapes_the_key(self) -> None:
        kv.put_value(self.conn, "x-ray:1", "payload")
        self.assertEqual(kv.delete_pair(self.conn, "x-ray:1"), 1)
        self.assertIn("DEL pairs key=x" + b"x-ray:1".hex(), self.conn.commands)
        self.assertEqual(kv.delete_pair(self.conn, "x-ray:1"), 0)

    def test_purge_can_leave_the_payloads_readable(self) -> None:
        abs_key = kv.put_value(self.conn, "p:1", "payload")
        self.assertEqual(kv.pair_purge(self.conn, "p:", payloads=False), 1)
        self.assertIsNone(kv.pair_get(self.conn, "p:1"))
        self.assertEqual(kv.read_absolute_key(self.conn, abs_key), "payload")


class BatchWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_batch_is_one_request_for_the_whole_page(self) -> None:
        entries = [(f"a:{index}", f"value-{index}") for index in range(5)]
        kv.put_values_batch(self.conn, entries)
        self.assertEqual(len(self.conn.commands), 1)
        self.assertTrue(self.conn.commands[0].startswith("PAIR_PUT_BATCH items="))
        for key, value in entries:
            self.assertEqual(kv.get_value(self.conn, key), value)

    def test_assigned_keys_are_returned_only_when_asked_for(self) -> None:
        entries = [("a:1", "one"), ("a:2", "two")]
        self.assertEqual(kv.put_values_batch(self.conn, entries), [])
        keys = kv.put_values_batch(self.conn, entries, want_keys=True)
        self.assertEqual(len(keys), 2)
        self.assertTrue(all(isinstance(key, int) for key in keys))

    def test_a_payload_that_starts_with_x_is_hex_escaped(self) -> None:
        # parseValue reads a leading `x` as hex on batch items too, so a base64
        # payload beginning with `x` would be stored as different bytes.
        kv.put_values_batch(self.conn, [("a:1", "xyzzy")])
        items = json.loads(
            base64.b64decode(self.conn.commands[0].split("items=", 1)[1]).decode("utf-8")
        )
        self.assertEqual(items[0]["v"], "x" + b"xyzzy".hex())
        self.assertEqual(kv.get_value(self.conn, "a:1"), "xyzzy")

    def test_a_partially_applied_batch_raises_instead_of_returning_a_count(self) -> None:
        conn = FakeConnection()

        def refuse(line: str) -> str:
            return "SUCCESS,command=PAIR_PUT_BATCH,requested=2,applied=1,failed=1,first_error=item_1:_invalid_value"

        conn.server._do_pair_put_batch = refuse  # type: ignore[assignment]
        with self.assertRaises(CheetahError) as raised:
            kv.put_values_batch(conn, [("a:1", "one"), ("a:2", "two")])
        self.assertIn("applied 1/2", str(raised.exception))

    def test_json_batch_writes_readable_documents(self) -> None:
        kv.put_json_batch(self.conn, [("a:1", {"n": 1}), ("a:2", {"n": 2})])
        self.assertEqual(kv.get_json(self.conn, "a:2"), {"n": 2})


class ScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()
        kv.put_values_batch(self.conn, [(f"s:{index:04x}", f"v{index}") for index in range(10)])

    def test_paging_resumes_from_the_cursor_verbatim(self) -> None:
        items = kv.scan_all(self.conn, "s:", limit=3)
        self.assertEqual(len(items), 10)
        cursor_commands = [
            command for command in self.conn.commands if command.startswith("PAIR_SCAN")
        ]
        self.assertGreater(len(cursor_commands), 1)
        self.assertIn("x", cursor_commands[1].split()[-1])

    def test_max_items_bounds_an_open_ended_sweep(self) -> None:
        self.assertEqual(len(kv.scan_all(self.conn, "s:", limit=3, max_items=4)), 4)

    def test_reducer_pages_hydrate_their_payloads(self) -> None:
        items = kv.scan_all(self.conn, "s:", limit=100, reducer="continuations")
        self.assertEqual(items[0].payload, b"v0")

    def test_summary_reports_records_and_payload_bytes(self) -> None:
        summary = kv.pair_summary(self.conn, "s:")
        self.assertEqual(summary.count, 10)
        self.assertGreater(summary.payload_bytes, 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

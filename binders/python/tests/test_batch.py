"""``BATCH``: one command, N argument sets, one round trip.

Two halves. The builders are pure, so the first asserts the exact line that
goes on the wire — the part a client gets silently wrong. The second drives the
collector and the auto-batch policy, which is where the interesting properties
live: one response per caller, ordering preserved, and nothing deferred that
was not issued as bulk work.
"""

from __future__ import annotations

import base64
import json
import logging
import unittest

from cheetah_db import batch as batch_mod
from cheetah_db.batch import (
    AutoBatchPolicy,
    BatchCollector,
    CheetahBatchError,
    DeferredResponse,
    build_batch,
    decode_result_lines,
    parse_batch_response,
    run_batch,
    split_command_line,
)
from cheetah_db.client import CheetahClient
from cheetah_db.protocol import parse_response

from .fakes import FakeCheetahServer, FakeConnection, FakeServerSocket


def field_of(line: str, name: str) -> str | None:
    for token in line.split():
        key, sep, value = token.partition("=")
        if sep and key == name:
            return value
    return None


def items_of(line: str) -> list:
    return json.loads(base64.b64decode(field_of(line, "items")).decode("utf-8"))


def payload_of(value) -> str:
    return base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")


class BuildBatchTests(unittest.TestCase):
    def test_items_and_continue_on_error_default_travel_explicitly(self) -> None:
        line = build_batch("PAIR_SET", ["ctx:a 1", "ctx:b 2"])
        self.assertTrue(line.startswith("BATCH PAIR_SET items="))
        self.assertEqual(items_of(line), ["ctx:a 1", "ctx:b 2"])
        self.assertEqual(field_of(line, "continue_on_error"), "1")
        self.assertNotIn("results=", line)

    def test_non_default_and_shared_modifiers_are_carried(self) -> None:
        line = build_batch(
            "GRAPH_EDGE_SET",
            [{"from": "a", "to": "b"}],
            continue_on_error=False,
            results=False,
            detached=True,
            shared={"type": "knows", "weight": 2},
        )
        self.assertEqual(field_of(line, "continue_on_error"), "0")
        self.assertEqual(field_of(line, "results"), "0")
        self.assertEqual(field_of(line, "async"), "1")
        self.assertEqual(field_of(line, "type"), "knows")

    def test_the_targets_the_server_refuses_are_refused_here_first(self) -> None:
        for target in ("BATCH", "JOB", "DATABASE", "RESET_DB", "EXIT"):
            with self.assertRaises(CheetahBatchError):
                build_batch(target, ["x"])

    def test_an_empty_list_and_a_shared_value_with_whitespace_are_refused(self) -> None:
        with self.assertRaises(CheetahBatchError):
            build_batch("PAIR_SET", [])
        with self.assertRaises(CheetahBatchError):
            build_batch("GRAPH_EDGE_SET", [{"from": "a"}], shared={"note": "two words"})

    def test_split_command_line(self) -> None:
        self.assertEqual(split_command_line("pair_set ctx:a 42"), ("PAIR_SET", "ctx:a 42"))
        self.assertEqual(split_command_line("SYSTEM_STATS"), ("SYSTEM_STATS", ""))


class ParseBatchTests(unittest.TestCase):
    def test_plain_lines_and_the_base64_fallback_decode_alike(self) -> None:
        self.assertEqual(
            decode_result_lines({"payload": payload_of(["SUCCESS,pair_set", None])}),
            ["SUCCESS,pair_set", None],
        )
        raw = "SUCCESS,size=1,value=\xff"
        binary = {
            "payload": payload_of([base64.b64encode(raw.encode("latin1")).decode("ascii")]),
            "results_encoding": "base64",
        }
        self.assertEqual(decode_result_lines(binary), [raw])

    def test_the_aggregate_and_the_per_item_responses_are_separate(self) -> None:
        parsed = parse_batch_response(
            parse_response(
                "SUCCESS,command=BATCH,target=PAIR_SET,requested=3,applied=2,failed=1,"
                "first_error=item_1:invalid_absolute_key_format,payload="
                + payload_of(["SUCCESS,pair_set", "ERROR,invalid_absolute_key_format", None])
            )
        )
        self.assertEqual((parsed.requested, parsed.applied, parsed.failed), (3, 2, 1))
        self.assertFalse(parsed.ok)
        self.assertEqual(parsed.results[1].error, "invalid_absolute_key_format")
        self.assertIsNone(parsed.results[2])
        with self.assertRaises(CheetahBatchError):
            parsed.raise_for_failures()


class RunBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_one_round_trip_binds_every_pair(self) -> None:
        keys = [int(self.conn.send(f"INSERT payload-{i}").field_value("key")) for i in range(3)]
        result = run_batch(
            self.conn, "PAIR_SET", [f"ctx:{i} {key}" for i, key in enumerate(keys)]
        )
        self.assertEqual((result.requested, result.applied, result.failed), (3, 3, 0))
        self.assertTrue(all(item.ok for item in result.results))
        # One BATCH line, not three PAIR_SETs from the caller.
        self.assertEqual(len([c for c in self.conn.commands if c.startswith("BATCH ")]), 1)
        for index, key in enumerate(keys):
            self.assertEqual(self.conn.send(f"PAIR_GET ctx:{index}").field_value("key"), str(key))

    def test_a_refused_request_raises_rather_than_reporting_zero(self) -> None:
        class Refusing:
            def send(self, _line: str):
                return parse_response("ERROR,batch_too_many_items (max 10000, got 20000)")

        with self.assertRaises(CheetahBatchError) as raised:
            run_batch(Refusing(), "PAIR_SET", ["ctx:a 1"])
        self.assertIn("batch_too_many_items", str(raised.exception))


class CollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sent: list[str] = []
        self.server = FakeCheetahServer()

        def send(line: str):
            self.sent.append(line)
            return parse_response(self.server.execute(line))

        self.send = send

    def test_a_queue_of_one_is_sent_as_itself(self) -> None:
        collector = BatchCollector(self.send)
        handle = collector.add("PAIR_SET ctx:a 1")
        self.assertTrue(handle.pending)
        collector.flush()
        self.assertEqual(self.sent, ["PAIR_SET ctx:a 1"])
        self.assertTrue(handle.ok)

    def test_several_become_one_batch_and_each_caller_gets_its_own_response(self) -> None:
        collector = BatchCollector(self.send)
        handles = [collector.add(f"PAIR_SET ctx:{index} {index + 1}") for index in range(4)]
        collector.flush()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(items_of(self.sent[0]), [f"ctx:{i} {i + 1}" for i in range(4)])
        self.assertTrue(all(handle.ok for handle in handles))
        self.assertEqual(collector.stats["batched"], 4)

    def test_touching_a_handle_flushes_the_queue(self) -> None:
        collector = BatchCollector(self.send)
        first = collector.add("PAIR_SET ctx:a 1")
        collector.add("PAIR_SET ctx:b 2")
        self.assertTrue(first.ok)  # resolving forces the flush
        self.assertEqual(len(self.sent), 1)

    def test_a_different_command_closes_the_open_queue(self) -> None:
        collector = BatchCollector(self.send)
        collector.add("PAIR_SET ctx:a 1")
        collector.add("PAIR_SET ctx:b 2")
        collector.add("PAIR_GET ctx:a")
        collector.flush()
        self.assertEqual(len(self.sent), 2)
        self.assertTrue(self.sent[0].startswith("BATCH PAIR_SET"))
        self.assertTrue(self.sent[1].startswith("PAIR_GET"))

    def test_max_size_flushes_on_its_own(self) -> None:
        collector = BatchCollector(self.send, max_size=2)
        for index in range(4):
            collector.add(f"PAIR_SET ctx:{index} {index + 1}")
        self.assertEqual(len(self.sent), 2)

    def test_a_failure_nobody_awaited_is_still_reported(self) -> None:
        reported: list[tuple[str, Exception]] = []

        def refuse(_line: str):
            return parse_response("ERROR,batch_too_many_items")

        collector = BatchCollector(refuse, on_error=lambda *args: reported.append(args))
        handles = [collector.add(f"PAIR_SET ctx:{i} {i}") for i in range(2)]
        collector.flush()
        self.assertEqual(len(reported), 1)
        with self.assertRaises(CheetahBatchError):
            handles[0].resolve()


class AutoBatchPolicyTests(unittest.TestCase):
    def test_unknown_settings_are_refused_rather_than_ignored(self) -> None:
        self.assertEqual(AutoBatchPolicy().with_overrides({"mode": "off"}).mode, "off")
        with self.assertRaises(ValueError):
            AutoBatchPolicy().with_overrides({"treshold": 4})

    def test_a_command_goes_hot_only_after_the_threshold(self) -> None:
        window = batch_mod.HotCommandWindow(AutoBatchPolicy(threshold=3, window=10.0))
        self.assertFalse(window.observe("PAIR_SET", now=0.0))
        self.assertFalse(window.observe("PAIR_SET", now=0.01))
        self.assertTrue(window.observe("PAIR_SET", now=0.02))

    def test_an_idle_gap_cools_a_command_back_down(self) -> None:
        policy = AutoBatchPolicy(threshold=2, window=10.0, idle=1.0)
        window = batch_mod.HotCommandWindow(policy)
        window.observe("PAIR_SET", now=0.0)
        self.assertTrue(window.observe("PAIR_SET", now=0.1))
        self.assertFalse(window.observe("PAIR_SET", now=5.0))

    def test_the_excluded_commands_are_never_batchable(self) -> None:
        window = batch_mod.HotCommandWindow(AutoBatchPolicy())
        for name in batch_mod.AUTO_BATCH_EXCLUDED:
            self.assertFalse(window.batchable(name), name)
        self.assertTrue(window.batchable("PAIR_SET"))

    def test_an_allowlist_narrows_it_further(self) -> None:
        window = batch_mod.HotCommandWindow(AutoBatchPolicy(commands=("PAIR_SET",)))
        self.assertTrue(window.batchable("PAIR_SET"))
        self.assertFalse(window.batchable("PAIR_GET"))

    def test_advise_says_it_once(self) -> None:
        window = batch_mod.HotCommandWindow(AutoBatchPolicy())
        with self.assertLogs("cheetah_db.batch", level=logging.INFO) as captured:
            window.advise_once("PAIR_SET")
            window.advise_once("PAIR_SET")
        self.assertEqual(len(captured.records), 1)


class ClientBatchingTests(unittest.TestCase):
    """The client-level policy, over a real socket."""

    def setUp(self) -> None:
        self.server = FakeServerSocket()
        self.addCleanup(self.server.close)

    def client(self, **options):
        options.setdefault("timeout", 1.0)
        client = CheetahClient(self.server.host, self.server.port, **options)
        self.addCleanup(client.close)
        return client

    @property
    def commands(self) -> list[str]:
        return self.server.server.commands

    def test_by_default_nothing_about_the_wire_changes(self) -> None:
        client = self.client()
        for index in range(20):
            self.assertTrue(client.execute("PAIR_SET", f"ctx:{index}", index + 1).ok)
        self.assertEqual([c for c in self.commands if c.startswith("BATCH ")], [])

    def test_a_batching_block_coalesces_and_flushes_on_exit(self) -> None:
        client = self.client()
        with client.batching():
            handles = [client.execute("PAIR_SET", f"ctx:{i}", i + 1) for i in range(4)]
            self.assertTrue(all(handle.pending for handle in handles))
            self.assertEqual([c for c in self.commands if c.startswith("BATCH ")], [])
        batches = [c for c in self.commands if c.startswith("BATCH ")]
        self.assertEqual(len(batches), 1)
        self.assertEqual(items_of(batches[0]), [f"ctx:{i} {i + 1}" for i in range(4)])
        self.assertTrue(all(handle.ok for handle in handles))

    def test_a_command_that_cannot_join_the_queue_does_not_overtake_it(self) -> None:
        client = self.client()
        with client.batching():
            client.execute("PAIR_SET", "ctx:a", 1)
            client.execute("PAIR_SET", "ctx:b", 2)
            client.send("SYSTEM_STATS")
        # The fake re-enters `execute` per item, so the log holds the batch's
        # own items too; what matters is that the BATCH line was written before
        # SYSTEM_STATS, and that nothing of the queue leaked out ahead of it.
        issued = [c for c in self.commands if not c.startswith("DATABASE")]
        self.assertTrue(issued[0].startswith("BATCH PAIR_SET"))
        self.assertEqual(issued[-1], "SYSTEM_STATS")

    def test_deferred_mode_starts_batching_once_a_command_is_hot(self) -> None:
        client = self.client(auto_batch={"mode": "deferred", "threshold": 3, "window": 60.0})
        for index in range(6):
            client.execute("PAIR_SET", f"ctx:{index}", index + 1)
        # The tail of a deferred run stays queued until something asks for it.
        client.flush()
        batches = [c for c in self.commands if c.startswith("BATCH ")]
        self.assertEqual(len(batches), 1)
        self.assertEqual(items_of(batches[0]), [f"ctx:{i} {i + 1}" for i in range(2, 6)])

    def test_off_means_off(self) -> None:
        client = self.client(auto_batch={"mode": "off"})
        with self.assertLogs("cheetah_db.batch", level=logging.INFO) as captured:
            logging.getLogger("cheetah_db.batch").info("marker")
            for index in range(20):
                client.execute("PAIR_SET", f"ctx:{index}", index + 1)
        self.assertEqual([record.getMessage() for record in captured.records], ["marker"])
        self.assertEqual([c for c in self.commands if c.startswith("BATCH ")], [])


class DeferredResponseTests(unittest.TestCase):
    def test_it_behaves_like_the_response_it_will_become(self) -> None:
        collector = BatchCollector(lambda line: parse_response("SUCCESS,key=7"))
        handle = collector.add("PAIR_GET ctx:a")
        self.assertIsInstance(handle, DeferredResponse)
        self.assertTrue(handle)  # __bool__ resolves
        self.assertEqual(handle.field_value("key"), "7")
        self.assertFalse(handle.pending)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

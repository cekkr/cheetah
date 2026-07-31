"""Graph encodings, recall bounds, and the batched merge."""

from __future__ import annotations

import base64
import json
import unittest

from cheetah_db import graph
from cheetah_db.client import CheetahError

from .fakes import FakeConnection


class EncodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_props_travel_base64_because_the_dialect_splits_on_whitespace(self) -> None:
        graph.set_node(self.conn, "term:rain", props={"gloss": "water falling"})
        line = self.conn.commands[-1]
        encoded = line.split("props=", 1)[1].split(" ")[0]
        self.assertEqual(
            json.loads(base64.b64decode(encoded).decode("utf-8")), {"gloss": "water falling"}
        )
        self.assertEqual(self.conn.server.nodes["term:rain"]["props"], {"gloss": "water falling"})

    def test_an_id_that_is_not_a_token_is_refused_rather_than_truncated(self) -> None:
        for bad in ("two words", "a,b", "  "):
            with self.assertRaises(CheetahError):
                graph.set_node(self.conn, bad)

    def test_labels_join_with_commas(self) -> None:
        graph.set_node(self.conn, "n:1", labels=["dbslm_term", "lexical"])
        self.assertIn("labels=dbslm_term,lexical", self.conn.commands[-1])

    def test_missing_node_reads_as_none_not_as_a_failure(self) -> None:
        self.assertIsNone(graph.get_node(self.conn, "n:absent"))

    def test_references_can_be_cleared_explicitly(self) -> None:
        graph.set_node(self.conn, "n:1", references=[{"id": "s1", "text": "A sentence."}])
        self.assertEqual(len(self.conn.server.nodes["n:1"]["references"]), 1)
        graph.set_node(self.conn, "n:1", references="-")
        self.assertEqual(self.conn.server.nodes["n:1"]["references"], [])

    def test_edge_batch_reports_the_server_accounting(self) -> None:
        result = graph.edge_set_batch(
            self.conn,
            [{"from": "a", "to": "b"}, {"from": "a", "to": "c"}],
            type="evokes",
        )
        self.assertEqual(result["requested"], 2)
        self.assertEqual(result["applied"], 2)
        self.assertIn("type=evokes", self.conn.commands[-1])

    def test_degree_of_an_unwritten_node_is_zero(self) -> None:
        self.assertEqual(graph.degree(self.conn, "n:absent")["degree"], 0)


class RecallTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_bounds_are_clamped_to_the_server_maxima(self) -> None:
        graph.recall(self.conn, ["a"], hops=99, branch_limit=99999, budget=10**9)
        line = self.conn.commands[-1]
        self.assertIn(f"hops={graph.MAX_RECALL_HOPS}", line)
        self.assertIn(f"branch_limit={graph.MAX_RECALL_BRANCH}", line)
        self.assertIn(f"budget={graph.MAX_RECALL_BUDGET}", line)

    def test_spaced_seeds_travel_base64_prefixed(self) -> None:
        graph.recall(self.conn, ["heavy rain", "storm"])
        seeds = self.conn.commands[-1].split("seeds=", 1)[1].split(" ")[0]
        self.assertTrue(seeds.startswith("base64:"))
        decoded = base64.b64decode(seeds[len("base64:") :]).decode("utf-8")
        self.assertEqual(decoded, "heavy rain,storm")

    def test_references_ask_for_seed_nodes_so_their_sentences_hydrate(self) -> None:
        graph.recall(self.conn, ["a"], references=True, reference_limit=4, include_seeds=True)
        line = self.conn.commands[-1]
        self.assertIn("references=1", line)
        self.assertIn("reference_limit=4", line)
        self.assertIn("include_seeds=1", line)

    def test_more_than_the_seed_cap_is_refused_by_recall_and_batched_by_recall_batched(self) -> None:
        seeds = [f"s{index}" for index in range(graph.MAX_RECALL_SEEDS + 5)]
        with self.assertRaises(CheetahError):
            graph.recall(self.conn, seeds)
        merged = graph.recall_batched(self.conn, seeds)
        self.assertEqual(len(merged), len(seeds))
        recall_calls = [c for c in self.conn.commands if c.startswith("GRAPH_RECALL")]
        self.assertEqual(len(recall_calls), 2)

    def test_batched_scores_combine_with_the_same_noisy_or_the_server_uses(self) -> None:
        conn = FakeConnection()
        payload = base64.b64encode(
            json.dumps(
                {
                    "seeds": [],
                    "associations": [
                        {
                            "id": "hit",
                            "score": 0.5,
                            "source_count": 1,
                            "sources": [{"seed": "a", "activation": 0.5}],
                        }
                    ],
                }
            ).encode("utf-8")
        ).decode("ascii")
        conn.server._do_graph_recall = lambda rest: f"SUCCESS,count=1,payload={payload}"  # type: ignore[assignment]
        merged = graph.recall_batched(conn, [f"s{index}" for index in range(40)])
        # 1 - (1-0.5)(1-0.5) = 0.75 across the two batches, source counts summed.
        self.assertAlmostEqual(merged[0]["score"], 0.75)
        self.assertEqual(merged[0]["source_count"], 2)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

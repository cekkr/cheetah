# To do:
- Persist cluster fork overrides and gossip snapshots to disk so scheduler reassignments survive restarts and peers can recover state after downtime.
- Extend the cluster messenger to ship actual fork data (trie payloads + prediction tables) when reassigning shards, not just metadata.
- Add optional reducer digests (entropy / CDF / rolling hashes) and trie-level rolling-hash mirrors for fast Top-K / similarity scans.
- Wire the `demo/graph-nell` end-to-end execution test (`CHEETAH_NELL_E2E=1`) into CI, and optionally add a large-slice variant that runs a bounded pass over the real NELL dataset when it is present.

# Done (implemented + verified by tests — do not re-add):
- Multi-hop `GRAPH_QUERY` with branch pruning + early-stop cost limits — `graph.go`, `TestGraphQueryMultiHopBoundsAndCost`.
- Graph reducers `degree` / `triangle` / `pagerank_seed` streaming from `adj/out`/`adj/in` — `reducers.go`, `TestGraphReducersDegreeTriangleAndPageRankSeed`.
- Edge-property secondary indexes (`graph/idx/<prop>/<value>/...`) for `WHERE edge.props.*` — `graph.go`, `TestGraphPropertySecondaryIndexAndPredicate`.
- Real-execution graph pipeline test: boots the actual server over TCP and drives the full `demo/graph-nell` ingest → query → predict flow — `demo/graph-nell/main_test.go` (`TestGraphNELLEndToEnd`), plus unit coverage for the demo's evaluation/loader math.

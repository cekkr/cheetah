# To do:
- Persist cluster fork overrides and gossip snapshots to disk so scheduler reassignments survive restarts and peers can recover state after downtime.
- Extend the cluster messenger to ship actual fork data (trie payloads + prediction tables) when reassigning shards, not just metadata.
- Extend `GRAPH_QUERY` from single-hop patterns to bounded multi-hop traversals with branch pruning + early-stop cost limits.
- Add graph-centric reducers (`degree`, `triangle`, `pagerank_seed`) that stream from `adj/out`/`adj/in` prefixes without full edge hydration.
- Introduce optional edge-property secondary indexes (`graph/idx/<prop>/<value>/...`) for fast `WHERE edge.props.*` filtering.

# To do:
- Persist cluster fork overrides and gossip snapshots to disk so scheduler reassignments survive restarts and peers can recover state after downtime.
- Extend the cluster messenger to ship actual fork data (trie payloads + prediction tables) when reassigning shards, not just metadata.
- Wire prediction-table maintenance into ingest/training so context matrices refresh automatically without manual CLI commands.

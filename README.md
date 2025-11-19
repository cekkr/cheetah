# cheetah-db

High-throughput key/value store with a trie-backed pair table purpose-built for statistical data
pipelines. cheetah-db ingests byte-encoded contexts, n-gram payloads, and arbitrary binary blobs,
keeps them partitioned by value size, and exposes TCP + CLI commands that stream results with
bounded memory use. The engine targets workloads where millions of probabilities, counters, or
other dense analytical slices must be served with predictable latency.

## Highlights

- **Byte-faithful layout.** Every entry is cataloged by byte length, table ID, and entry index, so
  reads turn into deterministic `ReadAt` calls instead of scanning variable-length payloads.
- **Trie-indexed pair table.** The `pairs/` directory holds fixed-size nodes that behave like a
  prefix tree. Nodes index a single byte per hop by default to keep tables compact, while the optional
  2-byte stride (set via `pair_index_bytes`) trades extra disk usage for shallower lookups. `PAIR_SCAN`
  and `PAIR_REDUCE` walk that trie, making namespace sweeps and reducer workloads practical even when
  the keyspace spans billions of n-gram contexts. Unique suffixes automatically collapse into jump
  nodes so single-key branches no longer allocate full tables.
- **Payload caching.** `database.go` keeps a bounded cache (defaults: 16k entries ≈64 MB) keyed by
  `<value_size, table_id, entry_id>` so hot payloads never hit disk. Tune it with
  `CHEETAH_PAYLOAD_CACHE_ENTRIES`, `CHEETAH_PAYLOAD_CACHE_MB`, or
  `CHEETAH_PAYLOAD_CACHE_BYTES`, or disable caching entirely by setting any of them to `0`.
- **Resource-aware reducers.** The server detects available CPU cores at startup, samples live
  CPU/I/O pressure, and scales reducer worker pools accordingly so concurrent connections avoid
  exhausting compute or disk bandwidth.
- **Multi-tenant databases.** `engine.go` multiplexes logical databases under `cheetah_data/<name>`
  and exposes them over both CLI and TCP, making it easy to isolate experiments or pilot rollouts.
- **Reducer streaming.** Reducers stream inline payloads through a bounded worker pool, overlap disk
  reads with encoding work, and emit cursor tokens so callers can page through arbitrarily large
  namespaces without restarting the command.

## Architecture at a Glance

- `main.go` boots the TCP listener (`0.0.0.0:4455` by default) plus the local CLI and routes commands
  to database handles returned by `engine.GetDatabase(name)`.
- `engine.go` lazily instantiates `Database` structs backed by `cheetah_data/<dbname>` and ensures all
  tables (main keys, values, recycling queues, pair trie) flush cleanly at shutdown.
- `database.go` orchestrates CRUD operations:
  - `MainKeysTable` stores compact metadata describing payload size + pointer offsets.
  - `ValuesTable` files hold fixed-width blobs grouped by byte length and table ID so offsets remain
    arithmetic instead of scan-based.
  - `RecycleTable` files keep tombstoned slots per value size so inserts can reuse space without
    compaction pauses.
  - `PairTable` nodes store child pointers and terminal flags independently, unlocking
    prefix-sharing namespaces such as `ctx:`, `ctxv:`, `prob:`, or `meta:`.
- `server.go` accepts newline-delimited commands (`INSERT`, `READ`, `PAIR_SCAN`, `PAIR_REDUCE`, …)
  over TCP so external adapters can talk to the engine without embedding Go code.

## Heavy Statistical Workloads

cheetah-db was designed to stage dense statistical datasets such as n-gram probabilities,
continuation metadata, and concept caches:

- Use dedicated namespaces (e.g., `ctx:`, `prob:<order>`, `cont:`, `meta:`) to keep context metadata,
  quantized probability tables, and continuation payloads separated yet streamable.
- Mirror Top-K caches, follow-up penalties, or other heavy slices directly into the trie so cache
  lookups never re-open SQLite or object stores.
- When benchmarking reducers, export `CHEETAHDB_BENCH=1` and run
  `go test -run TestCheetahDBBenchmark -count=1 -v` for reproducible throughput snapshots.
- Keep `cheetah-db/AI_REFERENCE.md` handy for cache-sizing tables, tmux/screen launch recipes, and
  namespace troubleshooting tips when you run sustained ingest/eval loops.

## Memory & SSD Guidelines

- Start the server with a payload-cache budget sized for your workload. 64 MB works for small corpora
  while 128–256 MB keeps multi-GB statistical slices in RAM. Increase the byte budget before raising
  the entry count so the cache retains whole payloads.
- Inserts seed the cache and deletes invalidate their slots, so chaining ingest → reduction → decoder
  stages benefits from a single long-lived process. Avoid restarting the server between stages unless
  you want to profile cold starts.
- To prime the cache after restarts, issue low-limit `PAIR_SCAN ctx:` passes (following cursors) or
  scripted `READ` loops over the namespaces you are about to benchmark. This shifts the I/O churn
  into RAM, keeping SSD wear predictable.
- When profiling disk I/O, set `CHEETAH_PAYLOAD_CACHE_ENTRIES=0` (or the MB/byte variants) to disable
  caching entirely, then re-enable it immediately afterward for day-to-day runs.

## Building & Running

```bash
cd cheetah-db
bash build.sh              # produces ./cheetah-server
./cheetah-server           # interactive CLI + TCP listener
# or launch headless
CHEETAH_HEADLESS=1 ./cheetah-server
```

Environment variables:

- `CHEETAH_DATA_DIR` — root directory for database folders (defaults to `cheetah_data`).
- `CHEETAH_PAYLOAD_CACHE_ENTRIES` / `_MB` / `_BYTES` — cache tuning knobs.
- `CHEETAH_LOG_LEVEL` — set to `3`/`debug` for level 3 traces (command ingress, reducer/trie steps).

Prefer declarative settings? Copy `config.example.ini` to `config.ini` (or point
`CHEETAH_CONFIG_PATH` at a custom file) and edit:

- `[server]` covers `listen_addr`, `data_dir`, and `default_database`.
- `[database]` sets `pair_index_bytes` (1 or 2) plus payload-cache sizing.
- `[tuning]` exposes `max_pair_tables` so you can pin the open-file budget.

Per-database overrides can be forced at runtime via CLI/TCP commands—append
`key=value` tokens such as `pair_bytes=1` or `payload_cache_entries=0` to the
`DATABASE`/`RESET_DB` commands to rebuild a trie with different settings.

The binary prints `[cheetah_data/default]>` when ready. Use `DATABASE <name>` (CLI) to switch between
logical databases or send the same command over TCP.

## Command Reference

```
INSERT:<size> <payload>         # create payload, returns abs key
READ <abs_key>                  # fetch payload by key
EDIT:<size> <abs_key> <payload> # overwrite payload in-place
PAIR_SET <hex_prefix> <payload> # map trie prefix to payload key
PAIR_SCAN <prefix> [limit]      # stream ordered namespace slices (cursors supported)
PAIR_REDUCE <mode> <prefix>     # stream reducer payloads (counts/probabilities/etc.)
PAIR_SUMMARY <prefix> [depth] [branch_limit]
                                # aggregate namespace statistics (payload totals, branch fan-out)
RESET_DB [name]                 # delete/recreate the current (or named) database on disk
DELETE <abs_key>                # tombstone entry
RECYCLE <value_size>            # report recycle stats per table
SYSTEM_STATS                    # snapshot of CPU/IO usage + concurrency hints
LOG_FLUSH [limit]               # dump + clear the in-memory log ring buffer (optionally capped)
```

- Prefix strings (`ctx:`, `ctxv:`, `prob:2`, etc.) are treated as raw bytes; encode binary prefixes
  as `x<HEX>`.
- `PAIR_SCAN` replies include `items=<hex_prefix>:<abs_key>` pairs plus `next_cursor=<token>` when
  additional pages remain. Reissue the command with `CURSOR <token>` (TCP) or `PAIR_SCAN <prefix> <limit> <token>` (CLI) to continue.
- `PAIR_REDUCE` includes inline base64 payloads so reducers can hydrate counters/probabilities
  without extra `READ` calls. Each response also includes `next_cursor` when more items exist.
- `PAIR_SUMMARY` walks the trie beneath a namespace prefix, counts terminal entries, sums payload
  sizes (without hydrating the bytes), tracks min/max payloads and keys, and emits branch-level
  fan-out counts up to the requested depth. Use the optional `branch_limit` to cap the number of
  branch digests returned (default: 32). This is the entry point for data-centric statistics—e.g.,
  estimating hot prefixes before launching GPU reducers or precomputing rolling hashes described in
  the tree-indexing section below.
- `DATABASE` and `RESET_DB` accept optional overrides (`DATABASE ctx pair_bytes=1 payload_cache_entries=0`)
  to rebuild a specific database with narrower trie nodes or a different payload-cache budget without
  editing `config.ini`.
- `SYSTEM_STATS` emits `logical_cores`, GOMAXPROCS, goroutine counts, CPU percentages, and
  per-second disk I/O deltas so you can script adaptive ingest/decoder pipelines without shelling
  out to `top`/`iostat`. The payload cache now reports `payload_cache_*` fields (entries/bytes,
  hits/misses/evictions, hit % plus an advisory bypass threshold) in the same response so adapters
  can auto-tune `CHEETAH_PAYLOAD_CACHE_*` or skip caching multi-megabyte payloads that would churn.
- `LOG_FLUSH` returns the most recent log lines captured by the server (default ring buffer depth:
  256 entries) and clears the buffer. Pass a numeric limit to trim the output without truncating the
  stored log metadata.

## Command Walkthroughs

The CLI and TCP listener both speak newline-delimited commands, so anything you can type by hand can
be scripted from tests or adapters. A quick ingestion session looks like:

```text
[cheetah_data/default]> INSERT:5 HELLO
SUCCESS,key=1
[cheetah_data/default]> READ 1
SUCCESS,size=5,value=HELLO
[cheetah_data/default]> EDIT 1 HELLO
SUCCESS,key=1_updated
[cheetah_data/default]> DELETE 1
SUCCESS,key=1_deleted
```

- `INSERT:<declared_size>` validates that the payload length matches the colon-suffix (or infers it
  when omitted), writes the bytes into the size-partitioned value table, and returns the absolute
  key (`mainKeys` offset). `READ`, `EDIT`, and `DELETE` operate on that numeric key and either reuse
  cache hits or fall back to deterministic `ReadAt` offsets inside the same value table file.
- Pair namespaces bind human-readable prefixes to absolute keys. For ASCII prefixes you can type
  them directly; binary prefixes use `x<hex>` (the same helper that `PAIR_SCAN` and `PAIR_REDUCE`
  emit). Example:

  ```text
  [cheetah_data/default]> INSERT:18 ctx:BERLIN|CONTEXT
  SUCCESS,key=42
  [cheetah_data/default]> PAIR_SET ctx:BERLIN 42
  SUCCESS,pair_set
  [cheetah_data/default]> PAIR_GET ctx:BERLIN
  SUCCESS,pair=ctx:BERLIN,key=42
  ```

- `PAIR_SCAN` walks the trie in lexical order. Limits and cursors keep the scan resumable:

  ```text
  [cheetah_data/default]> PAIR_SCAN ctx: 2
  SUCCESS,count=2,next_cursor=x000104,items=6378743a4245524c494e:42;6378743a4e41584f53:77
  [cheetah_data/default]> PAIR_SCAN ctx: 2 x000104
  SUCCESS,count=2,items=6378743a4e45574f524c4c:81;6378743a5041524953:96
  ```

  Here each `items` entry is `<hex_prefix>:<abs_key>`. Passing `*` as the prefix or cursor makes the
  server start from the root or continue from “wherever you left off,” respectively.
- `PAIR_REDUCE` stays in the same namespace but executes a Go reducer before streaming rows back. A
  counts example (base64 payload contains packed counters so the client can decode without `READ`):

  ```text
  [cheetah_data/default]> PAIR_REDUCE counts ctx: 1
  SUCCESS,reducer=counts,count=1,next_cursor=x0000af,items=6378743a4245524c494e:42:AAEAAAABAAAD
  ```

  Reducers control the payload schema; if you extend `commands.go` with a new reducer you only need
  to document how to decode its base64 block.
- `PAIR_PURGE <prefix> [page_size]` wipes every pair entry beneath the prefix and deletes the backing
  payload keys inside Go. Use `PAIR_PURGE ctx:` (or `*` to nuke the entire trie) when you need a hot
  reset before an ingest run—each batch clears up to 4096 entries so the purge finishes in seconds
  instead of hours of TCP round-trips.
- `RESET_DB [name]` closes the target database, deletes `cheetah_data/<name>` on disk, and reopens it
  empty so hot-path clients can wipe everything (pairs, value tables, metadata) with a single command.
  Omitting the name resets whichever database is currently selected on the connection/CLI prompt.
- `SYSTEM_STATS` is a cheap heartbeat: call it between ingest/reduce loops to track CPU, memory, and
  fd counts without spawning `top`. Because `database.go` formats it in CSV-like key/value pairs, it
  can be parsed by shell scripts (`awk -F,`) or structured log scrapers.

- **Prediction tables & context matrices.** The database can now host multiple prediction tables
  (stored as fixed-byte `prediction_<name>.table` files alongside the trie; JSON only appears on the
  CLI for request/response payloads) and expose GPU-style probability merges:

  - `PREDICT_SET key=<prefix> value=<bytes> prob=<0-1> [weights=<base64 json>] [table=name]` stores a
    candidate value for the given prefix. Context weights use the JSON schema documented in
    `AI_REFERENCE.md` (encode the JSON blob, then pass it as base64).
  - `PREDICT_QUERY key=<prefix> [keys=a,b,c] [ctx=<base64 json>] [windows=<base64 json>]
    [key_windows=<base64 json>] [merge=avg|sum|max] [table=name]` evaluates one or many prefixes and
    merges their probability windows. `keys=` lets you query several prefixes at once, while
    `key_windows=` accepts a base64 array of `{ "key": "<hex>", "windows": [[...], ...] }` objects for
    per-prefix window overrides. Responses include the backend name (`cpu` or the simulated
    `webgpu-simulated` merger).
  - `PREDICT_TRAIN key=<prefix> target=<bytes> [ctx=<base64 json>] [lr=0.01] [table=name]` adjusts
    stored weights via the forward/backward loop, and `PREDICT_CTX key=<prefix> ctx=<base64 json>
    [mode=bias|scale] [strength=1] [table=name]` applies an immediate context bias without retraining.
  - `PREDICT_BACKEND [mode=cpu|gpu] [table=name]` toggles the probability merger per table, and
    `PREDICT_BENCH samples=<n> window=<len> [table=name]` compares CPU vs accelerated merges on the
    current host.

  All prediction commands accept plaintext prefixes or the `x<hex>` form. Context matrices and window
  specs must be base64-encoded JSON so CLI input stays newline-safe.

- **Cluster coordination.** Multi-node deployments can now tell cheetah where each fork lives. Set
  `CHEETAH_NODE_ID=<id>` on every server, then use:
  - `CLUSTER_UPDATE replication=<n> nodeA=host:port/weight ...` (or `json=<base64>`) to register the
    topology,
  - `CLUSTER_STATUS` to view assignments,
  - `FORK_ASSIGN <prefix|*>` to see which nodes own a shard, and
  - `CLUSTER_MOVE prefix=<bytes>|fork=<id> node=<nodeID>` to override placement (broadcast to peers via
    `CLUSTER_GOSSIP json=<payload>`). Overrides persist next to `cluster_topology.json`, so restarts
    keep the new mapping.

## Streaming Helpers

- `PAIR_SCAN <prefix> [limit]` favors namespace exhaustiveness: `PAIR_SCAN ctx: 100` walks the first
  100 contexts alphabetically, while `PAIR_SCAN * 0` streams the entire trie.
- `HotPathAdapter`-style integrations typically alternate `PAIR_SCAN` with `READ` to hydrate payloads;
  keep a cache of `<value_size, table_id, entry_id>` tuples nearby when building custom adapters.

## Reducer Hooks

- `PAIR_REDUCE counts ctx:` aggregates follower counts directly inside Go and emits the packed
  payloads inline, allowing MKNS-style reducers or other statistical aggregators to run without SQL.
- Custom reducers can be registered in Go (see `commands.go`). Each reducer receives the pair-trie
  iterator and can emit any payload format; clients decode the base64 payload per reducer contract.

## Operational Notes

- Prefer `screen` or `tmux` for long-lived sessions. Launch commands with explicit timeouts (≤30 min
  unless otherwise justified) so stalled reducers cannot block future sessions.
- `CHEETAH_HEADLESS=1` disables the interactive CLI while keeping the TCP listener up. When running in
  WSL or remote shells, pair it with `screen -dmS cheetahdb ...` and monitor `screen -ls` /
  `screen -wipe` before rebuilding.
- Rotate logs under `var/eval_logs/`—benchmark helpers emit files such as
  `cheetah_db_benchmark_<timestamp>.log` so you can diff throughput across cache sizes or reducer
  tweaks.

For deep operational checklists (tmux helpers, namespace triage, cache sizing matrices), see
`AI_REFERENCE.md` in this directory.

## Tree Indexing & Algorithmic Logic

cheetah-db’s performance hinges on a deterministic, trie-backed index that treats every namespace or
key prefix as a path through fixed-size `PairTable` nodes. Each node behaves like the `CharTreeNode`
structure shown in `src/helpers/char_tree_similarity.py`: it has fixed-span children (one raw byte
per hop by default, optionally two when configured), terminal
flags, and in-memory counts that highlight “hot” substrings. While the helper library leans on that
structure to compare strings (build a char tree, keep significant substrings, compute overlaps), the
database applies the same idea to on-disk tables:

- Every namespace (e.g., `ctx:BERLIN`) is stored as raw bytes. Walking those bytes selects a slot
  inside the root pair table and either follows a child table ID or marks the node as terminal with
  the absolute payload key. This mirrors the helper’s recursion where `CharTree` expands one
  character at a time and records substring counts.
- `PAIR_SCAN` snapshots each node and streams children in lexical order, so namespace enumeration only
  touches the branches that exist. Nodes allocate exactly `∑_{i=1..pair_index_bytes} 256^i` slots
  (256 entries when indexing a single byte, 256 + 65,536 entries when indexing two bytes), so the
  engine can seek directly via `branchIndex * PairEntrySize` with no heap allocations—the storage
  equivalent of how `CharTree.from_text` iterates substrings without rebuilding prefixes.
- Jump nodes collapse unique suffixes into a compact segment so single-key branches no longer
  allocate entire tables. `PAIR_SET` writes the remainder of a key into a jump node whenever a branch
  has no siblings, and the node is split automatically if a later key shares part of that suffix. On
  deletions the engine rechecks whether a child table now has only one branch and promotes it back
  into a jump when possible, keeping disk usage proportional to the number of active prefixes.
- `PAIR_REDUCE` executes reducers while it walks the trie. As soon as a branch is materialized, the
  reducer can hydrate payloads (`readValuePayload`) and emit inline aggregates. This is conceptually
  the same as weighting recurring substrings in `substring_multiset_similarity`: we take advantage of
  prefix locality to amortize disk reads and to reuse cached payloads when sibling prefixes live in
  the same tables.
- Future performance work leverages this “tree indexing” foundation: we can precompute namespace
  statistics (counts, rolling hashes, Top-K summaries) and branch-local caches the same way
  `char_tree_similarity.py` keeps only significant substrings. Because the layout guarantees stable
  offsets, prefetchers or GPU-backed reducers can schedule precise `ReadAt` calls without scanning.
- `PAIR_SUMMARY` is the first tooling pass for that roadmap: it reuses the same tree walk to report
  per-namespace totals and per-branch counts without shipping every payload back to Python. Passing
  `PAIR_SUMMARY ctx: 2 64`, for example, shows the hottest two-depth prefixes (capped at 64
  branches) while also returning min/max payload sizes—perfect for prioritizing which contexts to
  mirror into GPU reducers or char-tree similarity scans.

Treating namespace keys as traversable trees keeps INSERT/READ latency tied to fixed math instead of
variable-length scans. It also gives future tooling (fuzzy namespace matching, char-tree-style
similarity lookups, or trie-level compression) a solid footing because the invariants mirror the
pure-Python helper: branch per configured byte-span, attach metadata where a path terminates, and stream the
structure without rebuilding it in memory.

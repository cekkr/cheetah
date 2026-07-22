# cheetah-db

High-throughput key/value store with a trie-backed pair table purpose-built for statistical data
pipelines. cheetah-db ingests byte-encoded contexts, n-gram payloads, and arbitrary binary blobs,
keeps them partitioned by value size, and exposes TCP + CLI commands that stream results with
bounded memory use. The engine targets workloads where millions of probabilities, counters, or
other dense analytical slices must be served with predictable latency.

## Highlights

- **Byte-faithful layout.** Every entry is cataloged by byte length, table ID, and entry index, so
  reads turn into deterministic `ReadAt` calls instead of scanning variable-length payloads.
- **Trie-indexed pair table.** The `pairs/` directory holds nodes that behave like a prefix tree.
  Nodes index a single byte per hop by default, while the optional 2-byte stride (set via
  `pair_index_bytes`) buys shallower lookups. `PAIR_SCAN` and `PAIR_REDUCE` walk that trie, making
  namespace sweeps and reducer workloads practical even when the keyspace spans billions of n-gram
  contexts. Unique suffixes automatically collapse into jump nodes so single-key branches no longer
  allocate full tables, and **adaptive node indexing** keeps a sparse node as a compact
  binary-searched list instead of reserving its whole branch array — so the wide stride no longer
  costs ~707 KB per node.
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
- See [`AGENTS.md`](AGENTS.md) for cache-sizing guidance, launch recipes, and namespace
  troubleshooting notes when you run sustained ingest/eval loops.

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
bash build.sh              # produces ./cheetah-server (or: go build -o cheetah-server .)
./cheetah-server           # interactive CLI + TCP listener
# or launch headless
CHEETAH_HEADLESS=1 ./cheetah-server
```

Environment variables:

- `CHEETAH_DATA_DIR` — root directory for database folders (defaults to `cheetah_data`).
- `CHEETAH_PAYLOAD_CACHE_ENTRIES` / `_MB` / `_BYTES` — cache tuning knobs.
- `CHEETAH_LOG_LEVEL` — set to `3`/`debug` for level 3 traces (command ingress, reducer/trie steps).

- `CHEETAH_PREDICT_DEEPEN` - set to `0` to disable context-matrix deepening in prediction tables (derived layers now scale with context diversity).

Prefer declarative settings? Copy `config.example.ini` to `config.ini` (or point
`CHEETAH_CONFIG_PATH` at a custom file) and edit:

- `[server]` covers `listen_addr`, `data_dir`, and `default_database`.
- `[database]` sets `pair_index_bytes` (1 or 2) and `adaptive_pair_index` (default `true`), plus
  payload-cache sizing.
- `[tuning]` exposes `max_pair_tables` so you can pin the open-file budget,
  `pair_list_max_bytes` (default 4096) — the sorted-list size at which an adaptive node expands into
  a direct-mapped array, which also decides which nodes use a list at all — and the optional
  `pair_list_max_fill_percent` (default 0, off).

Per-database overrides can be forced at runtime via CLI/TCP commands—append
`key=value` tokens such as `pair_bytes=1`, `adaptive_pair_index=0`, or `payload_cache_entries=0` to
the `DATABASE`/`RESET_DB` commands to rebuild a trie with different settings.

`pair_index_bytes` and `adaptive_pair_index` are recorded in `pairs/format.dat` when a database is
created and that marker wins on every later open, so editing `config.ini` never reinterprets existing
on-disk data. Use `RESET_DB <name> [pair_bytes=…] [adaptive_pair_index=…]` to rebuild with new
settings. A `pairs/` directory written before this format existed is refused at open with
`incompatible_pair_format_rebuild_required`; rebuild it and re-ingest.

The binary prints `[cheetah_data/default]>` when ready. Use `DATABASE <name>` (CLI) to switch between
logical databases or send the same command over TCP.

## Command Reference

```
INSERT:<size> <payload>         # create payload, returns abs key
READ <abs_key>                  # fetch payload by key
EDIT:<size> <abs_key> <payload> # overwrite payload in-place
PAIR_SET <hex_prefix> <payload> # map trie prefix to payload key
PAIR_SET_HIDDEN <hex_prefix> <payload>
                                # map a hidden trie prefix to payload key
PAIR_SCAN <prefix> [limit] [cursor] [include_hidden=1]
                                # stream ordered namespace slices (cursors supported)
PAIR_REDUCE <mode> <prefix> [limit] [cursor] [include_hidden=1]
                                # stream reducer payloads (counts/probabilities/degree/triangle/pagerank_seed/etc.)
PAIR_REDUCE_ASYNC <mode> <prefix> [limit] [cursor]
                                # enqueue reducer job and return a job identifier
PAIR_REDUCE_STATUS <job_id>     # report reducer job progress/state
PAIR_REDUCE_FETCH <job_id>      # fetch reducer results once completed (PENDING while running)
PAIR_SUMMARY <prefix> [depth] [branch_limit] [include_hidden=1]
                                # aggregate namespace statistics (payload totals, branch fan-out)
GRAPH_NODE_SET id=<node> [labels=a,b] [props=<base64 json>]
                                # upsert a graph node record
GRAPH_NODE_GET id=<node>
GRAPH_NODE_DEL id=<node> [cascade=1]
GRAPH_EDGE_SET from=<node> to=<node> [type=<edge>] [weight=<float>] [directed=0|1]
                                # upsert a typed edge + adjacency indexes
GRAPH_EDGE_SET_BATCH items=<base64 json> [continue_on_error=0|1] [type=<edge>] [directed=0|1]
                                # bulk graph edge upsert in one command
GRAPH_EDGE_GET from=<node> to=<node> [type=<edge>] [directed=0|1]
GRAPH_EDGE_DEL from=<node> to=<node> [type=<edge>] [directed=0|1]
GRAPH_NEIGHBORS id=<node> [direction=out|in|both] [type=<edge>] [limit=<n>] [cursor=<token>]
                                # stream adjacency pages backed by trie prefixes
GRAPH_DEGREE id=<node> [direction=out|in|both] [type=<edge|*>] [weighted=0|1]
                                # fast degree/weighted-degree stats
GRAPH_NEIGHBOR_TYPES id=<node> [direction=out|in|both] [limit=<n>] [cursor=<token>] [weighted=0|1]
                                # compact relation histogram for fast graph feature extraction
GRAPH_QUERY MATCH (...) ...     # graph pattern query (see syntax below)
RESET_DB [name]                 # delete/recreate the current (or named) database on disk
DELETE <abs_key>                # tombstone entry
FILE_CHECKPOINT [IDLE=<dur>] [DROP_CACHE] [CLOSE_HANDLES]
                                # force-flush dirty sectors / release idle handles mid-run
SYSTEM_STATS                    # snapshot of CPU/IO usage + concurrency hints
LOG_FLUSH [limit]               # dump + clear the in-memory log ring buffer (optionally capped)
```

- Prefix strings (`ctx:`, `ctxv:`, `prob:2`, etc.) are treated as raw bytes; encode binary prefixes
  as `x<HEX>`.
- `PAIR_SCAN` replies include `items=<hex_prefix>:<abs_key>` pairs plus `next_cursor=<token>` when
  additional pages remain. Reissue `PAIR_SCAN <prefix> <limit> <token>` (over both CLI and TCP) to
  continue from that cursor. Add `include_hidden=1` to return hidden terminals.
- `PAIR_REDUCE` includes inline base64 payloads so reducers can hydrate counters/probabilities
  without extra `READ` calls. Each response also includes `next_cursor` when more items exist.
- `PAIR_REDUCE_ASYNC` is ideal for long-running reducers: it queues the request, returns a `job`
  token immediately, and lets clients poll `PAIR_REDUCE_STATUS`/`PAIR_REDUCE_FETCH` to monitor
  progress or stream the final payloads once the job completes.
- `PAIR_REDUCE_FETCH` replies with `PENDING,...,progress=<percent>,completed=<n>,total=<n>` while a
  job is still running so adapters can emit keep-alive logs; once the reducer finishes the response
  mirrors the synchronous `PAIR_REDUCE` payload (including `next_cursor` when more pages remain).
- `PAIR_SUMMARY` walks the trie beneath a namespace prefix, counts terminal entries, sums payload
  sizes (without hydrating the bytes), tracks min/max payloads and keys, and emits branch-level
  fan-out counts up to the requested depth. Use the optional `branch_limit` to cap the number of
  branch digests returned (default: 32) and `include_hidden=1` to count hidden terminals. This is the entry point for data-centric statistics—e.g.,
  estimating hot prefixes before launching GPU reducers or precomputing rolling hashes described in
  the tree-indexing section below.
- Graph storage is indexed in four isolated namespaces (`node`, `edge`, `adj/out`, `adj/in`) so
  node/edge writes do not force full graph scans. `GRAPH_NEIGHBORS` and `GRAPH_QUERY` always execute
  as prefix scans over adjacency indexes.
- `GRAPH_QUERY` supports bounded multi-hop traversal with pruning/cost controls:

  ```text
  MATCH (<left-node>)-[:<edge_type>]->(<right-node>)
  [WHERE <predicate> [AND <predicate> ...]]
  [HOPS <max>|<min>..<max>]
  [BRANCH_LIMIT <n>]
  [COST_LIMIT <float>]
  [RETURN edges|nodes|paths|count]
  [LIMIT <n>]
  [CURSOR <token>]
  ```

  Node expressions support `*` or `id='value'` (optionally `label='value'`). Predicates support
  `from.id`, `to.id`, `from.label`, `to.label`, `edge.type`, `edge.weight`, and
  `edge.props.<prop_key>`. The left node must be anchored by ID to keep execution index-backed.
  `edge.props.<prop_key> = <literal>` predicates use a secondary index namespace
  (`graph/idx/<prop>/<value>/...`) for fast filtering.
- Reducer modes `degree`, `triangle`, and `pagerank_seed` operate directly on graph adjacency
  namespaces (`adj/out` / `adj/in`) so graph statistics can stream without full edge hydration.
- `DATABASE` and `RESET_DB` accept optional overrides (`DATABASE ctx pair_bytes=1 payload_cache_entries=0`,
  plus `adaptive_pair_index=`, `pair_list_max_bytes=`, and `pair_list_max_fill_percent=`) to rebuild a specific database with different
  trie-node geometry or payload-cache budget without editing `config.ini`. Trie-format overrides only
  take effect when the database is created, so pair them with `RESET_DB`.
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
    candidate value for the given prefix. Context weights use the `ContextWeight` JSON schema defined
    in [`prediction_table.go`](prediction_table.go) (encode the JSON blob, then pass it as base64).
  - `PREDICT_QUERY key=<prefix> [keys=a,b,c] [ctx=<base64 json>] [windows=<base64 json>]
    [key_windows=<base64 json>] [merge=avg|sum|max] [table=name]` evaluates one or many prefixes and
    merges their probability windows. `ctx` may be a base64-encoded JSON array (`[[...], ...]`) or an
    object like `{"rows":[[...]],"weights":[...]}` where weights scale each row. `keys=` lets you
    query several prefixes at once, while `key_windows=` accepts a base64 array of
    `{ "key": "<hex>", "windows": [[...], ...] }` objects for per-prefix window overrides. Responses
    include the backend name (`cpu` or the simulated `webgpu-simulated` merger).
  - `PREDICT_TRAIN key=<prefix> target=<bytes> [ctx=<base64 json>] [lr=0.01] [table=name]
    [negatives=<hex,...>]` adjusts stored weights via the forward/backward loop (optionally
    down-weighting bad predictions listed in `negatives=`). The table now persists normalized window
    hints from every training/adversarial context and blends them into queries automatically when no
    `windows=` payload is supplied. `PREDICT_CTX key=<prefix> ctx=<base64 json> [mode=bias|scale]
    [strength=1] [table=name]` applies an immediate context bias without retraining.
  - `PREDICT_INHERIT key=<prefix> target=<bytes> sources=<hex,...> [merge=avg|sum|max] [table=name]`
    merges existing prediction values into a new target (for example, to seed composite/merged
    tokens with inherited context weights).
  - `PREDICT_INHERIT_BATCH items=<base64 json> [key=<prefix>] [merge=avg|sum|max] [table=name]`
    processes multiple inherit requests in one call. The JSON payload is an array of
    `{ "key": "<hex>", "target": "<hex>", "sources": ["<hex>", ...], "merge": "avg" }` objects.
  - `PREDICT_INHERIT_ASYNC items=<base64 json> [key=<prefix>] [merge=avg|sum|max] [table=name]`
    queues a batch job and returns a `job` token for later polling.
  - `PREDICT_INHERIT_STATUS <job_id>` reports job progress (merged/skipped/failed counts).
  - `PREDICT_INHERIT_FETCH <job_id>` returns batch results once the job completes (or `PENDING` while running).
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

## Graph Command Language

cheetah-db stores a labeled **property graph** next to the KV/pair data and drives it with the same
newline-delimited protocol. The surface splits cleanly into **setting** information (nodes, edges,
properties) and **calling** it back (point reads, adjacency, degree, and pattern queries). Graph
records are just opaque byte keys to the trie: they live under four reserved control-byte namespaces —
`\x01gn:` (nodes), `\x02ge:` (edges), `\x03go:` (out-adjacency), `\x04gi:` (in-adjacency) — plus a
`graph/idx/` property index, so node/edge writes never trigger a full graph scan and adjacency reads
are always prefix scans.

### Data model

- **Node** — `{ id, labels[], props{}, created_at, updated_at }`. `id` is any non-empty string; the
  server only trims it (it does **not** sanitize or add suffixes — any such convention lives in the
  client). `labels` is stored as a deduplicated, sorted set; `props` is a free-form JSON object.
- **Edge** — `{ id, from, to, type, directed, weight, props{}, created_at, updated_at }`. An edge is
  uniquely identified by the tuple `(from, to, type, directed)`; setting the same tuple again
  **upserts** it. `weight` defaults to `1.0` and `directed` defaults to `1` (true).
- **Properties** cross the wire as JSON — inline for simple values (`props={"since":2020}`) or
  base64-encoded JSON (`props=<base64>`) when the blob would contain spaces/newlines. Values may be
  strings, numbers, or booleans; `edge.props.*` values are additionally mirrored into a secondary
  index for fast `WHERE` filtering.
- **Record payloads** — every command that returns records answers `...,payload=<base64>`, where the
  base64 decodes to the JSON record (or a JSON array of records). This keeps each response on one line
  for line-oriented clients.

### Setting information

| Command | Purpose |
| --- | --- |
| `GRAPH_NODE_SET id=<id> [labels=a,b] [props=<json\|base64>]` | Upsert a node (preserves `created_at`; keeps existing labels/props when omitted). |
| `GRAPH_EDGE_SET from=<id> to=<id> [type=<t>] [weight=<f>] [directed=0\|1] [props=<json\|base64>] [autocreate=0\|1]` | Upsert one edge plus its adjacency/index entries. Missing endpoint nodes are auto-created unless `autocreate=0`. |
| `GRAPH_EDGE_SET_BATCH items=<base64 json[]> [continue_on_error=0\|1] [type=…] [directed=…] [weight=…] [props=…]` | Bulk upsert in one round-trip; top-level `type/directed/weight/props` act as per-item defaults. |
| `GRAPH_NODE_DEL id=<id> [cascade=1]` | Delete a node; `cascade=1` also removes its incident edges. |
| `GRAPH_EDGE_DEL from=<id> to=<id> [type=<t>] [directed=0\|1]` | Delete the edge addressed by the tuple. |

```text
[cheetah_data/graphlang]> GRAPH_NODE_SET id=alice labels=person,user props={"city":"berlin","age":30}
SUCCESS,node_set,id=alice
[cheetah_data/graphlang]> GRAPH_EDGE_SET from=alice to=bob type=follows weight=0.9 props={"since":2020} directed=1
SUCCESS,edge_set,id=MXxhbGljZXxmb2xsb3dzfGJvYg
[cheetah_data/graphlang]> GRAPH_EDGE_SET_BATCH items=<base64 of [{"from":"alice","to":"carol","type":"follows","weight":0.4}, …]> continue_on_error=1
SUCCESS,requested=2,applied=2,created=2,updated=0,failed=0
```

`GRAPH_EDGE_SET` returns the opaque edge `id`; the batch form reports
`requested/applied/created/updated/failed`, adding a base64 `payload=` array of `{index,error}`
objects when some rows fail under `continue_on_error=1`. Edge upserts also stub out any `from`/`to`
node that does not exist yet (disable with `autocreate=0`).

### Calling information

| Command | Returns |
| --- | --- |
| `GRAPH_NODE_GET id=<id>` | `payload=` the node record (or `ERROR,node_not_found`). |
| `GRAPH_EDGE_GET from=<id> to=<id> [type=<t>] [directed=0\|1]` | `payload=` the edge record (or `ERROR,edge_not_found`). |
| `GRAPH_NEIGHBORS id=<id> [direction=out\|in\|both] [type=<t\|*>] [limit=<n>] [cursor=<tok>]` | `count`, `next_cursor`, and `payload=` an array of edge records. |
| `GRAPH_DEGREE id=<id> [direction=out\|in\|both] [type=<t\|*>] [weighted=0\|1]` | `degree` (plus `weighted_degree` when `weighted=1`). |
| `GRAPH_NEIGHBOR_TYPES id=<id> [direction=out\|in\|both] [limit=<n>] [cursor=<tok>] [weighted=0\|1]` | `payload=` a compact relation histogram `[{type,count,weighted}]`. |

```text
[cheetah_data/graphlang]> GRAPH_NODE_GET id=alice
SUCCESS,id=alice,payload=<base64>
# payload decodes to: {"id":"alice","labels":["person","user"],"props":{"age":30,"city":"berlin"},"created_at":…,"updated_at":…}

[cheetah_data/graphlang]> GRAPH_NEIGHBORS id=alice direction=out type=* limit=8
SUCCESS,count=2,next_cursor=*,payload=<base64>
# payload decodes to: [{"from":"alice","to":"carol","type":"follows","weight":0.4,…}, {"from":"alice","to":"bob","type":"follows","weight":0.9,"props":{"since":2020},…}]

[cheetah_data/graphlang]> GRAPH_DEGREE id=alice direction=out type=* weighted=1
SUCCESS,id=alice,direction=out,type=*,degree=2,weighted_degree=1.300000

[cheetah_data/graphlang]> GRAPH_NEIGHBOR_TYPES id=alice direction=out limit=16 weighted=1
SUCCESS,count=1,next_cursor=*,payload=<base64>
# payload decodes to: [{"type":"follows","count":2,"weighted":1.3}]
```

Defaults: `direction=out`, `type=*` (all types), `limit=128` (max 2048). `next_cursor=*` means the
scan is exhausted (and, as an input cursor, `*` means "start from the beginning"); pass a returned
token back as `cursor=<tok>` to page. `direction=both` merges both sides and does not accept a cursor.
`GRAPH_NEIGHBOR_TYPES` is the fast path for feature extraction — it aggregates a per-relation histogram
without hydrating edge payloads.

### Pattern queries — `GRAPH_QUERY`

```text
GRAPH_QUERY MATCH (<left>)-[:<type|*>]->(<right>)     # follow out-edges of <left>
GRAPH_QUERY MATCH (<left>)<-[:<type|*>]-(<right>)     # follow in-edges of <left>
  [WHERE <predicate> [AND <predicate> ...]]
  [HOPS <max> | <min>..<max>]      # default 1..1
  [BRANCH_LIMIT <n>]               # fan-out cap per hop (default 128)
  [COST_LIMIT <float>]             # early-stop on accumulated traversal cost (cost per hop = 1/weight)
  [RETURN edges | nodes | paths | count]   # default edges
  [LIMIT <n>]                      # default 128
  [CURSOR <token>]
```

- **Node patterns** are `*` (wildcard) or `id='value'`, optionally narrowed with `label='value'`
  (e.g. `(id='alice',label='person')`). The **left node must be ID-anchored** — wildcard-left queries
  are rejected so execution stays index-backed.
- **Both arrow directions are supported and both anchor on the left node.** `-[:t]->` walks the
  `adj/out` index, `<-[:t]-` walks `adj/in`; in either case the left pattern constrains the anchor and
  the right pattern constrains the far endpoint. Predicates stay edge-oriented regardless of arrow:
  `from.id`/`to.id` always mean the edge's own `from`/`to` fields, so in a reverse query the anchor is
  `to.id`.
- **Predicates** read `from.id`, `to.id`, `from.label`, `to.label`, `edge.type`, `edge.weight`, and
  `edge.props.<key>` with operators `= != >= <= > <`. An `edge.props.<key> = <literal>` equality is
  served straight from the `graph/idx/` secondary index.
- **RETURN modes**: `edges` (full edge records), `nodes` (sorted unique node ids), `paths` (compact
  `{from,type,to,weight}` views), `count` (just `matches=<n>`, no payload). Since higher weight means
  lower cost, `COST_LIMIT` prunes paths that traverse low-weight edges first.

```text
[cheetah_data/graphlang]> GRAPH_QUERY MATCH (id='alice')-[:follows]->(*) WHERE edge.weight >= 0.5 RETURN edges LIMIT 8
SUCCESS,return=edges,matches=1,next_cursor=*,payload=<base64>
# payload decodes to: [{"from":"alice","to":"bob","type":"follows","weight":0.9,"props":{"since":2020},…}]

[cheetah_data/graphlang]> GRAPH_QUERY MATCH (id='alice')-[:*]->(*) RETURN nodes LIMIT 8
SUCCESS,return=nodes,matches=3,next_cursor=*,payload=<base64>
# payload decodes to: ["alice","bob","carol"]

[cheetah_data/graphlang]> GRAPH_QUERY MATCH (id='alice')-[:*]->(id='carol') HOPS 1..2 BRANCH_LIMIT 32 COST_LIMIT 5 RETURN paths LIMIT 8
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# payload decodes to: [{"from":"alice","type":"follows","to":"carol","weight":0.4},{"from":"bob","type":"likes","to":"carol","weight":0.7}]
```

Graph statistics can also stream through the reducer language: `PAIR_REDUCE degree|triangle|pagerank_seed <adj-hex-prefix>`
runs directly over the `adj/out`/`adj/in` namespaces without hydrating edges. For the authoritative
argument grammar of every command above, the source of truth is the `handleGraph*` dispatch in
[`graph.go`](graph.go) (routed from `ExecuteCommand` in [`database.go`](database.go)) — this section
documents that behavior, not a separate spec. A runnable, end-to-end example of the whole language
(ingest → adjacency → query → predict over TCP) lives in
[`demo/graph-nell/`](demo/graph-nell/README.md). For worked examples of turning natural-language
sentences into these commands (and back into answers), see
[Sentences → Graph → Answers](#sentences--graph--answers-llm-recipes).

## Sentences → Graph → Answers (LLM Recipes)

The graph language is meant to be driven by a model, in two directions:

- **Writing** — a *statement* becomes nodes and edges (`GRAPH_NODE_SET` / `GRAPH_EDGE_SET`).
- **Reading** — a *question or wish* becomes an **intent** plus a query plan
  (`GRAPH_NODE_GET` / `GRAPH_NEIGHBORS` / `GRAPH_NEIGHBOR_TYPES` / `GRAPH_QUERY`).

Every transcript below was captured from a fresh database; long `payload=` blobs are abbreviated and
shown decoded on the following comment line.

### The mapping contract

| Sentence part | Graph object | Rationale |
| --- | --- | --- |
| Entity — "my cat", "Acme" | node, id `type:slug` | ids are opaque bytes; a `type:` prefix keeps namespaces scannable |
| Kind / class — "is a cat", "a person" | `labels=animal,cat` | labels filter (`label='person'`), they never carry values |
| Attribute you will never query on its own — "named Luna", "female" | `props={...}` on the node | cheap to read back with the node, no extra hop |
| Attribute you *will* query or join — "siamese", "cute", "gluten-free" | its own node + a typed edge | reachable from both ends: "which of my pets are sweet?" starts at `trait:sweet` |
| Relation — "owns", "works at", "reports to" | edge `type=` (snake_case verb) | edge types are the traversal alphabet; keep them few and stable |
| Hedge / intensity — "may be", "very", "I think" | `weight=` (0–1 confidence) **+** `props={"modality":...}` | keeps the edge *type* stable so one query shape finds certain and uncertain facts |
| Disjunction — "either A or B, I forget" | one edge per alternative sharing `props.oneof`, weights summing to ~1 | see [uncertainty](#recording-uncertainty-alternatives-and-corrections); the engine has no `OR`, so alternatives are data |
| Time / provenance — "since 2019", "she told me" | edge `props` (`since`, `source`) | indexed for `WHERE edge.props.<k> = <v>` equality |
| Wish / question — "I would like…" | `intent:` node + `wants` / `about` edges | the question itself becomes a fact you can answer, revisit, and close |

Rule of thumb: **if a later question could start from it, it is a node.** Everything else is a prop.

### Writing a sentence

> *"My cat is a female siamese, very cute and sweet. But she may be sterile."*

```text
[cheetah_data/default]> GRAPH_NODE_SET id=person:owner labels=person props={"role":"speaker"}
SUCCESS,node_set,id=person:owner
[cheetah_data/default]> GRAPH_NODE_SET id=cat:luna labels=animal,cat props={"name":"Luna","sex":"female"}
SUCCESS,node_set,id=cat:luna
[cheetah_data/default]> GRAPH_NODE_SET id=breed:siamese labels=breed
SUCCESS,node_set,id=breed:siamese
[cheetah_data/default]> GRAPH_NODE_SET id=trait:cute labels=trait
SUCCESS,node_set,id=trait:cute
[cheetah_data/default]> GRAPH_NODE_SET id=trait:sweet labels=trait
SUCCESS,node_set,id=trait:sweet
[cheetah_data/default]> GRAPH_NODE_SET id=condition:sterile labels=condition,fertility
SUCCESS,node_set,id=condition:sterile
[cheetah_data/default]> GRAPH_EDGE_SET from=person:owner to=cat:luna type=owns weight=1.0
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8b3duc3xjYXQ6bHVuYQ
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed weight=1.0 props={"source":"owner_statement"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfYnJlZWR8YnJlZWQ6c2lhbWVzZQ
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=trait:cute type=has_trait weight=0.9 props={"intensity":"very"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfdHJhaXR8dHJhaXQ6Y3V0ZQ
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=trait:sweet type=has_trait weight=0.9
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfdHJhaXR8dHJhaXQ6c3dlZXQ
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition weight=0.4 props={"modality":"possible","verified":false}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
```

Read it back — the node carries the descriptive facts, the relation histogram carries the shape:

```text
[cheetah_data/default]> GRAPH_NODE_GET id=cat:luna
SUCCESS,id=cat:luna,payload=<base64>
# decodes to: {"id":"cat:luna","labels":["animal","cat"],"props":{"name":"Luna","sex":"female"},"created_at":…,"updated_at":…}

[cheetah_data/default]> GRAPH_NEIGHBOR_TYPES id=cat:luna direction=out limit=16 weighted=1
SUCCESS,count=3,next_cursor=*,payload=<base64>
# decodes to: [{"type":"has_trait","count":2,"weighted":1.8},{"type":"has_breed","count":1,"weighted":1},{"type":"has_condition","count":1,"weighted":0.4}]
```

Why it is modeled this way:

- **"may be sterile" is not a new edge type.** It is `has_condition` with `weight=0.4` and
  `props={"modality":"possible","verified":false}`. A hedge-specific type (`may_have_condition`) would
  force every later query to know the whole family of names; confidence in the weight keeps one query
  shape — and because traversal cost is `1/weight`, `COST_LIMIT` naturally prunes shaky facts first.
- **Traits and breeds are nodes, not props**, so the reverse question ("which of my animals are
  sweet?") is a prefix scan from `trait:sweet` instead of a full scan of every node's props.
- **The speaker is a node too** (`person:owner`), so possessives ("my cat") resolve to an edge rather
  than being lost.
- **`GRAPH_NODE_SET` upserts**: re-asserting the same sentence later keeps `created_at`, and omitting
  `labels=`/`props=` preserves the stored ones instead of blanking them.

### Reading a sentence

> *"I would like to have a litter for my cat."*

**Step 1 — record the intent.** A wish is a fact about the speaker; storing it makes the answer
revisitable and lets a later turn ask "what was I trying to do?".

```text
[cheetah_data/default]> GRAPH_NODE_SET id=intent:breed_litter labels=intent props={"goal":"litter","status":"open"}
SUCCESS,node_set,id=intent:breed_litter
[cheetah_data/default]> GRAPH_EDGE_SET from=person:owner to=intent:breed_litter type=wants weight=0.8
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8d2FudHN8aW50ZW50OmJyZWVkX2xpdHRlcg
[cheetah_data/default]> GRAPH_EDGE_SET from=intent:breed_litter to=cat:luna type=about weight=1.0
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfGFib3V0fGNhdDpsdW5h
```

**Step 2 — resolve the anchor.** "my cat" → the `owns` edge out of `person:owner`, i.e. `cat:luna`.
Every retrieval starts from an ID-anchored node.

**Step 3 — ask what is known, cheaply first.** `GRAPH_NEIGHBOR_TYPES` (above) is a histogram, not a
hydration: it tells the model *that* a `has_condition` edge exists before paying to read it.

**Step 4 — test the blockers the intent implies.** "Litter" needs a fertile female, so query
fertility, filtering out facts too weak to act on:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.weight >= 0.3 RETURN edges LIMIT 8
SUCCESS,return=edges,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"id":"MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl","from":"cat:luna","to":"condition:sterile","type":"has_condition","directed":true,"weight":0.4,"props":{"modality":"possible","verified":false},…}]

[cheetah_data/default]> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.props.verified = false RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"cat:luna","type":"has_condition","to":"condition:sterile","weight":0.4}]

[cheetah_data/default]> GRAPH_NEIGHBORS id=cat:luna direction=in type=owns limit=8
SUCCESS,count=1,next_cursor=*,payload=<base64>
# decodes to: [{"id":"MXxwZXJzb246b3duZXJ8b3duc3xjYXQ6bHVuYQ","from":"person:owner","to":"cat:luna","type":"owns",…}]
```

**Step 5 — answer from rows, then write back the conclusion.** Every clause of the reply is backed by
a returned row: *Luna is a female siamese (node props + `has_breed`), so a litter is biologically on
the table; the one blocker on record is a **possible, unverified** sterility (confidence 0.4), so the
next step is a vet fertility check.* That conclusion belongs in the graph:

```text
[cheetah_data/default]> GRAPH_NODE_SET id=action:vet_fertility_check labels=action props={"priority":"high"}
SUCCESS,node_set,id=action:vet_fertility_check
[cheetah_data/default]> GRAPH_EDGE_SET from=intent:breed_litter to=condition:sterile type=blocked_by weight=0.4
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfGJsb2NrZWRfYnl8Y29uZGl0aW9uOnN0ZXJpbGU
[cheetah_data/default]> GRAPH_EDGE_SET from=intent:breed_litter to=action:vet_fertility_check type=requires weight=0.9
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfHJlcXVpcmVzfGFjdGlvbjp2ZXRfZmVydGlsaXR5X2NoZWNr
[cheetah_data/default]> GRAPH_QUERY MATCH (id='intent:breed_litter')-[:*]->(*) RETURN paths LIMIT 8
SUCCESS,return=paths,matches=3,next_cursor=*,payload=<base64>
# decodes to: [{"from":"intent:breed_litter","type":"about","to":"cat:luna","weight":1},
#              {"from":"intent:breed_litter","type":"blocked_by","to":"condition:sterile","weight":0.4},
#              {"from":"intent:breed_litter","type":"requires","to":"action:vet_fertility_check","weight":0.9}]
```

The intent node is now a small plan: what it is about, what blocks it, what would unblock it. When the
vet answers, flip `verified` on the `has_condition` edge (upsert) and set the intent's `status`.

### Two more pairs

> **Write:** *"Marco joined Acme's Berlin office in 2019 and reports to Elena; Elena reports to Dana."*

```text
[cheetah_data/default]> GRAPH_NODE_SET id=person:marco labels=person props={"name":"Marco"}
SUCCESS,node_set,id=person:marco
[cheetah_data/default]> GRAPH_NODE_SET id=person:elena labels=person props={"name":"Elena"}
SUCCESS,node_set,id=person:elena
[cheetah_data/default]> GRAPH_NODE_SET id=person:dana labels=person props={"name":"Dana","title":"cfo"}
SUCCESS,node_set,id=person:dana
[cheetah_data/default]> GRAPH_NODE_SET id=org:acme labels=org
SUCCESS,node_set,id=org:acme
[cheetah_data/default]> GRAPH_NODE_SET id=city:berlin labels=city,place
SUCCESS,node_set,id=city:berlin
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=org:acme type=works_at weight=1.0 props={"since":2019,"office":"berlin"}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298d29ya3NfYXR8b3JnOmFjbWU
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=person:elena type=reports_to weight=1.0
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298cmVwb3J0c190b3xwZXJzb246ZWxlbmE
[cheetah_data/default]> GRAPH_EDGE_SET from=person:elena to=person:dana type=reports_to weight=1.0
SUCCESS,edge_set,id=MXxwZXJzb246ZWxlbmF8cmVwb3J0c190b3xwZXJzb246ZGFuYQ
[cheetah_data/default]> GRAPH_EDGE_SET from=org:acme to=city:berlin type=located_in weight=1.0
SUCCESS,edge_set,id=MXxvcmc6YWNtZXxsb2NhdGVkX2lufGNpdHk6YmVybGlu
```

> **Read:** *"Who ends up approving Marco's expense report?"* → intent = walk the reporting chain
> upward, so the shape is multi-hop over one edge type:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:reports_to]->(*) HOPS 1..3 RETURN paths LIMIT 16
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"reports_to","to":"person:elena","weight":1},
#              {"from":"person:elena","type":"reports_to","to":"person:dana","weight":1}]
```

> **Read:** *"Who works in the Berlin office?"* → the fact lives on the edge, so filter on edge props
> (served by the `graph/idx/` secondary index), or turn the question around and scan in-edges:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:works_at]->(*) WHERE edge.props.office = 'berlin' RETURN edges LIMIT 8
SUCCESS,return=edges,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","to":"org:acme","type":"works_at","weight":1,"props":{"office":"berlin","since":2019},…}]

[cheetah_data/default]> GRAPH_NEIGHBORS id=org:acme direction=in type=works_at limit=8
SUCCESS,count=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","to":"org:acme","type":"works_at","weight":1,"props":{"office":"berlin","since":2019},…}]
```

> "How is Marco connected at all?" is the open-ended version — wildcard type, bounded fan-out:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:*]->(*) HOPS 1..2 BRANCH_LIMIT 8 COST_LIMIT 5 RETURN paths LIMIT 16
SUCCESS,return=paths,matches=4,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"reports_to","to":"person:elena","weight":1},
#              {"from":"person:marco","type":"works_at","to":"org:acme","weight":1},
#              {"from":"org:acme","type":"located_in","to":"city:berlin","weight":1},
#              {"from":"person:elena","type":"reports_to","to":"person:dana","weight":1}]
```

> **Write:** *"My friend Sara Q. moved to Lisbon in 2025 and is strictly gluten-free — she's celiac."*
> Props containing spaces must be base64-encoded JSON (see the failure modes below):

```text
[cheetah_data/default]> GRAPH_NODE_SET id=person:sara labels=friend,person props=eyJuYW1lIjoiU2FyYSBRIiwibm90ZSI6Im1ldCBpbiAyMDE5In0=
SUCCESS,node_set,id=person:sara
[cheetah_data/default]> GRAPH_NODE_SET id=city:lisbon labels=city,place
SUCCESS,node_set,id=city:lisbon
[cheetah_data/default]> GRAPH_NODE_SET id=diet:gluten_free labels=constraint,diet
SUCCESS,node_set,id=diet:gluten_free
[cheetah_data/default]> GRAPH_EDGE_SET from=person:sara to=city:lisbon type=lives_in weight=1.0 props={"since":2025}
SUCCESS,edge_set,id=MXxwZXJzb246c2FyYXxsaXZlc19pbnxjaXR5Omxpc2Jvbg
[cheetah_data/default]> GRAPH_EDGE_SET from=person:sara to=diet:gluten_free type=follows_diet weight=1.0 props=eyJzdHJpY3QiOnRydWUsInJlYXNvbiI6ImNlbGlhYyBkaXNlYXNlIn0=
SUCCESS,edge_set,id=MXxwZXJzb246c2FyYXxmb2xsb3dzX2RpZXR8ZGlldDpnbHV0ZW5fZnJlZQ
```

> **Read:** *"Where should we have dinner when I visit Sara?"* → intent = collect the constraints that
> bind a plan (place + diet) in one wildcard hop, then check strictness and who else shares it:

```text
[cheetah_data/default]> GRAPH_NODE_SET id=intent:plan_dinner labels=intent props={"with":"person:sara","status":"open"}
SUCCESS,node_set,id=intent:plan_dinner
[cheetah_data/default]> GRAPH_EDGE_SET from=intent:plan_dinner to=person:sara type=about weight=1.0
SUCCESS,edge_set,id=MXxpbnRlbnQ6cGxhbl9kaW5uZXJ8YWJvdXR8cGVyc29uOnNhcmE
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:sara')-[:*]->(*) RETURN paths LIMIT 8
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:sara","type":"follows_diet","to":"diet:gluten_free","weight":1},
#              {"from":"person:sara","type":"lives_in","to":"city:lisbon","weight":1}]

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:sara')-[:follows_diet]->(*) WHERE edge.props.strict = true RETURN edges LIMIT 4
SUCCESS,return=edges,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:sara","to":"diet:gluten_free","type":"follows_diet","weight":1,"props":{"reason":"celiac disease","strict":true},…}]

[cheetah_data/default]> GRAPH_NEIGHBORS id=diet:gluten_free direction=in type=follows_diet limit=8
SUCCESS,count=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:sara","to":"diet:gluten_free","type":"follows_diet","weight":1,"props":{"reason":"celiac disease","strict":true},…}]
```

Answer: *Lisbon, and it must be strictly gluten-free (celiac) — not a preference.*

### Recording uncertainty, alternatives and corrections

The command surface looks absolute — an edge either exists or it does not — but three channels carry
uncertainty, and a model should use all three deliberately:

1. **`weight`** — how sure you are (0–1). It is also the traversal cost (`1/weight`), so shaky facts
   are the first thing `COST_LIMIT` prunes and the last thing a ranked walk expands.
2. **`props`** — *why* you are unsure: `modality`, `verified`, `source`, `as_of`, `ruled_out`.
   Equality on these is served by the `graph/idx/` secondary index, so they are filters, not just
   annotations.
3. **Shape** — mutually exclusive readings become *several edges tied together*, not one fuzzy edge.

Take a sentence with no single truth in it:

> *"Marco likes either the color light blue or aquamarine, I don't remember."*

Write **both** alternatives, each with half the confidence, tagged with a shared group id:

```text
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=color:light_blue type=likes weight=0.5 props={"oneof":"marco_fav_color","modality":"uncertain","verified":false}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298bGlrZXN8Y29sb3I6bGlnaHRfYmx1ZQ
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=color:aquamarine type=likes weight=0.5 props={"oneof":"marco_fav_color","modality":"uncertain","verified":false}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298bGlrZXN8Y29sb3I6YXF1YW1hcmluZQ
```

The convention (a *client* contract — the engine assigns no meaning to it) is: **weights inside one
`oneof` group sum to ~1.0**. That makes the three questions a model actually asks all answerable with
the same query shape:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) RETURN paths LIMIT 8
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":0.5},
#              {"from":"person:marco","type":"likes","to":"color:light_blue","weight":0.5}]
# → "one of these two, and I genuinely don't know which"

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.weight >= 0.8 RETURN count
SUCCESS,return=count,matches=0,next_cursor=*
# → "nothing I'd assert about Marco's colors" — absence of a confident row is itself the answer

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:*]->(*) WHERE edge.props.oneof = 'marco_fav_color' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":0.5},
#              {"from":"person:marco","type":"likes","to":"color:light_blue","weight":0.5}]
# → the alternative set as a unit, index-served
```

**When the open question itself matters**, promote the group to a `hypothesis:` node so it can carry
status and be *found* later — otherwise an unresolved doubt is only reachable if you already know
which entity to ask about:

```text
[cheetah_data/default]> GRAPH_NODE_SET id=hypothesis:marco_fav_color labels=hypothesis props={"question":"favourite_color","status":"open"}
SUCCESS,node_set,id=hypothesis:marco_fav_color
[cheetah_data/default]> GRAPH_EDGE_SET from=person:owner to=hypothesis:marco_fav_color type=unsure_about weight=1.0
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8dW5zdXJlX2Fib3V0fGh5cG90aGVzaXM6bWFyY29fZmF2X2NvbG9y
[cheetah_data/default]> GRAPH_EDGE_SET from=hypothesis:marco_fav_color to=person:marco type=about weight=1.0
SUCCESS,edge_set,id=MXxoeXBvdGhlc2lzOm1hcmNvX2Zhdl9jb2xvcnxhYm91dHxwZXJzb246bWFyY28
[cheetah_data/default]> GRAPH_EDGE_SET from=hypothesis:marco_fav_color to=color:light_blue type=alternative weight=0.5
SUCCESS,edge_set,id=MXxoeXBvdGhlc2lzOm1hcmNvX2Zhdl9jb2xvcnxhbHRlcm5hdGl2ZXxjb2xvcjpsaWdodF9ibHVl
[cheetah_data/default]> GRAPH_EDGE_SET from=hypothesis:marco_fav_color to=color:aquamarine type=alternative weight=0.5
SUCCESS,edge_set,id=MXxoeXBvdGhlc2lzOm1hcmNvX2Zhdl9jb2xvcnxhbHRlcm5hdGl2ZXxjb2xvcjphcXVhbWFyaW5l

[cheetah_data/default]> GRAPH_NEIGHBORS id=person:owner direction=out type=unsure_about limit=8
SUCCESS,count=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:owner","to":"hypothesis:marco_fav_color","type":"unsure_about","weight":1,…}]
# → "what am I still unsure about?" is now a one-hop read, not a scan
```

**Correcting the record** is an upsert, because `(from, to, type, directed)` identifies an edge. When
Marco confirms it was aquamarine, raise the winner and demote — do not silently erase — the loser:

```text
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=color:aquamarine type=likes weight=1.0 props={"oneof":"marco_fav_color","modality":"asserted","verified":true}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298bGlrZXN8Y29sb3I6YXF1YW1hcmluZQ
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=color:light_blue type=likes weight=0.05 props={"oneof":"marco_fav_color","ruled_out":true}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298bGlrZXN8Y29sb3I6bGlnaHRfYmx1ZQ
[cheetah_data/default]> GRAPH_NODE_SET id=hypothesis:marco_fav_color props={"question":"favourite_color","status":"resolved"}
SUCCESS,node_set,id=hypothesis:marco_fav_color

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.weight >= 0.8 RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":1}]

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.props.ruled_out = true RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:light_blue","weight":0.05}]
```

A demoted edge (`weight=0.05`, `ruled_out=true`) still answers *"weren't you saying light blue?"* and
keeps the same confidence filter from ever surfacing it as fact. Use `GRAPH_EDGE_DEL` only when the
alternative should be forgotten outright rather than remembered as wrong.

The full vocabulary, all built from the same three channels:

| The sentence says… | Encoding |
| --- | --- |
| plain assertion — "Marco works at Acme" | `weight=1.0`, `props={"verified":true}` |
| hedge — "may be", "I think", "probably" | `weight` 0.3–0.5 + `props={"modality":"possible","verified":false}` |
| disjunction — "either A or B" | one edge per alternative, shared `props.oneof`, weights summing to ~1 |
| hearsay — "Elena told me Marco hates red" | keep the claim, mark the origin: `props={"source":"elena"}` and a weight below first-hand facts |
| staleness — "as of 2024", "back then" | `props={"as_of":2024}`; re-assert with a higher weight when confirmed |
| negation — "Marco doesn't like red" | an explicit `dislikes` edge (or `props={"negated":true}`) — never a missing edge, which is indistinguishable from ignorance |
| retraction — "actually, not that" | upsert to a low weight with `props={"ruled_out":true}`, or `GRAPH_EDGE_DEL` to forget |

```text
[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=color:red type=dislikes weight=0.9 props={"source":"elena","as_of":2024}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298ZGlzbGlrZXN8Y29sb3I6cmVk
[cheetah_data/default]> GRAPH_NEIGHBOR_TYPES id=person:marco direction=out limit=8 weighted=1
SUCCESS,count=2,next_cursor=*,payload=<base64>
# decodes to: [{"type":"likes","count":2,"weighted":1.05},{"type":"dislikes","count":1,"weighted":0.9}]
# → count 2 vs weighted 1.05 is the signature of a resolved alternative set: two rows, one belief
```

Where the query language stops, and what to do about it:

- **`WHERE` is AND-only** — no `OR`, no `NOT`, no parentheses (see
  [`parseGraphWhereClause`](graph.go), which splits on `AND`). Disjunction is therefore expressed as
  **data, not syntax**: tag the alternatives with `props.oneof` and one equality predicate returns the
  whole group. `!=` covers per-field negation (`WHERE edge.type != 'likes'`), and anything richer
  belongs in the client, which can union the results of two queries.
- **Exclusivity is not enforced.** Nothing stops both alternatives from sitting at `weight=1.0`; the
  engine treats prefixes and props as opaque bytes by design. Keeping a group normalized is the
  writer's job — re-assert the whole group whenever you touch one of its edges.
- **An upsert resets what it does not mention.** `weight` defaults to `1.0` and `props` is replaced,
  not merged, so re-asserting an edge just to add a prop silently promotes a `0.4` hedge to a
  certainty. Always write the complete record: read it back first if you do not have every field.
- **One weight per edge.** If you need confidence *and* strength ("loves" vs "likes"), keep confidence
  in `weight` and put the other dimension in props (`props={"intensity":"strong"}`).
- **No validity intervals.** `as_of`/`since` props plus a re-assert are the whole story; the engine
  does no temporal reasoning.
- **Absence never means false.** `ERROR,node_not_found` and an empty `matches=0` mean *nothing is
  recorded* — a model must report that, not infer a negative.

### Choosing the retrieval command

| The sentence asks… | Use | Notes |
| --- | --- | --- |
| "What do you know about X?" | `GRAPH_NODE_GET` then `GRAPH_NEIGHBOR_TYPES` | histogram first, hydrate only the relation types that matter |
| "What is X's R?" (one hop, known relation) | `GRAPH_NEIGHBORS id=X type=R` | plain adjacency page, cursor-resumable |
| "Who R's X?" (reverse) | `GRAPH_NEIGHBORS id=X direction=in type=R`, or `MATCH (id='X')<-[:R]-(*)` | in-adjacency is its own index; keep X on the left and flip the arrow |
| "Is that certain?" | `… WHERE edge.weight >= 0.8` | `matches=0` means "nothing I'd assert", not "false" |
| "…but only the ones where <condition>" | `GRAPH_QUERY … WHERE …` | `edge.props.<k> = <literal>` is index-served; `edge.weight >= n` filters by confidence |
| "Who is behind/above/downstream of X?" | `GRAPH_QUERY … HOPS 1..n RETURN paths` | bound with `BRANCH_LIMIT` + `COST_LIMIT`; `paths` is the compact form |
| "Is X connected to Y at all?" | `GRAPH_QUERY MATCH (id='X')-[:*]->(id='Y') HOPS 1..n RETURN count` | `count` skips payload hydration entirely |
| "How much do you know about X?" | `GRAPH_DEGREE id=X direction=both weighted=1` | one number, no edge reads |
| "What do you remember overall?" | `PAIR_SUMMARY x01676e3a` / `PAIR_SCAN x01676e3a <limit>` | `x01676e3a` is the hex form of the reserved node namespace `\x01gn:` |

Note on `RETURN nodes`: it returns the sorted unique ids of *all* endpoints of the matched edges,
which includes the anchor node itself — drop it client-side if you only want the far side.

Inventorying the stored nodes goes through the trie directly, since graph records are ordinary pair
keys — `\x01gn:` followed by the base64url-encoded node id:

```text
[cheetah_data/default]> PAIR_SCAN x01676e3a 4
SUCCESS,count=4,next_cursor=x01676e3a59326c306554707361584e69623234,items=01676e3a593239755a476c30615739754f6e4e305a584a70624755:6;01676e3a593246304f6d7831626d45:2;01676e3a59326c30655470695a584a73615734:44;01676e3a59326c306554707361584e69623234:60
# item 2: hex 01676e3a593246304f6d7831626d45 → "\x01gn:" + "Y2F0Omx1bmE" → node id "cat:luna", payload key 2

[cheetah_data/default]> PAIR_SUMMARY x01676e3a 1 8
SUCCESS,command=PAIR_SUMMARY,count=17,total_payload_bytes=2542,min_payload_bytes=120,max_payload_bytes=182,min_key=1,max_key=71,max_depth=35,self_terminal=0,branch_count=6,branches=59:6;63:5;61:2;64:2;5a:1;62:1
# count is the number of stored nodes; the *_payload_bytes totals shift by a few bytes between runs
# because every node record embeds RFC3339Nano timestamps of varying length
```

### Failure modes an extractor must not hit

```text
[cheetah_data/default]> GRAPH_NODE_SET id=cat sitter labels=person
SUCCESS,node_set,id=cat
# arguments are whitespace-split key=value tokens: "sitter" was dropped and a node named "cat" was created

[cheetah_data/default]> GRAPH_NODE_SET id=person:sara props={"name":"Sara Q"}
ERROR,invalid_props:unexpected end of JSON input
# the space inside the JSON ended the token; base64-encode props that contain spaces

[cheetah_data/default]> GRAPH_QUERY MATCH (*)-[:owns]->(id='cat:luna') RETURN edges LIMIT 8
ERROR,graph_query_parse_failed:left_node_must_be_anchored_by_id

[cheetah_data/default]> GRAPH_NODE_GET id=cat:pepper
ERROR,node_not_found
```

- **Ids, labels and edge types may not contain spaces.** They are parsed as whitespace-separated
  `key=value` tokens, so `id=cat sitter` silently truncates to `cat` — slugify (`cat:sitter`,
  `person:sara`) and keep the human form in `props` (base64) instead.
- **Any `props=` value containing a space must be base64-encoded JSON.** Inline JSON is only safe when
  it has no spaces at all (`{"since":2019,"office":"berlin"}` is fine).
- **The left node of `MATCH` must be ID-anchored** — `MATCH (*)-[:owns]->(id='cat:luna')` is rejected.
  Reverse questions ("who owns Luna?") keep the anchor on the left and flip the arrow instead:
  `MATCH (id='cat:luna')<-[:owns]-(*)`, or read the in-adjacency directly with
  `GRAPH_NEIGHBORS id=cat:luna direction=in type=owns`.
- **`ERROR,node_not_found` is an answer, not a failure.** A model must say "I have nothing on
  cat:pepper" rather than inventing an edge; never fabricate a node id to make a query succeed.
- **Responses are one line.** Decode `payload=` from base64 into JSON before reasoning over it, and
  page with `next_cursor` (`*` means exhausted) instead of raising `limit` without bound.

### Drop-in prompt skeleton

```text
You turn user statements into cheetah-db graph commands, and user questions into graph queries.
Emit only commands, one per line, no prose.

WRITING a statement:
  1. Node per entity: id = "<type>:<slug>", lowercase, no spaces (person:, cat:, org:, city:,
     trait:, condition:, diet:, action:, intent:).
  2. labels = the kinds it belongs to (comma-separated, no spaces).
  3. props = descriptive attributes as compact JSON, no spaces; base64-encode the JSON if any
     value contains a space.
  4. Edge per relation: type = snake_case verb (owns, works_at, reports_to, has_trait,
     has_condition, lives_in, follows_diet).
  5. weight = your confidence 0..1 (asserted fact 1.0, hedged "may/might/I think" 0.3-0.5).
     Put the hedge itself in props: {"modality":"possible","verified":false}.
  6. "either A or B": write BOTH edges, each with props {"oneof":"<group>"} and weights that sum
     to ~1. Never collapse a disjunction into one guess and never drop it for lack of certainty.
  7. Negation is an explicit edge (dislikes, or props {"negated":true}) — a missing edge means
     "unknown", not "false". Hearsay carries props {"source":"<who>"} and a lower weight.
  8. Never invent a relation the sentence does not state. Re-assert known nodes freely: writes upsert.

READING a question or a wish:
  1. Create intent:<goal> and link it: <speaker> -[:wants]-> intent, intent -[:about]-> <subject>.
  2. Resolve the anchor node id from the sentence (possessives resolve through the speaker's edges).
  3. Cheapest sufficient probe first: GRAPH_NODE_GET / GRAPH_NEIGHBOR_TYPES / GRAPH_DEGREE.
  4. Then the targeted read: GRAPH_NEIGHBORS (one hop, direction=in for reverse) or
     GRAPH_QUERY (predicates, HOPS 1..n, RETURN edges|nodes|paths|count).
  5. Answer only from returned rows; report weight/modality as uncertainty in the wording.
  6. Write the conclusion back: intent -[:blocked_by]-> …, intent -[:requires]-> action:….
```

Intent *scoring* (ranking several candidate intents for one sentence) is a natural fit for the
`PREDICT_*` tables described above: store each candidate under a `PREDICT_SET key=<intent-prefix>`
and let `PREDICT_QUERY` merge the context windows instead of hard-coding a classifier.

For the system around these recipes — episodic/semantic/procedural memory tiers, the teach and recall
loops, a six-turn session where the assistant learns a fact mid-conversation, online intent routing
with `PREDICT_TRAIN`, consolidation and forgetting, and the adapter contract — see
[`studies/GRAPH_LLM.md`](studies/GRAPH_LLM.md).

## Streaming Helpers

- `PAIR_SCAN <prefix> [limit]` favors namespace exhaustiveness: `PAIR_SCAN ctx: 100` walks the first
  100 contexts alphabetically, while `PAIR_SCAN * 0` streams the entire trie.
- Client adapters typically alternate `PAIR_SCAN` with `READ` to hydrate payloads; keep a cache of
  `<value_size, table_id, entry_id>` tuples nearby when building custom adapters.

## Reducer Hooks

- `PAIR_REDUCE counts ctx:` aggregates follower counts directly inside Go and emits the packed
  payloads inline, allowing MKNS-style reducers or other statistical aggregators to run without SQL.
- Custom reducers can be registered in Go via the reducer registry (see `reducers.go`). Each reducer
  receives the pair-trie iterator and can emit any payload format; clients decode the base64 payload
  per reducer contract.

## Operational Notes

- Prefer `screen` or `tmux` for long-lived sessions. Launch commands with explicit timeouts (≤30 min
  unless otherwise justified) so stalled reducers cannot block future sessions.
- TCP keep-alives are configurable via `[server] keepalive_seconds` (or `CHEETAH_TCP_KEEPALIVE_SECONDS`).
  Increase this window for WAN clients so idle sockets survive long reducer sweeps; set to `0` to rely on OS defaults.
- `CHEETAH_HEADLESS=1` disables the interactive CLI while keeping the TCP listener up. When running in
  WSL or remote shells, pair it with `screen -dmS cheetahdb ...` and monitor `screen -ls` /
  `screen -wipe` before rebuilding.
- Rotate logs under `var/eval_logs/`—benchmark helpers emit files such as
  `cheetah_db_benchmark_<timestamp>.log` so you can diff throughput across cache sizes or reducer
  tweaks.

For the agent-facing operational map (file ownership, contracts, config/env reference, and known
gaps), see [`AGENTS.md`](AGENTS.md) in this directory.

## Tree Indexing & Algorithmic Logic

cheetah-db’s performance hinges on a deterministic, trie-backed index that treats every namespace or
key prefix as a path through fixed-size `PairTable` nodes. Each node has fixed-span children (one raw
byte per hop by default, optionally two when configured) plus independent terminal/child/jump flags,
so the same structure that indexes keys also highlights “hot” shared prefixes. The entry layout is
defined in [`types.go`](types.go) and the traversal logic lives in [`database.go`](database.go) and
[`pair_codec.go`](pair_codec.go):

- Every namespace (e.g., `ctx:BERLIN`) is stored as raw bytes. Walking those bytes selects a slot
  inside the root pair table and either follows a child table ID or marks the node as terminal with
  the absolute payload key. A single node can be both terminal and a parent, so `ctx:` and
  `ctx:BERLIN` can both carry values.
- `PAIR_SCAN` streams children in lexical order, so namespace enumeration only touches the branches
  that exist. A node spans `∑_{i=1..pair_index_bytes} 256^i` logical branches (256 when indexing a
  single byte, 256 + 65,536 when indexing two).
- **Adaptive node storage** (`adaptive_pair_index`, on by default) decouples that logical span from
  what a node actually costs. Each node file carries a small header and stores its entries either as
  a sorted, binary-searched list of `[branchKey|entry]` records (sparse nodes) or as the classic
  direct-mapped array reached by `PairHeaderSize + branchIndex * PairEntrySize` (populated nodes). A
  node uses a list only when its dense array would exceed `pair_list_max_bytes` (4 KiB), and converts
  to the array once the list passes that budget. A 1-byte-stride node is 2,828 bytes — already inside
  one filesystem block — so it stays dense and keeps its original behaviour; the list container
  therefore applies to 2-byte-stride nodes. That is where it matters, since such a node previously
  reserved ~707 KB even when holding a handful of children: on a 20k-key benchmark the `pairs/`
  directory drops from 776.5 MiB to 4.7 MiB apparent (~81–87 MiB to 4.5 MiB actually allocated) and a
  full-trie enumeration drops from ~1.6–1.9 s to 5 ms, while insert and lookup stay within run-to-run
  noise. Since a 1-byte-stride database gets no benefit, the savings require `pair_index_bytes = 2`.
  The stride and the flag are pinned per database in `pairs/format.dat`; changing them requires
  `RESET_DB`.
- Jump nodes collapse unique suffixes into a compact segment so single-key branches no longer
  allocate entire tables. `PAIR_SET` writes the remainder of a key into a jump node whenever a branch
  has no siblings, and the node is split automatically if a later key shares part of that suffix. On
  deletions the engine rechecks whether a child table now has only one branch and promotes it back
  into a jump when possible, keeping disk usage proportional to the number of active prefixes.
- `PAIR_REDUCE` executes reducers while it walks the trie. As soon as a branch is materialized, the
  reducer can hydrate payloads (`readValuePayload`) and emit inline aggregates, taking advantage of
  prefix locality to amortize disk reads and to reuse cached payloads when sibling prefixes live in
  the same tables.
- Future performance work leverages this “tree indexing” foundation: precompute namespace statistics
  (counts, rolling hashes, Top-K summaries) and branch-local caches. Because the layout guarantees
  stable offsets, prefetchers or GPU-backed reducers can schedule precise `ReadAt` calls without
  scanning.
- `PAIR_SUMMARY` is the first tooling pass for that roadmap: it reuses the same tree walk to report
  per-namespace totals and per-branch counts without hydrating every payload. Passing
  `PAIR_SUMMARY ctx: 2 64`, for example, shows the hottest two-depth prefixes (capped at 64
  branches) while also returning min/max payload sizes—perfect for prioritizing which contexts to
  mirror into GPU reducers or similarity scans.

Treating namespace keys as traversable trees keeps INSERT/READ latency tied to fixed math instead of
variable-length scans. It also gives future tooling (fuzzy namespace matching, prefix-similarity
lookups, or trie-level compression) a solid footing because the invariants hold everywhere: branch
per configured byte-span, attach metadata where a path terminates, and stream the structure without
rebuilding it in memory.

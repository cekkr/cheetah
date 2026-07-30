# cheetah-db

High-throughput key/value store with a trie-backed pair table purpose-built for statistical data
pipelines. cheetah-db ingests byte-encoded contexts, n-gram payloads, and arbitrary binary blobs,
keeps them partitioned by value size, and exposes TCP + CLI commands that stream results with
bounded memory use. The engine targets workloads where millions of probabilities, counters, or
other dense analytical slices must be served with predictable latency.

<img src="https://github.com/cekkr/cheetah/blob/main/assets/cheetah-logo.png?raw=true" width="480"/>

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
- **Associative recall.** `GRAPH_RECALL` takes several terms at once and spreads activation from all
  of them, returning everything they co-activate above a requested precision — each hit with the
  seeds that reached it, its distance, the edges that justify it, and a novelty score. Because the
  seeds combine in noisy-OR, what two topics *share* outranks what either topic merely touches, which
  is where correlations nobody asked for turn up. Free-text seeds resolve through a lexical index and
  declared synonym edges; `GRAPH_SIMILAR` answers the sibling question, "what else behaves like this".
- **Payload caching.** `src/database.go` keeps a bounded cache (defaults: 16k entries ≈64 MB) keyed by
  `<value_size, table_id, entry_id>` so hot payloads never hit disk. Tune it with
  `CHEETAH_PAYLOAD_CACHE_ENTRIES`, `CHEETAH_PAYLOAD_CACHE_MB`, or
  `CHEETAH_PAYLOAD_CACHE_BYTES`, or disable caching entirely by setting any of them to `0`.
- **Resource-aware reducers.** The server detects available CPU cores at startup, samples live
  CPU/I/O pressure, and scales reducer worker pools accordingly so concurrent connections avoid
  exhausting compute or disk bandwidth.
- **Multi-tenant databases.** `src/engine.go` multiplexes logical databases under `cheetah_data/<name>`
  and exposes them over both CLI and TCP, making it easy to isolate experiments or pilot rollouts.
- **Reducer streaming.** Reducers stream inline payloads through a bounded worker pool, overlap disk
  reads with encoding work, and emit cursor tokens so callers can page through arbitrarily large
  namespaces without restarting the command.

## Architecture at a Glance

All server sources live in `src/` (`package main`, import path `cheetahdb/src`). The repository root
holds documentation, `config.example.ini`, `build.sh`, and the two standalone build targets `gold/`
(reference prototype) and `demo/graph-nell/` (benchmark client).

- `src/main.go` boots the TCP listener (`0.0.0.0:4455` by default) plus the local CLI and routes commands
  to database handles returned by `engine.GetDatabase(name)`.
- `src/engine.go` lazily instantiates `Database` structs backed by `cheetah_data/<dbname>` and ensures all
  tables (main keys, values, recycling queues, pair trie) flush cleanly at shutdown.
- `src/database.go` orchestrates CRUD operations:
  - `MainKeysTable` stores compact metadata describing payload size + pointer offsets.
  - `ValuesTable` files hold fixed-width blobs grouped by byte length and table ID so offsets remain
    arithmetic instead of scan-based. Each table reserves new slots from an atomic in-memory
    high-water mark seeded at open, so queued asynchronous writes never hand two inserts the same
    offset.
  - `RecycleTable` files keep tombstoned slots per value size so inserts can reuse space without
    compaction pauses.
  - `PairTable` nodes store child pointers and terminal flags independently, unlocking
    prefix-sharing namespaces such as `ctx:`, `ctxv:`, `prob:`, or `meta:`.
- `src/server.go` accepts newline-delimited commands (`INSERT`, `READ`, `PAIR_SCAN`, `PAIR_REDUCE`, …)
  over TCP so external adapters can talk to the engine without embedding Go code.

## Heavy Statistical Workloads

cheetah-db was designed to stage dense statistical datasets such as n-gram probabilities,
continuation metadata, and concept caches:

- Use dedicated namespaces (e.g., `ctx:`, `prob:<order>`, `cont:`, `meta:`) to keep context metadata,
  quantized probability tables, and continuation payloads separated yet streamable.
- Mirror Top-K caches, follow-up penalties, or other heavy slices directly into the trie so cache
  lookups never re-open SQLite or object stores.
- When benchmarking reducers, export `CHEETAHDB_BENCH=1` and run
  `go test -run TestCheetahDBBenchmark -count=1 -v ./src` for reproducible throughput snapshots.
- See [`AGENTS.md`](AGENTS.md) for cache-sizing guidance, launch recipes, and namespace
  troubleshooting notes when you run sustained ingest/eval loops.

## Memory & SSD Guidelines

- Start the server with a payload-cache budget sized for your workload. 64 MB works for small corpora
  while 128–256 MB keeps multi-GB statistical slices in RAM. Increase the byte budget before raising
  the entry count so the cache retains whole payloads.
- Inserts seed the cache and deletes invalidate their slots, so chaining ingest → reduction → decoder
  stages benefits from a single long-lived process. Avoid restarting the server between stages unless
  you want to profile cold starts.
- Pair-trie writers are serialized for the duration of one mutation. Multiple TCP connections can
  issue `PAIR_SET` concurrently without losing acknowledged shared-prefix mappings; reads and scans
  remain concurrent.
- To prime the cache after restarts, issue low-limit `PAIR_SCAN ctx:` passes (following cursors) or
  scripted `READ` loops over the namespaces you are about to benchmark. This shifts the I/O churn
  into RAM, keeping SSD wear predictable.
- When profiling disk I/O, set `CHEETAH_PAYLOAD_CACHE_ENTRIES=0` (or the MB/byte variants) to disable
  caching entirely, then re-enable it immediately afterward for day-to-day runs.

## Building & Running

```bash
bash build.sh              # produces ./cheetah-server (or: go build -o cheetah-server ./src)
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

Every command is **one line in, one line out**. A response opens with `SUCCESS` or `ERROR,<reason>`
and continues as comma-separated `key=value` fields; anything list- or record-shaped travels inside a
single `payload=<base64>` field (JSON once decoded), never as extra lines. The CLI and the TCP
listener speak the same vocabulary — only `DATABASE`, `RESET_DB` and `EXIT` are handled by the
front-ends, because they change *which* database the connection is talking to.

Three argument dialects coexist, which is why a command's family is easier to guess than its syntax:

| Dialect | Used by | Example |
| --- | --- | --- |
| **positional** | the KV and `PAIR_*` families, `LOG_FLUSH`, `FORK_ASSIGN`, job polling | `PAIR_SCAN ctx: 64 x000104` |
| **`key=value` tokens** | every `GRAPH_*` except `GRAPH_QUERY`, every `PREDICT_*` except the two job-polling ones, every `CLUSTER_*` | `GRAPH_DEGREE id=alice direction=both` |
| **clause language** | `GRAPH_QUERY` only | `MATCH (id='alice')-[:follows]->(*) HOPS 1..2 RETURN paths` |

`FILE_CHECKPOINT` adds a fourth, smaller convention: bare uppercase flags (`DROP_CACHE`,
`CLOSE_HANDLES`, `IDLE=<dur>`). Wherever a command takes a **byte prefix** (`PAIR_*`, `FORK_ASSIGN`)
the text is used raw — write `x<HEX>` for binary, and `*` for "the whole trie / no prefix".

The authoritative inventory is the `ExecuteCommand` switch in [`src/database.go`](src/database.go)
plus the two front-ends and the two tables it consults first — the micro-command registry
([`src/micro_command.go`](src/micro_command.go)) and the alias table
([`src/command_alias.go`](src/command_alias.go)). The overlaps between the families below are being
factored out into composable **micro-commands**, with every current name kept as an alias that
reproduces its old response byte for byte; `JOB` and `DEL` have landed (see
[Micro-commands](#micro-commands--the-canonical-form)), `SCAN`/`GET`/`SET` are still planned in
[`NEXT_STEPS.md`](NEXT_STEPS.md).

### Micro-commands — the canonical form

A micro-command is a verb, a target and `key=value` modifiers; the historical names are aliases over
them, so nothing below replaces anything you already use.

| Micro-command | Absorbs | Notes |
| --- | --- | --- |
| `DEL values key=<n>` | `DELETE` | one value |
| `DEL pairs key=<v>` | `PAIR_DEL` | one name |
| `DEL pairs prefix=<p> [limit=<n>] [payloads=0\|1]` | `PAIR_PURGE` | a namespace; `payloads=0` unlinks the names and leaves the values readable by key — the one thing `PAIR_PURGE` could not say. `prefix=*` is the whole trie |
| `DEL graph node=<id> [cascade=1]` | `GRAPH_NODE_DEL` | a node, optionally with its edges |
| `DEL graph from=<a> to=<b> [type=] [directed=]` | `GRAPH_EDGE_DEL` | one edge |
| `JOB submit <command>` · `JOB submit command=<base64>` | `PAIR_REDUCE_ASYNC`, `PREDICT_INHERIT_ASYNC` | answers `job=<id>`; only commands registered as bounded are accepted (today `PAIR_REDUCE`, `PREDICT_INHERIT_BATCH`), anything else is `ERROR,command_not_submittable` |
| `JOB status id=<job>` | `PAIR_REDUCE_STATUS`, `PREDICT_INHERIT_STATUS` | `state=`, `progress=`, `completed=`/`total=`, plus the family's own counters |
| `JOB fetch id=<job>` | `PAIR_REDUCE_FETCH`, `PREDICT_INHERIT_FETCH` | the submitted command's own response under `job=<id>` while completed, `PENDING,…` while running, and it consumes the job |

Micro-commands take `key=value` only. A binary value is written `x<hex>` exactly as elsewhere, and
must be when it contains spaces — the positional forms survive inside the alias rewriters, which
re-encode for you. `RESET_DB` is deliberately **not** a `DEL` target: it lives in the front-ends
because it changes what the connection is pointing at, not what a database contains.

```text
[cheetah_data/notes]> DEL pairs prefix=ctx: payloads=0
SUCCESS,deleted=2
# the names are gone from the trie; READ on their keys still answers

[cheetah_data/notes]> JOB submit PAIR_REDUCE counts ctx:
SUCCESS,job=reduce_1,kind=reduce,command=PAIR_REDUCE,state=queued,total=0,reducer=counts
[cheetah_data/notes]> JOB status id=reduce_1
SUCCESS,job=reduce_1,kind=reduce,state=completed,progress=100.00,completed=2,total=2,reducer=counts
[cheetah_data/notes]> JOB fetch id=reduce_1
SUCCESS,job=reduce_1,reducer=counts,count=2,items=…
# the same line PAIR_REDUCE would have answered, under the job id
```

#### What each alias runs

An alias is three things: a **rewriter** that translates the historical argument dialect into a micro
call, the **micro-command** itself, and a **formatter** that rebuilds the historical response line
from the micro response's named fields. Below, `r ← MICRO …` is the micro call, `r.field` reads a
field out of its response, and `→` is the line the client receives.

Two rules apply to every alias and are not repeated in each block:

- **an `ERROR` from the micro-command passes through untouched**, because micro error tokens are
  deliberately the same words the old commands used (`not_found`, `already_deleted`,
  `node_not_found`, `edge_not_found`). The only exception is the `job_not_found` /
  `job_manager_unavailable` remap noted in the `JOB` blocks.
- **the old dialect's own validation happens in the rewriter**, before the micro-command runs. That
  is why the wordings below (`missing_key`, `pair_purge_requires_prefix`, …) never reach `DEL` or
  `JOB`. The async submits are the exception: they hand the whole argument list to the submitted
  command, so its errors surface from inside `JOB submit` — still synchronously, still before a job
  id exists.

```text
DELETE <key>
    if <key> is absent          → "ERROR,missing_key"
    if <key> is not a uint64    → "ERROR,invalid_key_format"
    r ← DEL values key=<key>
    → "SUCCESS,key=" + r.key + "_deleted"

PAIR_DEL <name>                          # <name> is the whole rest of the line, spaces included
    v = parse_bytes(<name>)              # plaintext, or x<hex>
    if v is malformed hex       → "ERROR,invalid_hex_value:…"
    if v is empty               → "ERROR,pair_value_cannot_be_empty"
    r ← DEL pairs key=x<hex of v>        # re-encoded: the micro dialect splits on whitespace
    → "SUCCESS,pair_deleted"

PAIR_PURGE <prefix> [<limit>]
    if <prefix> is absent        → "ERROR,pair_purge_requires_prefix"
    if <prefix> is malformed hex → "ERROR,invalid_hex_value:…"
    if <limit> is not an int     → "ERROR,invalid_limit"
    p = (<prefix> == "*") ? "*" : x<hex of parse_bytes(<prefix>)>
    r ← DEL pairs prefix=<p> [limit=<limit>]      # payloads= is left at its default of 1
    → "SUCCESS,purged=" + r.deleted

GRAPH_NODE_DEL id=<id> [cascade=<b>]
    if <id> is empty            → "ERROR,graph_node_del_requires_id"
    r ← DEL graph node=<id> [cascade=<b>]
    → "SUCCESS,node_deleted,id=" + r.node

GRAPH_EDGE_DEL from=<a> to=<b> [type=<t>] [directed=<d>]
    if <a> or <b> is empty      → "ERROR,graph_edge_del_requires_from_and_to"
    r ← DEL graph from=<a> to=<b> [type=<t>] [directed=<d>]
    → "SUCCESS,edge_deleted,id=" + r.edge
```

The two async trios are the same shape over `JOB`. They differ only in which command they submit and
which fields their formatter picks — which is exactly the redundancy the envelope removed:

```text
PAIR_REDUCE_ASYNC <mode> <prefix> [<limit>] [<cursor>]
    if the argument list is empty  → "ERROR,pair_reduce_requires_args"
    r ← JOB submit command=base64("PAIR_REDUCE " + <arguments verbatim>)
      # the rest of the validation happens inside JOB submit, synchronously and before an
      # id exists: ERROR,unknown_reducer_mode, ERROR,invalid_limit, ERROR,invalid_hex_value:…
    → "SUCCESS,reducer=" + r.reducer + ",job=" + r.job + ",state=queued"

PAIR_REDUCE_STATUS <job>
    if <job> is absent             → "ERROR,missing_job_id"
    r ← JOB status id=<job>
    on ERROR: job_not_found        → "ERROR,reduce_job_not_found"
    → "SUCCESS,job=" + r.job + ",state=" + r.state + ",progress=" + r.progress
      # a failed job still answers SUCCESS here, with state=failed — as it always did

PAIR_REDUCE_FETCH <job>
    if <job> is absent             → "ERROR,missing_job_id"
    r ← JOB fetch id=<job>                        # consumes the job, done or failed
    on ERROR: job_not_found        → "ERROR,reduce_job_not_found"
              job_failed:<err>     → passes through
    if r is PENDING → "PENDING,job=,reducer=,state=,progress=,completed=,total="   (values from r)
    → "SUCCESS," + r without its job= field       # byte-identical to the PAIR_REDUCE line

PREDICT_INHERIT_ASYNC table=<t> items=<base64 json[]> [merge=<mode>]
    r ← JOB submit command=base64("PREDICT_INHERIT_BATCH " + <arguments verbatim>)
    → "SUCCESS,table=" + r.table + ",job=" + r.job + ",state=queued,total=" + r.total

PREDICT_INHERIT_STATUS <job>
    if <job> is absent             → "ERROR,missing_job_id"
    r ← JOB status id=<job>
    on ERROR: job_not_found        → "ERROR,predict_inherit_job_not_found"
    if r.state == "failed" and r.error → "ERROR,job_failed:" + r.error
    → "SUCCESS,job=,state=,progress=,completed=,total=,merged=,skipped=,failed="  (values from r)

PREDICT_INHERIT_FETCH <job>
    if <job> is absent             → "ERROR,missing_job_id"
    r ← JOB fetch id=<job>
    on ERROR: job_not_found        → "ERROR,predict_inherit_job_not_found"
    if r is PENDING → "PENDING,job=,state=,progress=,completed=,total=,merged=,skipped=,failed="
    → "SUCCESS,job=,merged=,skipped=,failed=,total="                              (values from r)
```

Note what the last two blocks say about `JOB status` on a **failed** job: it answers `SUCCESS` with
`state=failed` plus an `error=` field, and each alias decides what that means in its own dialect —
`PAIR_REDUCE_STATUS` kept reporting it as a status, `PREDICT_INHERIT_STATUS` kept turning it into
`ERROR,job_failed:`. `JOB fetch` is the one that errors outright, which is what both old fetches did.

Every alias today resolves to **exactly one** micro call; the rewriter/formatter pair is where the
sequence would grow if a future alias needed several. The pairs live in
[`src/command_alias.go`](src/command_alias.go), and
[`src/command_alias_test.go`](src/command_alias_test.go) pins each rendered line against what the
command answered before the decomposition.

### The layer a command belongs to

Most confusion between two commands disappears once you know which layer each one addresses. There
are two storage layers; everything else is a view over them:

```text
value layer   INSERT / READ / EDIT / DELETE      bytes, addressed by a numeric key
name  layer   PAIR_SET / PAIR_GET / PAIR_DEL …   byte prefixes in a trie → numeric keys
  ├─ views over one walk  PAIR_SCAN (names) · PAIR_SUMMARY (statistics) · PAIR_REDUCE (payloads)
  ├─ graph records        GRAPH_* — nodes/edges/adjacency under reserved \x01..\x05 prefixes
  └─ recall               GRAPH_RECALL / GRAPH_SIMILAR / GRAPH_TERM_INDEX
side tables   PREDICT_*                          prediction_<name>.table files, not the trie
control       DATABASE / RESET_DB · CLUSTER_* / FORK_ASSIGN · SYSTEM_STATS / LOG_FLUSH / FILE_CHECKPOINT
```

A value and a name are independent on purpose: `INSERT` returns a key, `PAIR_SET` binds a name to
it, and either can be removed without the other. That is why there is no single "put" command, and
why four different commands delete four different things.

### Session and database scope

Handled in the front-ends ([`src/main.go`](src/main.go), [`src/server.go`](src/server.go)), not in
the dispatcher, because they mutate per-connection state.

| Command | What it means |
| --- | --- |
| `DATABASE <name> [key=value …]` | Point **this connection** at another logical database (`cheetah_data/<name>`), creating it on first use. Overrides are remembered for that name: `pair_bytes=`/`pair_index_bytes=`, `adaptive_pair_index=`, `pair_list_max_bytes=`, `pair_list_max_fill_percent=`, `payload_cache_entries=`, `payload_cache_mb=`, `payload_cache_bytes=`. The trie-geometry ones only bite when the directory is *created*. |
| `RESET_DB [name] [key=value …]` | Close the database, delete `cheetah_data/<name>` on disk, reopen it empty. The only way to adopt a new trie geometry, since `pairs/format.dat` wins on every ordinary open. Omitting the name resets whichever database the connection currently holds. |
| `EXIT` | CLI only: leave the interactive loop. A TCP client just closes the socket. |

**In context** — you keep each experiment in its own database, and a benchmark run needs the wide
stride, which only a rebuild can adopt:

```text
[cheetah_data/default]> DATABASE notes
SUCCESS,database_changed_to_notes
[cheetah_data/notes]> DATABASE bench pair_bytes=2 payload_cache_entries=0
SUCCESS,database_changed_to_bench
[cheetah_data/bench]> RESET_DB bench
SUCCESS,database_reset_to_bench
# the overrides were recorded on the name, so the rebuilt directory is the one that adopts them
[cheetah_data/bench]> DATABASE notes
SUCCESS,database_changed_to_notes
```

### Value layer — the bytes

| Command | What it means |
| --- | --- |
| `INSERT:<size> <payload>` | Store `<payload>` in the value table for that byte length and return its **absolute key** (a `main_keys` offset). The declared `<size>` is validated against the payload — it decides which file the bytes land in, so a wrong number is an error, not a hint. Back-to-back equal-size inserts reserve distinct slots before their asynchronous writes are queued. |
| `INSERT <payload>` | The same write with the size inferred from the payload. |
| `READ <abs_key>` | Hydrate the bytes behind a key: one arithmetic `ReadAt` (or a payload-cache hit). Answers `SUCCESS,size=<n>,value=<bytes>`. |
| `EDIT <abs_key> <payload>` | Overwrite the value under an existing key. A length change **relocates** it into the correctly sized value table and recycles the old slot, so the key stays valid. There is no `EDIT:<size>` form — the size always comes from the payload. |
| `DELETE <abs_key>` | Tombstone the value, push its slot onto the recycle stack, invalidate its cache entry. Any pair name still pointing at that key is **not** removed. |

**In context** — staging one context payload, correcting it, and retiring it. Note the key survives an
edit that changes the length, and that a wrong declared size is refused rather than silently accepted:

```text
[cheetah_data/notes]> INSERT:18 ctx:BERLIN|CONTEXT
SUCCESS,key=1
[cheetah_data/notes]> READ 1
SUCCESS,size=18,value=ctx:BERLIN|CONTEXT
[cheetah_data/notes]> EDIT 1 ctx:BERLIN|CONTEXT|v2
SUCCESS,key=1_updated
[cheetah_data/notes]> READ 1
SUCCESS,size=21,value=ctx:BERLIN|CONTEXT|v2
# 18 → 21 bytes: the payload moved to another value table, the key did not change
[cheetah_data/notes]> INSERT:16 ctx:NAXOS|CONTEXT
ERROR,value_size_mismatch (expected 16, got 17)
[cheetah_data/notes]> DELETE 1
SUCCESS,key=1_deleted
[cheetah_data/notes]> READ 1
ERROR,key_not_found (deleted)
```

### Name layer — the pair trie

| Command | What it means |
| --- | --- |
| `PAIR_SET <prefix> <abs_key>` | Bind a byte prefix to a value key. Upserts; the prefix may be any byte string (`x<HEX>` for binary), and a prefix of another prefix is legal — a trie node is terminal and a parent at the same time. Complete mutations are serialized, so concurrent writers sharing ancestors retain every acknowledged binding. |
| `PAIR_SET_HIDDEN <prefix> <abs_key>` | The same binding with the hidden flag set: `PAIR_SCAN`/`PAIR_SUMMARY`/`PAIR_REDUCE` skip it unless they pass `include_hidden=1`. A **visibility bit on one entry**, not a separate namespace. |
| `PAIR_PUT_BATCH items=<base64 json[]> [hidden=1] [keys=1] [continue_on_error=1]` | **Store and bind many pairs in one request.** Each item is `{"k":"<prefix>","v":"<value>"}`, both fields following the same `x<HEX>` rule as a positional argument. This is `INSERT` + `PAIR_SET` per item without the two round trips each — the cost that dominates a bulk ingest, where the server idles while the client waits. Not a transaction: items are independent and applied in order, so the reply always carries `requested`/`applied`/`failed` (plus `first_error`) rather than a bare `ERROR`. By default it stops at the first bad item; `continue_on_error=1` skips and keeps going. Assigned keys are returned in `payload=` only with `keys=1` — write-once rows do not need them and a large batch should not pay for them. Cap: 10 000 items. |
| `PAIR_GET <prefix>` | Resolve exactly one name to its key. A point lookup, never a scan — a prefix with no terminal of its own answers `ERROR`, even when keys exist beneath it. |
| `PAIR_DEL <prefix>` | Unbind one name. The value it pointed at survives; pair `DELETE` with it to reclaim the bytes. |
| `PAIR_PURGE <prefix\|*> [batch]` | Unbind **and** delete the payloads of every name under a prefix, looping inside the server until the namespace is empty (`batch` sizes each page, default 4096). This is the bulk form of `PAIR_DEL` + `DELETE`, moved server-side so a namespace wipe is seconds instead of thousands of round trips. `*` empties the whole trie. Payloads are deleted in parallel, the trie entries one at a time — the concurrent-purge race that used to fail mid-way or leave entries behind is fixed (`TestPairPurgeSharedAncestors`). Use `RESET_DB` instead when the whole database is disposable: recreating the directory is cheaper than walking it. |

**In context** — a name is bound, resolved, and hidden from ordinary sweeps. `PAIR_GET` is a point
lookup, so the parent prefix answers `not_found` even though keys live beneath it:

```text
[cheetah_data/notes]> PAIR_SET ctx:BERLIN 1
SUCCESS,pair_set
[cheetah_data/notes]> PAIR_GET ctx:BERLIN
SUCCESS,key=1
[cheetah_data/notes]> PAIR_GET ctx:
ERROR,not_found
[cheetah_data/notes]> PAIR_SET_HIDDEN ctx:_wip 4
SUCCESS,pair_set_hidden
[cheetah_data/notes]> PAIR_SCAN ctx: 8
SUCCESS,count=1,items=6374783a4245524c494e:1
[cheetah_data/notes]> PAIR_SCAN ctx: 8 * include_hidden=1
SUCCESS,count=2,items=6374783a4245524c494e:1;6374783a5f776970:4
# `*` in the cursor position means "from the beginning"; the hidden entry only shows on request
[cheetah_data/notes]> PAIR_DEL ctx:_wip
SUCCESS,pair_deleted
```

### Reading a namespace — one walk, three payload contracts

The first three descend the same subtree and differ only in what they are willing to pay for; the
last three are the async envelope around `PAIR_REDUCE`.

| Command | What it means |
| --- | --- |
| `PAIR_SCAN <prefix> [limit] [cursor] [include_hidden=1]` | **The names.** An ordered page of `<hex_prefix>:<abs_key>` items plus `next_cursor` when more remain. Pages are complete, strictly increasing and resumable: exactly `limit` items until the last one. |
| `PAIR_SUMMARY <prefix> [depth] [branch_limit] [include_hidden=1]` | **The shape, without hydrating a byte.** Terminal count, total/min/max payload bytes, min/max key, max depth, and per-branch fan-out counts down to `depth` (capped at `branch_limit`, default 32). The "how much is under here, and where is it dense?" probe you run *before* committing to a scan. |
| `PAIR_REDUCE <mode> <prefix> [limit] [cursor] [include_hidden=1]` | **The bytes, already processed.** The same page as `PAIR_SCAN`, with each payload hydrated and pushed through a registered reducer, returned inline as base64 — so a client never issues one `READ` per row. Modes: `counts`/`count`/`probabilities`/`probs`/`backoffs`/`continuations`/`continuation` (payload pass-through) and `degree`/`triangle`/`pagerank_seed` (graph adjacency statistics). New modes are registered in [`src/reducers.go`](src/reducers.go), not added to the dispatcher. |
| `PAIR_REDUCE_ASYNC <mode> <prefix> [limit] [cursor]` | The identical request, detached: returns a `job=<id>` immediately instead of blocking on a long sweep. Jobs are in-memory and do not survive a restart. |
| `PAIR_REDUCE_STATUS <job_id>` | How far that job has got — state, progress percent, completed/total. No results. |
| `PAIR_REDUCE_FETCH <job_id>` | `PENDING,…,progress=…` while it runs, then **exactly** the synchronous `PAIR_REDUCE` response, `next_cursor` included. |

**In context** — five city contexts are staged under `ctx:`. The same namespace is then read three
ways, and once asynchronously:

```text
[cheetah_data/notes]> PAIR_SCAN ctx: 3
SUCCESS,count=3,next_cursor=x6374783a4d554e494348,items=6374783a4245524c494e:1;6374783a4c4953424f4e:5;6374783a4d554e494348:7
[cheetah_data/notes]> PAIR_SCAN ctx: 3 x6374783a4d554e494348
SUCCESS,count=2,items=6374783a5041524953:4;6374783a504f52544f:6
# no next_cursor on the second page: the namespace is exhausted

[cheetah_data/notes]> PAIR_SUMMARY ctx: 1 8
SUCCESS,command=PAIR_SUMMARY,count=5,total_payload_bytes=91,min_payload_bytes=17,max_payload_bytes=21,min_key=1,max_key=7,max_depth=6,self_terminal=0,branch_count=4,branches=50:2;42:1;4c:1;4d:1
# 5 names, 91 bytes of payload, and no byte of it was read; branch 50 ("P") holds 2 of them

[cheetah_data/notes]> PAIR_REDUCE counts ctx: 2
SUCCESS,reducer=counts,count=2,next_cursor=x6374783a4c4953424f4e,items=6374783a4245524c494e:1:Y3R4OkJFUkxJTnxDT05URVhUfHYy;6374783a4c4953424f4e:5:Y3R4OkxJU0JPTnxDT05URVhU
# same items as PAIR_SCAN plus a third field per row: the payload, base64, no READ needed

[cheetah_data/notes]> PAIR_REDUCE_ASYNC counts ctx: 4096
SUCCESS,reducer=counts,job=reduce_1,state=queued
[cheetah_data/notes]> PAIR_REDUCE_STATUS reduce_1
SUCCESS,job=reduce_1,state=running,progress=0.00
[cheetah_data/notes]> PAIR_REDUCE_FETCH reduce_1
SUCCESS,reducer=counts,count=5,items=6374783a4245524c494e:1:Y3R4OkJFUkxJTnxDT05URVhUfHYy;…
```

### Graph — writing what is the case

Full grammar in [Graph Command Language](#graph-command-language).

| Command | What it means |
| --- | --- |
| `GRAPH_NODE_SET id=<id> [labels=…] [props=…] [references=<base64-json[]>]` | Upsert an **entity**. `labels` are the kinds it belongs to (filterable, valueless); `props` are descriptive attributes you will not query on their own; `references` are bounded complete sentences with `id`, `text`, optional `source`, and `ordinal`. Omitting a field keeps the stored one; `references=-` clears the sentence list; `created_at` survives. |
| `GRAPH_NODE_DEL id=<id> [cascade=1]` | Forget the entity. Without `cascade=1` its incident edges are left dangling — pass it whenever you mean "and everything that was said about it". |
| `GRAPH_EDGE_SET from= to= [type=] [weight=] [directed=] [confidence=] [modality=] [ambiguity=] [props=] [autocreate=0]` | Upsert a **relation**, identified by the tuple `(from, to, type, directed)`. `weight` is traversal strength (cost `1/weight`); `confidence`/`modality` are how sure the claim is; `ambiguity` names the group of mutually exclusive readings it belongs to. Missing endpoint nodes are stubbed out unless `autocreate=0`. |
| `GRAPH_EDGE_SET_BATCH items=<base64 json[]> [continue_on_error=1] [type=] [directed=] [weight=] [props=]` | The same upsert for many edges in one round-trip, with the top-level tokens acting as per-item defaults. Reports `requested/applied/created/updated/failed`. |
| `GRAPH_EDGE_DEL from= to= [type=] [directed=]` | **Forget** the relation. Different from writing `confidence=ruled_out`, which keeps it on record as excluded and still answerable. |
| `GRAPH_AMBIGUITY_SET from= group= options=<id>[=<share>][,…] [type=] [normalize=0]` | Write a whole set of **mutually exclusive readings** at once and normalize their shares to sum to 1. The engine has no `OR`, so a disjunction is stored as a group rather than expressed as a query. |
| `GRAPH_AMBIGUITY_RESOLVE from= group= winner= [drop=1]` | Collapse the set: the winner becomes `certain`, the others `ruled_out` (or are deleted with `drop=1`), and the group dissolves. |

**In context** — *"Marco's cat Luna is a siamese and may be sterile; they both live in Berlin."* One
hedged edge, one batch for the rest, and an alternative set for what is genuinely unknown:

```text
[cheetah_data/notes]> GRAPH_NODE_SET id=cat:luna labels=animal,cat props={"name":"Luna","sex":"female"}
SUCCESS,node_set,id=cat:luna
[cheetah_data/notes]> GRAPH_EDGE_SET from=person:marco to=cat:luna type=owns weight=1.0
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298b3duc3xjYXQ6bHVuYQ
[cheetah_data/notes]> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=possible
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
# no such node as condition:sterile yet — the edge stubs it out (autocreate=0 would refuse instead)

[cheetah_data/notes]> GRAPH_EDGE_SET_BATCH items=<base64 of [{"from":"cat:luna","to":"city:berlin","type":"lives_in","weight":1.0}, …4 rows]> continue_on_error=1
SUCCESS,requested=4,applied=4,created=4,updated=0,failed=0
[cheetah_data/notes]> GRAPH_EDGE_SET_BATCH items=<base64 of [{"to":"city:berlin","type":"lives_in"}]> continue_on_error=1
SUCCESS,requested=1,applied=0,created=0,updated=0,failed=1,payload=<base64>
# payload decodes to: [{"index":0,"error":"graph_edge_set_requires_from_and_to"}]

# "he moved to Lisbon — or was it Berlin? I think Berlin."
[cheetah_data/notes]> GRAPH_AMBIGUITY_SET from=person:marco type=lives_in group=where_marco options=city:berlin=0.7,city:lisbon
SUCCESS,ambiguity_set,group=where_marco,options=2,confidence_sum=1.0000
# the undeclared alternative takes the remaining 0.3 — one command, both readings on record

[cheetah_data/notes]> GRAPH_EDGE_DEL from=cat:luna to=condition:sterile type=has_condition
SUCCESS,edge_deleted,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
[cheetah_data/notes]> GRAPH_NODE_DEL id=cat:temp cascade=1
SUCCESS,node_deleted,id=cat:temp
```

### Graph — calling it back

Four of these read the *same* adjacency index — `GRAPH_NEIGHBORS`, `GRAPH_DEGREE`,
`GRAPH_NEIGHBOR_TYPES` and `GRAPH_QUERY` — and differ only in how much of it they hydrate.

| Command | What it means |
| --- | --- |
| `GRAPH_NODE_GET id=<id>` | One node record. `ERROR,node_not_found` is an answer — "nothing recorded" — not a failure. |
| `GRAPH_EDGE_GET from= to= [type=] [directed=]` | One edge record, addressed by its identifying tuple. |
| `GRAPH_NEIGHBORS id= [direction=out\|in\|both] [type=] [limit=] [cursor=]` | **A page of edge records** around a node. The plain adjacency read, cursor-resumable. |
| `GRAPH_DEGREE id= [direction=] [type=] [weighted=1]` | **How many** (and how heavy), as a number. No records are read — the cheapest "how much do I know about X?". |
| `GRAPH_NEIGHBOR_TYPES id= [direction=] [limit=] [cursor=] [weighted=1]` | **Which relations, and how many of each** — a compact `[{type,count,weighted}]` histogram without hydrating a single edge. The fast probe before deciding what to hydrate. |
| `GRAPH_AMBIGUITY_GET from= group= [direction=] [limit=]` | One alternative group read back, strongest reading first, with `confidence_sum` and `top`. |
| `GRAPH_QUERY MATCH … [WHERE …] [HOPS …] [BRANCH_LIMIT …] [COST_LIMIT …] [RETURN …] [LIMIT …] [CURSOR …]` | The same walk with **predicates, several hops and explicit bounds**, and a choice of what comes back: `edges`, `nodes`, `paths` or just `count`. The left node must be ID-anchored so execution stays index-backed. |

**In context** — *"What do you know about Luna?"*, asked cheaply first and hydrated only where it
pays. Each of these reads the same `adj/out` index:

```text
[cheetah_data/notes]> GRAPH_DEGREE id=cat:luna direction=both type=* weighted=1
SUCCESS,id=cat:luna,direction=both,type=*,degree=4,weighted_degree=4.000000
# one number, zero records read: "there are four things on file"

[cheetah_data/notes]> GRAPH_NEIGHBOR_TYPES id=cat:luna direction=out limit=16 weighted=1
SUCCESS,count=3,next_cursor=*,payload=<base64>
# payload decodes to: [{"type":"has_breed","count":1,"weighted":1},{"type":"has_condition","count":1,"weighted":1},{"type":"lives_in","count":1,"weighted":1}]
# now you know a has_condition edge exists — before paying to read it

[cheetah_data/notes]> GRAPH_QUERY MATCH (id='cat:luna')-[:*]->(*) RETURN paths LIMIT 8
SUCCESS,return=paths,matches=3,next_cursor=*,payload=<base64>
# payload decodes to: [{"from":"cat:luna","type":"has_breed","to":"breed:siamese","weight":1},
#                      {"from":"cat:luna","type":"has_condition","to":"condition:sterile","weight":1},
#                      {"from":"cat:luna","type":"lives_in","to":"city:berlin","weight":1}]

[cheetah_data/notes]> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.modality >= 'probable' RETURN count
SUCCESS,return=count,matches=0,next_cursor=*
# the sterility is only `possible`, so nothing here is assertable — matches=0 means "not worth
# stating", never "false"

[cheetah_data/notes]> GRAPH_QUERY MATCH (id='city:berlin')<-[:lives_in]-(*) RETURN nodes LIMIT 8
SUCCESS,return=nodes,matches=4,next_cursor=*,payload=<base64>
# payload decodes to: ["cat:luna","cat:mia","city:berlin","person:marco"]
# reverse question, anchor still on the left; `nodes` includes the anchor itself

[cheetah_data/notes]> GRAPH_QUERY MATCH (*)-[:owns]->(id='cat:luna') RETURN edges LIMIT 8
ERROR,graph_query_parse_failed:left_node_must_be_anchored_by_id
# flip the arrow instead, or read GRAPH_NEIGHBORS id=cat:luna direction=in type=owns
```

### Graph — associative recall

| Command | What it means |
| --- | --- |
| `GRAPH_RECALL seeds=<t>[,…] [precision=] [hops=] [min_sources=] [direction=] [type=] [decay=] [expand=] [references=0\|1] [reference_limit=] [limit=] [branch_limit=] [budget=]` | **The question you don't know how to ask.** Spreads activation from every seed at once and returns everything they co-activate, ranked, each hit carrying the seeds that reached it, its conceptual distance, the evidence path and a novelty score. Seeds may be free text. `min_sources=2` narrows it to convergences — what several seeds *share*. `references=1` also hydrates complete node sentences and episodic payloads cited by `edge.props.src`, under a separate bound. |
| `GRAPH_SIMILAR id=<id> [by=context\|lexical\|all] [limit=] [precision=]` | **"What else behaves like this?"** — nodes with the same neighbours (distributional) or the same words in their id (lexical). No edge between them is required. |
| `GRAPH_TERM_INDEX [action=stats\|rebuild\|drop] [limit=] [cursor=]` | Maintenance of the derived `\x05gt:` lexical index that free-text seeds resolve through. It is never authoritative: exact ids and synonym edges keep working without it, and `rebuild` is resumable through `next_cursor`. |

**In context** — the conversation has been touching Luna and Marco, and nobody has asked a question
yet. Recall answers what a query cannot be written for:

```text
[cheetah_data/notes]> GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 limit=4
SUCCESS,command=GRAPH_RECALL,seeds=2,resolved=3,visited=7,expanded=10,hydrated=28,references=0,count=4,bridges=4,truncated=0,precision=0.100,payload=<base64>
# payload decodes to {"seeds":[{"term":"cat:luna","matches":[{"id":"cat:luna","score":1,"match":"exact"},
#                                                            {"id":"cat:mia","score":0.33,"match":"lexical"}]}, …],
#  "associations":[
#   {"id":"city:berlin","score":0.72325,"novelty":0.361625,"distance":1,"source_count":2,"bridge":true,
#    "sources":[{"seed":"cat:luna","activation":0.55,"hops":1},{"seed":"person:marco","activation":0.385,"hops":1}],
#    "via":[{"from":"cat:luna","type":"lives_in","to":"city:berlin","weight":1,"confidence":1,"modality":"certain"}]},
#   {"id":"color:aquamarine","score":0.686125,…,"source_count":2,"bridge":true,…},
#   {"id":"city:lisbon","score":0.240776,…,"via":[{…,"confidence":0.3,"modality":"unlikely"}]}]}
# note the last one: Lisbon scores low precisely because that edge is only `unlikely` — belief is
# part of the ranking, not a separate filter. `resolved=3` is 2 seeds, one of which also matched
# cat:mia lexically.

[cheetah_data/notes]> GRAPH_RECALL seeds=cat:luna,person:marco hops=3 precision=0.05 min_sources=2 limit=4
SUCCESS,command=GRAPH_RECALL,seeds=2,resolved=3,visited=7,expanded=14,hydrated=34,references=0,count=4,bridges=4,truncated=0,precision=0.050,payload=<base64>
# the "what do these two have to do with each other?" view: only nodes both seeds reach

[cheetah_data/notes]> GRAPH_RECALL seeds=berlin hops=1 precision=0.1 limit=4
SUCCESS,command=GRAPH_RECALL,seeds=1,resolved=1,visited=4,expanded=1,hydrated=3,references=0,count=3,bridges=0,truncated=0,precision=0.100,payload=<base64>
# "seeds":[{"term":"berlin","matches":[{"id":"city:berlin","score":0.495,"match":"lexical"}]}]
# a bare word, not an id — resolved through the term index, and it says so

[cheetah_data/notes]> GRAPH_SIMILAR id=cat:luna limit=3
SUCCESS,command=GRAPH_SIMILAR,id=cat:luna,count=3,truncated=0,payload=<base64>
# payload decodes to: [{"id":"cat:mia","score":0.777778,"context":0.666667,"lexical":0.333333,
#                       "shared_count":2,"shared":["breed:siamese","city:berlin"]}, …]
# luna and mia are never linked to each other; they are similar because of where they both point

[cheetah_data/notes]> GRAPH_TERM_INDEX action=stats
SUCCESS,command=GRAPH_TERM_INDEX,action=stats,enabled=1,entries=21
[cheetah_data/notes]> GRAPH_TERM_INDEX action=rebuild limit=4096
SUCCESS,command=GRAPH_TERM_INDEX,action=rebuild,nodes=9,terms=21,next_cursor=*
# next_cursor=* means the whole graph fitted in one slice; otherwise pass the token back
```

### Prediction tables

A separate store (`prediction_<name>.table`), addressed by prefix, with `table=<name>` selecting
which one (omitted = the unnamed default, which is why responses read `table=`). Prefixes **and**
every value in `value=`, `target=`, `sources=`, `negatives=` follow the same decoding rule as pair
keys: plaintext, or `x<hex>` for binary — a bare hex string is taken literally, so `negatives=646f67`
adds a value spelled `646f67` rather than `dog`. Context matrices and window specs cross the wire as
base64-encoded JSON. Values come back **hex-encoded** in `items=<hex>:<prob>`.

Failures in this family used to answer the bare reason (`inherit_sources_missing`) with no `ERROR,`
prefix. They no longer do: `ExecuteCommand` normalizes any response that opens with neither
`SUCCESS`, `ERROR` nor `PENDING` into `ERROR,<reason>`, so classifying on the prefix is safe here as
everywhere else.

| Command | What it means |
| --- | --- |
| `PREDICT_SET key= value= prob= [weights=] [table=]` | Declare a candidate value for a prefix with its probability and optional context weights. The write path. |
| `PREDICT_QUERY key= [keys=] [ctx=] [windows=] [key_windows=] [merge=avg\|sum\|max] [table=]` | Evaluate one or many prefixes and merge their probability windows under the current backend. The read path. |
| `PREDICT_TRAIN key= target= [ctx=] [lr=] [negatives=<value>[,…]] [table=]` | Move the stored weights toward a target through the forward/backward loop, optionally down-weighting the listed negatives. **Persistent learning.** |
| `PREDICT_CTX key= ctx= [mode=bias\|scale] [strength=] [table=]` | Apply a context adjustment **immediately, without training** — a nudge to this query, not a lesson. |
| `PREDICT_INHERIT key= target= sources=<value>[,…] [merge=] [table=]` | Seed a new value by merging existing ones — how a composite token starts life with its parts' context weights. Every source must already exist under `key`, or the command answers `inherit_sources_missing`. |
| `PREDICT_INHERIT_BATCH items=<base64 json> [key=] [merge=] [table=]` | The same merge for many targets in one call. |
| `PREDICT_INHERIT_ASYNC items=<base64 json> [key=] [merge=] [table=]` | The same batch, detached: returns `job=<id>`. |
| `PREDICT_INHERIT_STATUS <job_id>` | Progress of that job — merged/skipped/failed counters. |
| `PREDICT_INHERIT_FETCH <job_id>` | `PENDING` while it runs, then the batch results. |
| `PREDICT_BACKEND [mode=cpu\|gpu] [table=]` | Read or switch which merger a table uses. The "gpu" path is `webgpu-simulated` — CPU fan-out, not a real WebGPU binding. |
| `PREDICT_BENCH samples= window= [table=]` | Compare the two mergers on this host, so the choice above is measured rather than assumed. |

**In context** — three candidate continuations for the prefix `ctx:the`, then one round of learning
and one composite token seeded from its parts:

```text
[cheetah_data/notes]> PREDICT_SET key=ctx:the value=cat prob=0.6
SUCCESS,table=,prediction_values=1
[cheetah_data/notes]> PREDICT_SET key=ctx:the value=kitten prob=0.25
SUCCESS,table=,prediction_values=2
[cheetah_data/notes]> PREDICT_SET key=ctx:the value=dog prob=0.15
SUCCESS,table=,prediction_values=3
[cheetah_data/notes]> PREDICT_QUERY key=ctx:the
SUCCESS,count=3,backend=cpu,table=,items=636174:0.4269;6b697474656e:0.3009;646f67:0.2722
# items are <hex value>:<merged probability> — 636174 is "cat"

[cheetah_data/notes]> PREDICT_TRAIN key=ctx:the target=cat lr=0.05 negatives=dog
SUCCESS,table=,prediction_values=3,lr=0.0500
[cheetah_data/notes]> PREDICT_QUERY key=ctx:the
SUCCESS,count=3,backend=cpu,table=,items=636174:0.4344;6b697474656e:0.3007;646f67:0.2649
# cat up, dog down, and the change persisted — contrast PREDICT_CTX, which biases nothing on disk

[cheetah_data/notes]> PREDICT_INHERIT key=ctx:the target=feline sources=cat,kitten merge=avg
SUCCESS,table=,prediction_values=4,merged_sources=2
[cheetah_data/notes]> PREDICT_QUERY key=ctx:the
SUCCESS,count=4,backend=cpu,table=,items=636174:0.3191;66656c696e65:0.2655;6b697474656e:0.2209;646f67:0.1946
# "feline" was never trained: it arrived with the averaged context weights of cat and kitten

[cheetah_data/notes]> PREDICT_INHERIT key=ctx:the target=x sources=nosuchvalue
ERROR,inherit_sources_missing
# the prefix is added at the dispatcher boundary, so this family classifies like the others

[cheetah_data/notes]> PREDICT_BACKEND mode=gpu
SUCCESS,table=,backend=webgpu-simulated
[cheetah_data/notes]> PREDICT_BENCH samples=64 window=8
SUCCESS,table=,samples=64,window=8,bench=webgpu-simulated=12.75µs|cpu=1.167µs
# at this size the simulated backend is 10x slower — which is the point of having the command
```

### Cluster placement

Placement metadata only: the server does not route your commands for you.

| Command | What it means |
| --- | --- |
| `CLUSTER_UPDATE replication=<n> <nodeID>=host:port/weight …` (or `json=<base64>`) | Register the topology — who exists, where, and how many replicas a fork wants. Persisted to `cluster_topology.json`. |
| `CLUSTER_STATUS` | The whole picture: node list, replication factor, last update, assignment count. |
| `FORK_ASSIGN <prefix\|*>` | **One** placement: which `fork_id` a prefix hashes to and which nodes own it. The read that `CLUSTER_MOVE` overrides. |
| `CLUSTER_MOVE prefix=<bytes>\|fork=<id> node=<nodeID>` | Force a fork onto a node, build its transfer payload and gossip it to peers. Note the override lives in memory: it is **not** persisted across a restart (see [`NEXT_STEPS.md`](NEXT_STEPS.md)). |
| `CLUSTER_GOSSIP json=<base64>` | The inbound peer channel — heartbeats and `fork_move` messages. Machine-to-machine; do not drive it by hand, and only enable clustering among trusted nodes (the protocol is unauthenticated). |

**In context** — two nodes are registered, the `ctx:` shard is looked up, then pinned:

```text
[cheetah_data/notes]> CLUSTER_STATUS
SUCCESS,cluster_nodes=0,replication=1,updated=0001-01-01T00:00:00Z,nodes=,assignments=0
# the zero time means no topology has ever been registered

[cheetah_data/notes]> CLUSTER_UPDATE replication=2 nodeA=10.0.0.1:4455/2 nodeB=10.0.0.2:4455/1
SUCCESS,cluster_nodes=2,replication=2
[cheetah_data/notes]> CLUSTER_STATUS
SUCCESS,cluster_nodes=2,replication=2,updated=2026-07-22T22:01:21Z,nodes=nodeA@10.0.0.1:4455(cap=2)|nodeB@10.0.0.2:4455(cap=1),assignments=0

[cheetah_data/notes]> FORK_ASSIGN ctx:
SUCCESS,fork_id=45471590a920dbcc,nodes=nodeA|nodeB
# the prefix hashes to one fork; replication=2 puts it on both nodes

[cheetah_data/notes]> CLUSTER_MOVE prefix=ctx: node=nodeB
SUCCESS,fork_id=45471590a920dbcc,node=nodeB
[cheetah_data/notes]> FORK_ASSIGN ctx:
SUCCESS,fork_id=45471590a920dbcc,nodes=nodeB
# the override holds — until the process restarts, when the ring answers nodeA|nodeB again
```

### Server operations

| Command | What it means |
| --- | --- |
| `SYSTEM_STATS` | Live gauges: logical cores, GOMAXPROCS, goroutines, CPU percentages, per-second disk I/O deltas, payload-cache entries/bytes/hits/misses/evictions and an advisory bypass threshold. A cheap heartbeat between ingest and reduce loops. |
| `LOG_FLUSH [limit]` | Recent history: dump **and clear** the in-memory log ring (default depth 256). Entries come back as `payload=<base64>` decoding to a JSON array of strings — the response is still one line. |
| `FILE_CHECKPOINT [IDLE=<dur>] [DROP_CACHE] [CLOSE_HANDLES]` | Force the managed-file layer to act now: flush dirty sectors, optionally drop cached sectors and release idle file handles mid-run. The manual form of what shutdown does. |

**In context** — between an ingest stage and a reduce stage, check the machine, settle the files, and
read what the server logged:

```text
[cheetah_data/notes]> SYSTEM_STATS
SUCCESS,command=SYSTEM_STATS,timestamp=2026-07-22T22:01:21Z,logical_cores=8,gomaxprocs=8,goroutines=70,mem_alloc_bytes=2467088,mem_sys_bytes=13977864,process_cpu_pct=0.82,process_cpu_supported=1,system_cpu_pct=NA,system_cpu_supported=0,io_supported=0,recommended_workers=1:1;32:8;256:8;4096:8,payload_cache_enabled=1,payload_cache_entries=65,…
# `NA` + `_supported=0` is how a platform-unavailable metric reports (system CPU and /proc IO on
# macOS); `recommended_workers` is queue-depth → worker-count advice you can size a client on

[cheetah_data/notes]> FILE_CHECKPOINT
SUCCESS,file_checkpoint_flushed=0
# nothing was dirty
[cheetah_data/notes]> FILE_CHECKPOINT IDLE=0s CLOSE_HANDLES
SUCCESS,file_checkpoint_flushed=135
# IDLE=0s means "every handle counts as idle", so all 135 were released

[cheetah_data/notes]> LOG_FLUSH 3
SUCCESS,count=3,payload=<base64>
# payload decodes to: ["2026/07/23 00:01:20.234948 [INFO] Loaded database: scratch",
#                      "2026/07/23 00:01:20.239303 [INFO] Reset database: scratch",
#                      "2026/07/23 00:01:20.239869 [INFO] Loaded database: scratch"]
# the ring is now empty: LOG_FLUSH returns and clears in one step

[cheetah_data/notes]> NOPE arg
ERROR,unknown_command
```

### Telling the look-alikes apart

The command set grew feature by feature, so several names describe the same walk at a different
price. The distinctions that actually matter:

- **`DELETE` vs `PAIR_DEL` vs `PAIR_PURGE` vs `RESET_DB`** — four erasures at four scopes: one
  value, one name, a whole namespace (names *and* values), the whole database (including its pinned
  trie geometry). Deleting a value never removes the names pointing at it, and vice versa. The first
  three are now one verb with the scope written out — `DEL values key=` / `DEL pairs key=` /
  `DEL pairs prefix=` — which is the form to reach for when the distinction is what keeps biting.
- **`PAIR_GET` vs `READ`** — resolution vs hydration. `PAIR_GET` turns a name into a key;
  `READ` turns a key into bytes. Two calls, deliberately: a name can be re-pointed without touching
  the payload.
- **`PAIR_SCAN` vs `PAIR_SUMMARY` vs `PAIR_REDUCE`** — one walk, three contracts: names,
  statistics-without-hydration, hydrated-and-reduced payloads. Choosing wrongly costs an order of
  magnitude, not correctness.
- **`PAIR_SET` vs `PAIR_SET_HIDDEN`** — one flag bit on one entry, not a second namespace. Every
  reader can opt back in with `include_hidden=1`.
- **synchronous vs `_ASYNC` + `_STATUS`/`_FETCH`** — identical work, different envelope. Both
  families (`PAIR_REDUCE_*`, `PREDICT_INHERIT_*`) now run on the single `JOB` envelope and keep their
  own response fields only in their alias formatters, so a job id, a progress percentage and a
  fetch-consumes-the-job contract mean the same thing in both.
- **`GRAPH_NEIGHBORS` vs `GRAPH_DEGREE` vs `GRAPH_NEIGHBOR_TYPES` vs `GRAPH_QUERY … RETURN`** — one
  adjacency scan at four hydration levels: edge records, a count, a per-type histogram, and records
  filtered by predicates over several hops. `GRAPH_QUERY … RETURN count` and `GRAPH_DEGREE` answer
  the same question for a single unfiltered hop.
- **`GRAPH_QUERY` vs `GRAPH_RECALL`** — a question you can already phrase (anchored, exact, bounded)
  vs one you cannot (seeded, ranked, evidence-carrying).
- **`GRAPH_RECALL` vs `GRAPH_SIMILAR`** — what is *around* these terms vs what *behaves like* this
  node.
- **`GRAPH_EDGE_DEL` vs `confidence=ruled_out`** — forgetting vs recording an exclusion. Only the
  second keeps *"weren't you saying light blue?"* answerable.
- **`GRAPH_EDGE_SET` vs `GRAPH_EDGE_SET_BATCH` vs `GRAPH_AMBIGUITY_SET`** — one edge, many
  independent edges, one set of edges that are *alternatives to each other*.
- **`PREDICT_TRAIN` vs `PREDICT_CTX`** — a lesson that persists vs a bias applied to the next query.
- **`CLUSTER_STATUS` vs `FORK_ASSIGN`** — the whole map vs the owners of one prefix.
- **`SYSTEM_STATS` vs `LOG_FLUSH`** — live gauges vs recent history (and `LOG_FLUSH` is destructive:
  it clears what it returns).
- **`RESET_DB` vs `PAIR_PURGE *`** — recreate the directory, which is the only way to adopt new trie
  overrides, vs empty the trie in place, which keeps the format pinned in `pairs/format.dat`.

### Response and paging notes

- `PAIR_SCAN` replies carry `items=<hex_prefix>:<abs_key>;…` plus `next_cursor=<token>` when more
  pages remain; reissue `PAIR_SCAN <prefix> <limit> <token>` to continue. Cursor continuation is
  **positional** — there is no `CURSOR` keyword outside `GRAPH_QUERY`.
- `PAIR_REDUCE` returns the same items with an extra `:<base64>` field per row, so counters and
  probabilities arrive already hydrated. `PAIR_REDUCE_FETCH` answers
  `PENDING,…,progress=<percent>,completed=<n>,total=<n>` while the job runs — enough for a client to
  emit keep-alive logs — and then mirrors the synchronous response exactly, `next_cursor` included.
- Graph storage occupies four isolated namespaces (`node`, `edge`, `adj/out`, `adj/in`) plus the
  `graph/idx/` property index, so node/edge writes never force a full graph scan and
  `GRAPH_NEIGHBORS`/`GRAPH_QUERY` always execute as prefix scans over an adjacency index. The full
  `GRAPH_QUERY` grammar — patterns, predicates, `HOPS`/`BRANCH_LIMIT`/`COST_LIMIT`, `RETURN` modes —
  is in [Pattern queries](#pattern-queries--graph_query).
- Every paging command uses the same convention: `next_cursor=*` (or absent) means the scan is
  exhausted, and `*` as an *input* cursor or prefix means "start from the beginning / no prefix".

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
  SUCCESS,key=42
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

  Reducers control the payload schema; if you extend `src/commands.go` with a new reducer you only need
  to document how to decode its base64 block.
- `PAIR_PURGE <prefix> [page_size]` wipes every pair entry beneath the prefix and deletes the backing
  payload keys inside Go. Use `PAIR_PURGE ctx:` (or `*` to nuke the entire trie) when you need a hot
  reset before an ingest run—each batch clears up to 4096 entries so the purge finishes in seconds
  instead of hours of TCP round-trips.
- `RESET_DB [name]` closes the target database, deletes `cheetah_data/<name>` on disk, and reopens it
  empty so hot-path clients can wipe everything (pairs, value tables, metadata) with a single command.
  Omitting the name resets whichever database is currently selected on the connection/CLI prompt.
- `SYSTEM_STATS` is a cheap heartbeat: call it between ingest/reduce loops to track CPU, memory, and
  fd counts without spawning `top`. Because `src/database.go` formats it in CSV-like key/value pairs, it
  can be parsed by shell scripts (`awk -F,`) or structured log scrapers.

- **Prediction tables & context matrices.** The database can now host multiple prediction tables
  (stored as fixed-byte `prediction_<name>.table` files alongside the trie; JSON only appears on the
  CLI for request/response payloads) and expose GPU-style probability merges:

  - `PREDICT_SET key=<prefix> value=<bytes> prob=<0-1> [weights=<base64 json>] [table=name]` stores a
    candidate value for the given prefix. Context weights use the `ContextWeight` JSON schema defined
    in [`src/prediction_table.go`](src/prediction_table.go) (encode the JSON blob, then pass it as base64).
  - `PREDICT_QUERY key=<prefix> [keys=a,b,c] [ctx=<base64 json>] [windows=<base64 json>]
    [key_windows=<base64 json>] [merge=avg|sum|max] [table=name]` evaluates one or many prefixes and
    merges their probability windows. `ctx` may be a base64-encoded JSON array (`[[...], ...]`) or an
    object like `{"rows":[[...]],"weights":[...]}` where weights scale each row. `keys=` lets you
    query several prefixes at once, while `key_windows=` accepts a base64 array of
    `{ "key": "<hex>", "windows": [[...], ...] }` objects for per-prefix window overrides. Responses
    include the backend name (`cpu` or the simulated `webgpu-simulated` merger).
  - `PREDICT_TRAIN key=<prefix> target=<bytes> [ctx=<base64 json>] [lr=0.01] [table=name]
    [negatives=<value>,...]` adjusts stored weights via the forward/backward loop (optionally
    down-weighting bad predictions listed in `negatives=`). The table now persists normalized window
    hints from every training/adversarial context and blends them into queries automatically when no
    `windows=` payload is supplied. `PREDICT_CTX key=<prefix> ctx=<base64 json> [mode=bias|scale]
    [strength=1] [table=name]` applies an immediate context bias without retraining.
  - `PREDICT_INHERIT key=<prefix> target=<bytes> sources=<value>,... [merge=avg|sum|max] [table=name]`
    merges existing prediction values into a new target (for example, to seed composite/merged
    tokens with inherited context weights).
  - `PREDICT_INHERIT_BATCH items=<base64 json> [key=<prefix>] [merge=avg|sum|max] [table=name]`
    processes multiple inherit requests in one call. The JSON payload is an array of
    `{ "key": "…", "target": "…", "sources": ["…", ...], "merge": "avg" }` objects.
  - `PREDICT_INHERIT_ASYNC items=<base64 json> [key=<prefix>] [merge=avg|sum|max] [table=name]`
    queues a batch job and returns a `job` token for later polling.
  - `PREDICT_INHERIT_STATUS <job_id>` reports job progress (merged/skipped/failed counts).
  - `PREDICT_INHERIT_FETCH <job_id>` returns batch results once the job completes (or `PENDING` while running).
  - `PREDICT_BACKEND [mode=cpu|gpu] [table=name]` toggles the probability merger per table, and
    `PREDICT_BENCH samples=<n> window=<len> [table=name]` compares CPU vs accelerated merges on the
    current host.

  All prediction commands accept plaintext prefixes or the `x<hex>` form — and so does every
  *value* they take (`value=`, `target=`, `sources=`, `negatives=`, and the same fields inside an
  `items=` payload), so a bare hex string is stored literally rather than decoded. Context matrices
  and window specs must be base64-encoded JSON so CLI input stays newline-safe. Values are returned
  hex-encoded in `items=<hex>:<prob>`.

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
- **Edge** — `{ id, from, to, type, directed, weight, confidence, modality, ambiguity, props{},
  created_at, updated_at }`. An edge is uniquely identified by the tuple `(from, to, type, directed)`;
  setting the same tuple again **upserts** it. `weight` defaults to `1.0` and `directed` defaults to
  `1` (true).
- **Belief fields** — `confidence` (0–1), `modality` (a word from an ordered scale) and `ambiguity`
  (the group of mutually exclusive alternatives this edge belongs to) are optional and describe *how
  sure* the edge is, as opposed to `weight`, which is traversal strength. They are omitted from the
  record when never declared, and an edge that declares nothing counts as `certain`. See
  [Uncertainty and ambiguity](#uncertainty-and-ambiguity).
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
| `GRAPH_NODE_SET id=<id> [labels=a,b] [props=<json\|base64>] [references=<base64-json[]>]` | Upsert a node (preserves `created_at`; keeps existing labels/props/references when omitted). References are complete sentence objects `{id,text,source?,ordinal?}`; missing ids are SHA-256-derived, and `references=-` clears them. |
| `GRAPH_EDGE_SET from=<id> to=<id> [type=<t>] [weight=<f>] [directed=0\|1] [props=<json\|base64>] [autocreate=0\|1]` | Upsert one edge plus its adjacency/index entries. Missing endpoint nodes are auto-created unless `autocreate=0`. |
| `GRAPH_EDGE_SET_BATCH items=<base64 json[]> [continue_on_error=0\|1] [type=…] [directed=…] [weight=…] [props=…]` | Bulk upsert in one round-trip; top-level `type/directed/weight/props` act as per-item defaults. |
| `GRAPH_NODE_DEL id=<id> [cascade=1]` | Delete a node; `cascade=1` also removes its incident edges. |
| `GRAPH_EDGE_DEL from=<id> to=<id> [type=<t>] [directed=0\|1]` | Delete the edge addressed by the tuple. |
| `GRAPH_AMBIGUITY_SET from=<id> group=<g> options=<id>[=<share>][,…] [type=<t>] [normalize=0\|1]` | Write a whole set of mutually exclusive alternatives, shares normalized to sum 1. |
| `GRAPH_AMBIGUITY_RESOLVE from=<id> group=<g> winner=<id> [drop=0\|1]` | Collapse the set: winner becomes `certain`, the others `ruled_out` (or deleted). |

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
| `GRAPH_AMBIGUITY_GET from=<id> group=<g> [direction=out\|in] [limit=<n>]` | `count`, `confidence_sum`, `top`, `top_modality`, and `payload=` the alternatives, strongest first. |
| `GRAPH_RECALL seeds=<t>[,…] [precision=…] [hops=…] [min_sources=…] [references=0\|1] [reference_limit=…] […]` | `resolved`, `visited`, `expanded`, `references`, `count`, `bridges`, `truncated`, and `payload=` the resolved seeds plus the ranked associations. With `references=1`, each association may include complete stored sentences and episodic source payloads. See [associative recall](#associative-recall--graph_recall-graph_similar). |
| `GRAPH_SIMILAR id=<id> [by=context\|lexical\|all] [limit=<n>]` | `count`, `truncated`, and `payload=` `[{id,score,context,lexical,shared_count,shared,labels}]`. |
| `GRAPH_TERM_INDEX [action=stats\|rebuild\|drop] [limit=<n>] [cursor=<tok>]` | `entries`/`enabled` (stats), `nodes`+`terms`+`next_cursor` (rebuild), `removed` (drop). |

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
- **Predicates** read `from.id`, `to.id`, `from.label`, `to.label`, `edge.type`, `edge.weight`,
  `edge.confidence`, `edge.modality`, `edge.ambiguity`, and `edge.props.<key>` with operators
  `= != >= <= > <`. An `edge.props.<key> = <literal>` equality is served straight from the
  `graph/idx/` secondary index. `edge.confidence` accepts a number **or** a scale word
  (`>= possible` is `>= 0.5`), and `edge.modality` compares the word itself for `=`/`!=` and its rank
  on the scale for the ordering operators.
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

### Associative recall — `GRAPH_RECALL`, `GRAPH_SIMILAR`

`GRAPH_QUERY` answers a question you already know how to ask. `GRAPH_RECALL` answers the one you
don't: give it the terms a conversation is touching and it spreads activation from all of them at
once, returning everything they co-activate — ranked, with the evidence for each item — so the model
can choose what to open next.

```text
GRAPH_RECALL seeds=<term>[,<term>…]     # free text or node ids; base64:<list> when a term has spaces
  [precision=<0..1|word>]   # cut-off, default 0.25; accepts scale words (`probable` = 0.75)
  [hops=<n>]                # default 3, max 6
  [min_sources=<n>]         # 2 = only what several seeds reach: the convergence view
  [direction=out|in|both]   # default both — association is not directional
  [type=<t>[,…]]            # restrict the relations that carry activation
  [decay=<0..1>]            # activation kept per hop, default 0.55
  [expand=exact|lexical|synonyms|all]   # how seeds resolve, default all
  [synonym_types=<t>[,…]|-] # default synonym,alias,same_as,aka,abbreviation,acronym
  [references=0|1]           # hydrate complete sentence evidence, default 0
  [reference_limit=<n>]      # global sentence cap for this recall, default 32, max 256
  [limit=<n>] [branch_limit=<n>] [budget=<n>] [include_seeds=0|1] [seed_limit=<n>]
```

Scoring, in one line each:

- **Activation** leaves a seed at its resolution score (1.0 for an exact id) and is multiplied at each
  edge by `decay × weight × confidence` — so a `possible` edge carries half of what a plain one does,
  and `precision` is a belief threshold as much as a distance one. It gates seed resolution too: a
  free-text seed must overlap a node's words by at least `precision` to resolve to it.
- **Convergence** combines the seeds in noisy-OR (`1 − Π(1 − aᵢ)`): a node two seeds each reach at
  0.55 scores **0.7975**, above either of their own direct neighbours. `bridge=true` marks it.
- **`distance`** is conceptual depth: crossing a synonym edge costs a hop but no distance, because an
  alias is not a different subject. The per-source `hops` still counts the real steps.
- **`novelty` = score × distance/(distance+1) × sources/seeds** — high for a far node that several
  seeds reach, ~0 for the obvious neighbour of one seed. Sort by it to read the surprises first.

```text
[cheetah_data/default]> GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 limit=8
SUCCESS,command=GRAPH_RECALL,seeds=2,resolved=2,visited=8,expanded=6,hydrated=15,references=0,count=6,bridges=3,truncated=0,precision=0.100,payload=<base64>
# payload decodes to {"seeds":[…],"associations":[
#   {"id":"city:berlin","score":0.7975,"novelty":0.39875,"distance":1,"source_count":2,"bridge":true,
#    "sources":[{"seed":"cat:luna","activation":0.55,"hops":1},{"seed":"person:marco","activation":0.55,"hops":1}],
#    "via":[{"from":"cat:luna","type":"lives_in","to":"city:berlin","weight":1,"confidence":1,"modality":"certain"}]},
#   {"id":"country:germany","score":0.513494,"novelty":0.342329,"distance":2,"source_count":2,"bridge":true,…},
#   {"id":"breed:siamese","score":0.55,"novelty":0.1375,"distance":1,"source_count":1,…}, …]}

[cheetah_data/default]> GRAPH_RECALL seeds=cat:luna,person:marco hops=3 precision=0.05 min_sources=2
SUCCESS,command=GRAPH_RECALL,seeds=2,resolved=2,visited=8,expanded=13,hydrated=24,references=0,count=5,bridges=5,truncated=0,precision=0.050,payload=<base64>
# only what more than one seed reaches — the "what do these two have to do with each other?" question
```

References keep recall grounded in complete language instead of returning only token matches. Store
them directly on a node (base64 is mandatory because sentences contain spaces), or attach an
episodic payload key to an edge as `props.src`; recall can return both in one bounded payload:

```text
[cheetah_data/default]> GRAPH_NODE_SET id=module:parser labels=module references=<base64 of [{"id":"parser-contract","text":"The parser rejects non-finite values before applying configuration.","source":"design-contract","ordinal":1}]>
SUCCESS,node_set,id=module:parser
[cheetah_data/default]> GRAPH_RECALL seeds=task:validation hops=1 references=1 reference_limit=8
SUCCESS,command=GRAPH_RECALL,…,references=2,…,payload=<base64>
# the module:parser association may contain the stored design-contract sentence plus the verbatim
# episode named by the evidence edge's props.src. Reference text also feeds the derived term index,
# so free-text seeds can resolve through whole remembered sentences.
```

A seed does not have to be an id. Free text resolves through the lexical index (`berlin` →
`city:berlin`, scored by word overlap) and then through declared synonym edges, and every match says
which route it took:

```text
[cheetah_data/default]> GRAPH_RECALL seeds=berlin hops=1 precision=0.1
SUCCESS,command=GRAPH_RECALL,seeds=1,resolved=2,visited=5,expanded=2,hydrated=5,references=0,count=3,…,payload=<base64>
# "seeds":[{"term":"berlin","matches":[{"id":"city:berlin","score":0.495,"match":"lexical"},
#                                      {"id":"city:berlino","score":0.47025,"match":"synonym"}]}]
```

`GRAPH_SIMILAR` is the other half: not "what is connected to X" but "what else behaves like X",
scored on shared neighbours (`context`) and shared id words (`lexical`), with the shared contexts
listed as evidence.

```text
# cat:luna and cat:mia are both siamese and both live in Berlin; no edge joins them
[cheetah_data/default]> GRAPH_SIMILAR id=cat:luna limit=8
SUCCESS,command=GRAPH_SIMILAR,id=cat:luna,count=1,truncated=0,payload=<base64>
# payload decodes to: [{"id":"cat:mia","score":1,"context":1,"lexical":0.333333,
#                       "shared_count":2,"shared":["breed:siamese","city:berlin"]}]
# context=1: identical neighbourhoods. lexical=0.333: `cat` out of {cat,luna,mia}
```

Free-text seeds need the lexical index, which is maintained automatically on node writes. Databases
written before it existed (or with `CHEETAH_GRAPH_TERM_INDEX=0`) index nothing until a rebuild — it is
resumable, so a large graph is walked in bounded slices:

```text
[cheetah_data/default]> GRAPH_TERM_INDEX action=stats
SUCCESS,command=GRAPH_TERM_INDEX,action=stats,enabled=1,entries=12
[cheetah_data/default]> GRAPH_TERM_INDEX action=rebuild limit=4096
SUCCESS,command=GRAPH_TERM_INDEX,action=rebuild,nodes=6,terms=12,next_cursor=*
# next_cursor != * means there is more: pass it back as cursor=<token>
```

Both commands are bounded by `branch_limit` (neighbours read per node and direction) and `budget`
(edges hydrated in total). Hitting either ends the walk and reports `truncated=1` — a partial answer
that says so, never a stall. Exact ids and synonym edges keep working without the index; only
free-text matching depends on it.

Graph statistics can also stream through the reducer language: `PAIR_REDUCE degree|triangle|pagerank_seed <adj-hex-prefix>`
runs directly over the `adj/out`/`adj/in` namespaces without hydrating edges. For the authoritative
argument grammar of every command above, the source of truth is the `handleGraph*` dispatch in
[`src/graph.go`](src/graph.go) (routed from `ExecuteCommand` in [`src/database.go`](src/database.go)) — this section
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
| Hedge — "may be", "I think", "probably" | `confidence=possible` (a number or a word) | keeps the edge *type* stable, so one query shape finds certain and uncertain facts |
| Disjunction — "either A or B, I forget" | `GRAPH_AMBIGUITY_SET … group=<g> options=A,B` | see [uncertainty and ambiguity](#uncertainty-and-ambiguity); the engine has no `OR`, so alternatives are a group |
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
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=possible props={"src":"1"}
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

- **"may be sterile" is not a new edge type.** It is `has_condition` with `confidence=possible`. A
  hedge-specific type (`may_have_condition`) would force every later query to know the whole family of
  names; a declared confidence keeps one query shape and lets `WHERE edge.modality >= 'probable'`
  separate what is assertable from what is not.
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

### Uncertainty and ambiguity

Not everything you are told is certain, and not everything is unambiguous. The engine carries both
first-class on an edge, in **numbers and in words**:

| Field | Written as | Meaning |
| --- | --- | --- |
| `confidence=` | a number `0..1` **or** a scale word | how sure the fact is |
| `modality=` | a scale word | the same thing said in language |
| `ambiguity=` | a group id | this edge is one of several mutually exclusive readings |

`weight` stays what it was — traversal strength, and cost `1/weight` — so a belief no longer has to
borrow it. `props` still carry provenance (`src`, `source`, `as_of`).

The scale is ordered, and each word is an anchor on the 0–1 line:

| Word | Confidence | Accepted aliases |
| --- | --- | --- |
| `ruled_out` | 0.00 | `impossible`, `excluded`, `false`, `no` |
| `unlikely` | 0.25 | `improbable`, `doubtful`, `rare` |
| `possible` | 0.50 | `maybe`, `perhaps`, `uncertain`, `unverified` |
| `probable` | 0.75 | `likely`, `presumably`, `expected` |
| `certain` | 1.00 | `sure`, `asserted`, `definite`, `confirmed`, `verified`, `yes`, `true` |

Give either one and the server derives the other — a number is labelled with its nearest word, a word
is stored with its anchor value:

```text
[cheetah_data/default]> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition weight=1.0 confidence=possible props={"src":"1"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
[cheetah_data/default]> GRAPH_EDGE_GET from=cat:luna to=condition:sterile type=has_condition
SUCCESS,id=…,payload=<base64>
# decodes to: {"from":"cat:luna","to":"condition:sterile","type":"has_condition","weight":1,
#              "confidence":0.5,"modality":"possible","props":{"src":"1"},…}

[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=org:acme type=works_at confidence=0.8
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298d29ya3NfYXR8b3JnOmFjbWU
# stored as: "confidence":0.8,"modality":"probable"   ← 0.8 is nearest to the 0.75 anchor

[cheetah_data/default]> GRAPH_EDGE_SET from=person:marco to=person:elena type=reports_to modality=likely
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298cmVwb3J0c190b3xwZXJzb246ZWxlbmE
# stored as: "confidence":0.75,"modality":"probable"  ← the alias canonicalizes
```

Rules worth knowing:

- **Undeclared means certain.** An edge with no `confidence`/`modality` stores neither and reads as
  `certain` (1.0) in every predicate. Asserting without qualification is asserting.
- **A belief survives a partial upsert.** Unlike `weight` — which defaults back to `1.0` when
  omitted — `confidence`, `modality` and `ambiguity` are *preserved* when you do not mention them, so
  re-asserting an edge to change a prop cannot silently promote a hedge. Pass `confidence=-` to clear
  the belief on purpose.
- **A deliberate mismatch is allowed.** Give both and both are stored as given
  (`confidence=0.9 modality=possible`); give one and the other is derived.

### Ambiguity: enumerating the readings

> *"Marco likes either the color light blue or aquamarine, I don't remember."*

One command writes the whole alternative set, tags each edge with the group, and normalizes the
shares to sum to 1:

```text
[cheetah_data/default]> GRAPH_AMBIGUITY_SET from=person:marco type=likes group=fav_color options=color:light_blue,color:aquamarine
SUCCESS,ambiguity_set,group=fav_color,options=2,confidence_sum=1.0000

[cheetah_data/default]> GRAPH_AMBIGUITY_GET from=person:marco group=fav_color
SUCCESS,group=fav_color,count=2,confidence_sum=1.0000,top=color:aquamarine,top_modality=possible,payload=<base64>
# decodes to: [{"from":"person:marco","to":"color:aquamarine","type":"likes","confidence":0.5,"modality":"possible","ambiguity":"fav_color",…},
#              {"from":"person:marco","to":"color:light_blue","type":"likes","confidence":0.5,"modality":"possible","ambiguity":"fav_color",…}]
```

`options=` takes an optional share per alternative — a number or a scale word after `=`. Undeclared
alternatives are filled in two readings, chosen by what you did declare:

```text
# probability reading: declared shares are ≤ 1 and sum to ≤ 1, so the rest splits the leftover
[cheetah_data/default]> GRAPH_AMBIGUITY_SET from=person:sara type=lives_in group=where_sara options=city:lisbon=0.7,city:porto
SUCCESS,ambiguity_set,group=where_sara,options=2,confidence_sum=1.0000
[cheetah_data/default]> GRAPH_AMBIGUITY_GET from=person:sara group=where_sara
SUCCESS,group=where_sara,count=2,confidence_sum=1.0000,top=city:lisbon,top_modality=probable,payload=<base64>
# decodes to: [{"to":"city:lisbon","confidence":0.7,"modality":"probable",…},
#              {"to":"city:porto","confidence":0.3,"modality":"unlikely",…}]

# relative reading: a share above 1 (or a sum above 1) means "three to one"
#   options=city:lisbon=3,city:porto=1   → 0.75 / 0.25
```

Pass `normalize=0` to store the shares untouched (each must then be within 0–1).

Because the scale is ordered, the words are a filter — and so is the number, interchangeably:

```text
[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.modality >= 'probable' RETURN count
SUCCESS,return=count,matches=0,next_cursor=*
# → nothing here is worth asserting yet

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:*]->(*) WHERE edge.ambiguity = 'fav_color' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=2,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":1},
#              {"from":"person:marco","type":"likes","to":"color:light_blue","weight":1}]
```

When the answer arrives, resolve the group in one command — the winner becomes `certain`, the others
`ruled_out`, and the group dissolves:

```text
[cheetah_data/default]> GRAPH_AMBIGUITY_RESOLVE from=person:marco group=fav_color winner=color:aquamarine
SUCCESS,ambiguity_resolved,group=fav_color,winner=color:aquamarine,ruled_out=1,dropped=0

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.modality >= 'probable' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":1}]

[cheetah_data/default]> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.modality = 'ruled_out' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:light_blue","weight":1}]

[cheetah_data/default]> GRAPH_AMBIGUITY_GET from=person:marco group=fav_color
ERROR,ambiguity_group_not_found,group=fav_color
```

The same query that returned nothing while the memory was ambiguous now returns exactly one answer,
and the discarded reading is still on record as *excluded* rather than forgotten — so *"weren't you
saying light blue?"* remains answerable. Use `drop=1` to delete the losers instead of ruling them
out.

The rest of the vocabulary still lives in `props`, because it is provenance rather than belief:

| The sentence says… | Encoding |
| --- | --- |
| plain assertion — "Marco works at Acme" | nothing to declare; the edge is `certain` |
| hedge — "may be", "I think", "probably" | `confidence=possible` / `confidence=probable` |
| disjunction — "either A or B" | `GRAPH_AMBIGUITY_SET … group=<g> options=A,B` |
| hearsay — "Elena told me Marco hates red" | `confidence=probable props={"source":"elena"}` |
| staleness — "as of 2024", "back then" | `props={"as_of":2024}`, re-asserted with a new confidence when confirmed |
| negation — "Marco doesn't like red" | an explicit `dislikes` edge — never a missing edge, which is indistinguishable from ignorance |
| retraction — "actually, not that" | `confidence=ruled_out`, or `GRAPH_EDGE_DEL` to forget entirely |

Where the query language stops, and what to do about it:

- **`WHERE` is AND-only** — no `OR`, no `NOT`, no parentheses (see
  [`parseGraphWhereClause`](src/graph.go), which splits on `AND`). This is why a disjunction is stored as
  a *group* rather than expressed as a query: one `edge.ambiguity` equality returns the whole set.
  `!=` covers per-field negation, and anything richer belongs in the client.
- **Exclusivity is enforced at write time, not as an invariant.** `GRAPH_AMBIGUITY_SET` and
  `GRAPH_AMBIGUITY_RESOLVE` keep a group normalized, but a later plain `GRAPH_EDGE_SET` on one member
  can unbalance it — rewrite the group through `GRAPH_AMBIGUITY_SET` rather than editing members.
- **Groups are anchored.** `GRAPH_AMBIGUITY_GET`/`_RESOLVE` scan one node's adjacency, so a group
  spans the alternatives leaving (or entering) a single node. There is no global "all open groups"
  index; link them from a stable node if you need to enumerate them.
- **Confidence is stored rounded to six decimals** after normalization, so payloads stay readable.
- **No validity intervals.** `as_of`/`since` props are a convention; the engine does no temporal
  reasoning.
- **Absence never means false.** `ERROR,node_not_found` and `matches=0` mean *nothing is recorded* —
  a model must report that, not infer a negative.

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
| "What comes to mind around X, Y, Z?" (no known relation) | `GRAPH_RECALL seeds=X,Y,Z` | the associative path: free-text seeds allowed, every hit carries its evidence |
| "What do X and Y have to do with each other?" | `GRAPH_RECALL seeds=X,Y min_sources=2` | only what both reach; sort the result by `novelty` for the non-obvious ones |
| "What else is like X?" | `GRAPH_SIMILAR id=X` | same neighbours or same words — no edge between them required |
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
  5. confidence= is your certainty: omit it for a flat assertion, or pass a word
     (ruled_out|unlikely|possible|probable|certain) or a number 0..1 for anything hedged.
     Keep provenance in props: {"src":"<episode key>","source":"<who said it>"}.
  6. "either A or B": one GRAPH_AMBIGUITY_SET with every reading in options= (add =<share> when
     you lean one way). Never collapse a disjunction into one guess, never drop it for lack of
     certainty, and resolve it with GRAPH_AMBIGUITY_RESOLVE when the answer arrives.
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
- Custom reducers can be registered in Go via the reducer registry (see `src/reducers.go`). Each reducer
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
defined in [`src/types.go`](src/types.go) and the traversal logic lives in [`src/database.go`](src/database.go) and
[`src/pair_codec.go`](src/pair_codec.go):

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

# Cheetah Reference

Cheetah-specific directives and operational notes live here. Refer back to this file whenever you work with `cheetah-db`.
Read and collect potential implementation to do in NEXT_STEPS.md

- **Cheetah-db is now the authoritative database.** Every ingest run, decoder lookup, and evaluation
  must assume cheetah is the primary store for counts/probabilities/context metadata. SQLite survives
  only as a scratch/cache/export format (e.g., `--db var/tmp.sqlite3` for quick analysis or when
  emitting `.sqlite3` artifacts) and should never be treated as the long-term source of truth again.
  When in doubt: start/attach to the cheetah server first, keep `DBSLM_BACKEND=cheetah-db`, and only
  lean on SQLite when a workflow explicitly requires a transient file. The trainer now strictly exits
  if the cheetah TCP endpoint cannot be reached—there is no SQLite fallback path anymore.
- `README.md` now reinforces this requirement: it includes a concrete `.env` block (host/port/database
  knobs), documents the tmux helpers (`scripts/start_cheetah_server.sh`/`stop_cheetah_server.sh`),
  and explains when `python src/train.py ... --reset` or the emergency `--backonsqlite` flag should
  be used. Treat SQLite as a small/fast scratchpad only; all long-lived runs must keep
  `DBSLM_BACKEND=cheetah-db`.
- When `src/train.py --reset` talks to cheetah, it now shrinks `PAIR_SCAN` page sizes whenever a page
  stalls and raises the TCP idle-grace target to `max(DBSLM_CHEETAH_TIMEOUT_SECONDS * 180, 60)`
  seconds. This avoids the `cheetah response timed out after 30.0s of inactivity` spam even when the
  namespace is empty or the server sits on a slow disk; increase
  `DBSLM_CHEETAH_TIMEOUT_SECONDS` further if remote cheetah instances still need more time.
- When training/decoding from inside WSL but pointing at a cheetah server running on Windows, the
  hot-path adapter auto-retries the Windows bridge IP discovered via `/etc/resolv.conf` whenever the
  configured `DBSLM_CHEETAH_HOST` resolves to loopback. Override `DBSLM_CHEETAH_HOST` with the exact
  Windows/LAN address when cheetah lives elsewhere (container, remote host, etc.). If the server
  advertises `0.0.0.0:4455`, keep the client pointed at a *real* address (127.0.0.1, LAN IP, etc.);
  connecting to `0.0.0.0` is invalid, so the adapter now rewrites that case to loopback.
- `cheetah-db` now keeps a bounded payload cache inside `database.go`, keyed by
  `<value_size, table_id, entry_id>` so hot `READ`/`PAIR_REDUCE` loops remain in RAM instead of
  pounding the same `values_<size>_<tableID>.table` sectors. It defaults to 16k entries (~64 MB) and
  is tunable via `CHEETAH_PAYLOAD_CACHE_ENTRIES`, `CHEETAH_PAYLOAD_CACHE_MB`, or
  `CHEETAH_PAYLOAD_CACHE_BYTES` (set either to `0` to disable the cache when profiling raw disk I/O).
- Context relativism is now first-class: `AbsoluteVectorOrder` deterministically sorts nested token
  structures and mirrors them into the `ctxv:` namespace, `DBSLMEngine.context_relativism()` streams
  probabilistic projections directly from cheetah, and `Decoder` falls back to those slices whenever
  a Top-K entry is missing.
- `CheetahHotPathAdapter` mirrors raw follower counts (`PAIR_REDUCE counts`) and decoder metadata so
  MKNS rebuilds and session-cache profiles can run entirely over TCP. `NGramStore.topk_hit_ratio()`
  exposes coverage so you can watch cheetah eventually serve ≥90% of decoder requests.
- Probability/backoff slices (`prob:<order>`) and continuation metadata (`cont:`) are mirrored into
  cheetah alongside counts, and the Go reducers now return inline payloads for `counts`,
  `probabilities`, and `continuations`, eliminating the extra `READ` hop per entry.
- `src/train.py` now logs the active cheetah hot-path endpoint (host, port, namespace) at startup and
  reports the observed Top-K hit ratio after ingest completes so every run leaves an explicit trace
  that cheetah-db handled the work (runs still abort unless `--backonsqlite` is provided when the
  server is unreachable).
- cheetah-db now mirrors context metadata + Top-K slices directly during ingest. `DBSLM_BACKEND`
  defaults to `cheetah-db`, the decoder reports its hit ratio via
  `DBSLMEngine.cheetah_topk_ratio()`, and Level 1 lookups can iterate namespaces with
  `NGramStore.iter_hot_context_hashes()` or trigger probabilistic tree queries via
  `engine.context_relativism(...)`. The old `ColdStorageFlusher`/MariaDB path has been removed.
- MiniLM-driven context window embeddings live in cheetah metadata as well. The trainer flushes the
  serialized prototypes to `meta:context_dimension_embeddings`, and `run.py` workers read them back
  so context-dimension penalties + cosine weights stay consistent even when SQLite is bypassed.
  Inspect the payload via `PAIR_GET meta:context_dimension_embeddings` whenever you need to audit the
  learned windows or confirm that a `6,12,24` span absorbed fresh corpora.
- Future cheetah task: keep a small rolling log (`meta:context_dimension_embeddings:<date>`) or
  multi-centroid payload so we can store several prototypes per dimension without bloating SQLite.
  This would let Go-side tools visualize how each dimension drifts and give the decoder multiple
  reference anchors per window length.
- `PAIR_SCAN` traversal now fan-outs across the trie using a worker pool sized from
  `ResourceMonitor.RecommendedWorkers(...)`: per-branch tasks stage into a channel, workers hydrate
  pair-table pages concurrently, and results are deduplicated/ordered in a shared accumulator that
  respects cursors + `limit` cut-offs. This keeps trie pagination multi-core aware and prevents a
  single slow disk seek from blocking the entire scan.
- cheetah-db keeps cached file handles per pair-trie node (RW locked), parallelizes reducer payload
  hydration with a bounded worker pool, and treats child pointers + terminal keys as
  independent flags so prefix-sharing namespaces (`ctx:*`, `ctxv:*`, `topk:*`, etc.) finally
  coexist. `PAIR_SCAN`/`PAIR_REDUCE` accept optional cursors and emit `next_cursor=x...` when a page
  hits the configured limit, allowing clients to stream arbitrarily large namespaces without
  reopening readers. Run `CHEETAHDB_BENCH=1 go test -run TestCheetahDBBenchmark -count=1 -v` from
  `cheetah-db/` to reproduce the latest snapshots:

- **Cluster-ready fork scheduling is wired in.** Every trie fork (child pointer or jump node) is
  hashed into a deterministic `fork_id` mirrored under `cluster_topology.json`. Use the new commands
  to steer the distributed planner:
  - `CLUSTER_UPDATE` accepts inline specs (`replication=2 nodeA=10.0.0.1:4455/4 ...`) or
    `json=<base64>` payloads to register nodes, capacities, and replication factors.
  - `CLUSTER_STATUS` dumps the active topology + fork counters so you can verify placement before
    migrating shards.
  - `FORK_ASSIGN <prefix|*>` reveals which nodes own the shard for any prefix. `PAIR_SCAN`,
    `PAIR_SUMMARY`, and downstream reducers automatically observe forks so scheduler stats track
    live workloads and highlight hotspots pre-migration.
  Because the metadata persists next to the trie, resyncing a node only requires replaying WAL
  segments for the forks it owns rather than rehydrating the entire namespace.
  - `CLUSTER_MOVE prefix=<bytes>|fork=<id> node=<nodeID>` forces a fork onto a specific node. The
    gossip messenger (enabled via `CHEETAH_NODE_ID=<id>`) broadcasts the override so peers update
    their schedulers, and `CLUSTER_GOSSIP` handles remote RPCs from other nodes. Heartbeats flow over
    TCP to every peer declared in `CLUSTER_UPDATE`, so multi-node deployments can actively move hot
    branches without manual restarts.

## Matrix-related tree predictions

Cheetah's byte trees already partition sequences for fast lookup. The matrix-related tree reuses the
same structure but swaps the terminal payload for a prediction table that reacts to a mutable context
matrix.

### Fixed-byte prediction storage

- Every prediction table persists to `prediction_<name>.table` next to the trie. The file begins with
  the `CHPREDTB` magic + version tag, followed by a deterministic entry count so the server can memory
  map or stripe the file without parsing JSON. Each entry encodes the key length, last update stamp,
  value count, and context metadata using `uint32` lengths plus IEEE-754 floats, mirroring the
  fixed-byte guarantees of the regular value tables. Context vectors and window hints also use
  `[len][float64...]` blocks so a pager can jump directly to the next record.
- JSON now exists only for CLI/IPC payloads (`PREDICT_SET weights=`, `PREDICT_QUERY ctx=`, etc.). On
  disk the tables never store JSON, eliminating the previous bottleneck of rewriting megabytes of
  text for every update. Existing `.json` tables are auto-migrated the first time they are opened and
  rewritten into the binary format.
- Because each record is deterministic and endian-stable, the files can be split, mirrored, or copied
  with the same tooling used for `values_*.table` (e.g., `PAIR_SUMMARY` forklifts, fork transfers).
  Key/value pairs stay contiguous, which keeps hot prediction shards cache-friendly and compatible
  with the fixed-byte assumptions elsewhere in the engine.

### Prediction table contract

- Keys map to multiple candidate results (value, probability vector, weight blobs) rather than a
  single scalar payload, letting queries request the top-N series for identical byte prefixes.
- Queries can specify multiple windows (e.g., overlapping 3/5/7-byte spans) and merge the resulting
  probability vectors before ranking results.
- Probabilities respond to a context matrix defined as an array of arrays where each sub-array is a
  context vector of arbitrary length. Missing entries default to zero so sparse contexts are cheap.
- Training/ingest MUST prune edges whose normalized probability stays below the configured
  `discard_below` threshold to avoid gigabytes of low-value weights during early learning.

### Context matrix weighting

- Each context sub-array is treated as an angular vector whose bias/dot products alter result
  probabilities. Vectors apply in declaration order: parents never depend on the deeper optional
  arrays, but deeper arrays can fine-tune an already-biased probability when higher precision is
  needed.
- The training loop runs forward/backward passes. Forward: collect per-window probabilities, fold in
  active context vectors, truncate to the requested byte span, then merge. Backward: adjust the
  stored weights so correlated byte sequences + contexts keep consistent probabilities, attaching
  new context arrays on demand. Missing vector indices auto-initialize to zero, so the matrix can be
  infinitely sparse.
- Because results influence one another, store weights per result-value blob. Updating one value lets
  the recursion propagate to related entries when their correlation flags are set during training.

### Execution notes

- GPU/WebGPU acceleration: expose a prediction-table setting (e.g., `enable_accelerated_merges`)
  that routes merging and context-application kernels through WebGPU/Vulkan whenever the host
  supports it; CPU fallback remains the default path.
- Multi-window merging: truncate each probability vector to the requested byte count before merging
  so contexts with different strides remain comparable, then bias/normalize during the merge cycle.
- Distributed execution: align prediction tables with the cluster-ready fork scheduler so matrix
  queries run on the shard that hosts the bytes, avoiding expensive cross-node replays.

#### CLI workflow & acceleration toggles

- `PREDICT_SET key=<value> value=<result> prob=<0-1> [weights=<base64 json>]` persists a prediction
  row. Byte inputs accept plaintext or `x...` hex. Context weights follow the `ContextWeight` schema
  (encode JSON -> base64) so sparse depth vectors stay compact.
- `PREDICT_QUERY key=<value> [ctx=<base64 json>] [windows=<base64 json>]` evaluates a key with the
  provided context matrix and optional probability windows. Responses return ordered pairs as
  `<value_hex>:<prob>` plus the backend name. Supply `keys=a,b,c` for multi-window merges across
  several prefixes, `key_windows=<base64 json>` for per-key probability windows, `merge=avg|sum|max`
  to control aggregation, and `table=<name>` whenever multiple prediction tables coexist.
- `PREDICT_TRAIN key=<value> target=<result> [ctx=...] [lr=0.01]` runs the recursive update loop so
  contexts fine-tune weights without rewriting payloads.
- `PREDICT_BACKEND [cpu|gpu]` toggles between the CPU path and the simulated WebGPU merger
  (`CHEETAH_PREDICT_MERGER=gpu` sets the default). Acceleration fans out merges across CPU cores to
  mirror WebGPU behaviour until native bindings are available.
- `PREDICT_BENCH samples=<n> window=<len>` benchmarks CPU vs accelerated merges to decide when to
  enable GPU-style execution on a host.
- `PREDICT_CTX key=<value> ctx=<base64 json> [mode=bias|scale] [strength=1] [table=<name>]` applies a
  context-matrix adjustment to the stored probabilities without running a full training cycle,
  enabling online bias corrections during ingest/serving.

Context matrices + window specs are passed as base64-encoded JSON arrays so CLI whitespace stays
stable. The probability merger truncates vectors to the shared byte-span before aggregating and
automatically normalizes outputs.

### Indexing Defaults & Jump Nodes

- `pair_index_bytes` now defaults to `1` so each `PairTable` file tops out at 256 entries. Use
  `config.ini` (see `config.example.ini`) or append overrides to `DATABASE`/`RESET_DB`
  (`DATABASE ctx pair_bytes=2 payload_cache_entries=0`) whenever you truly need the wider
  stride—2-byte tables allocate 256 + 65,536 slots and should be reserved for extremely dense
  prefixes. Overrides persist per-database until reset.
- Unique suffixes automatically collapse into jump nodes. When an insert discovers that the remaining
  bytes on a branch only belong to that key, the entry stores the tail under `pair_jumps/` and points
  to that segment instead of allocating another 256-entry table. New keys split those jump nodes as
  soon as they overlap, and deletions re-check whether a child table is back to a single branch so it
  can be re-promoted into a jump. This keeps disk usage proportional to the number of active prefixes
  instead of the raw namespace depth.
- `PAIR_SCAN` and `PAIR_SUMMARY` now respect compressed segments automatically. When a prefix lands
  inside a jump node, the resolver extends the requested prefix with the forced bytes before queuing
  traversal work, so clients do not have to manage the compressed paths themselves.
  - `var/eval_logs/cheetah_db_benchmark_20251112-130623.log` — 24 workers / 30 s (~64 ops/s aggregate).
  - `var/eval_logs/cheetah_db_benchmark_20251112-164324.log` — 32 workers / 45 s (90→56 ops/s before the graceful drain, 1002 inserts, errors=0).
  - `var/eval_logs/cheetah_db_benchmark_20251112-164803.log` — 24 workers / 30 s rerun (96→67 ops/s, pair scans present in every bucket).
- The pair-table cache now enforces a descriptor cap derived from `RLIMIT_NOFILE` (override via
  `CHEETAH_MAX_PAIR_TABLES`). Idle handles are closed and transparently re-opened when the trie node
  is touched again, so long-running ingests stop tripping `open ... next_id.dat: too many open files`
  even on workstations with aggressive limits (macOS default 256). Set the env var to a lower value
  when running multiple cheetah instances on the same host or higher when you raise the OS limit.
- Pair-table reads/writes now route through a managed file layer that caches hot 4 KiB sectors and
  batches dirty pages through a shared flush queue backed by a worker pool sized to the detected CPU
  cores (override via `CHEETAH_FLUSH_WORKERS`). Writers update in-RAM copies while the limited set
  of background workers drains the queue, so thousands of pair tables no longer spawn their own
  goroutines or burn CPU after they go idle, yet dirty sectors still reach disk quickly enough to
  keep SSD churn low.
- The same managed file layer now tracks RAM pressure via the resource monitor and aggressively
  evicts/flushes idle sectors. By default, sectors that sit untouched for 30 s are flushed and
  removed, everything is forced out after 300 s, and when `MemAvailable/MemTotal` falls below
  `CHEETAH_CACHE_PRESSURE_HIGH` (~0.9) the cache trims low-scoring sectors (weighted toward writes)
  until usage returns to the `CHEETAH_CACHE_PRESSURE_LOW` band. Tune idle/force/sweep/stat windows
  plus read/write weights with the `CHEETAH_CACHE_*` env block and watch `SYSTEM_STATS` to verify
  memory is being released in real time instead of piling up until shutdown.
- The managed file layer now exposes a central checkpoint controller. Every `ManagedFile` registers
  with the `FileManager`, which can force-flush dirty sectors, optionally disable caching, and close
  handles based on idle time. Call `FILE_CHECKPOINT [IDLE=<duration>] [DROP_CACHE] [CLOSE_HANDLES]`
  via the TCP/CLI protocol whenever you need to drain pending writes mid-run (for crash safety or to
  shrink RAM use on cold namespaces). The engine automatically issues the equivalent of
  `FILE_CHECKPOINT DROP_CACHE CLOSE_HANDLES` during shutdown, eliminating the "preparing DB exit..."
  loops that appeared after heavy ingest sessions when thousands of pair tables still had dirty
  caches waiting to flush.
- cheetah-server now boots with a resource monitor: it detects logical cores, samples process vs
  system CPU percentages, and polls `/proc/self/io` for disk churn. Reducer worker pools call
  `RecommendedWorkers()` so hot `PAIR_REDUCE` bursts automatically back off when CPU or I/O pressure
  spikes, keeping multi-connection workloads responsive. Issue `SYSTEM_STATS` via CLI/TCP to read
  the latest snapshot (`logical_cores`, goroutines, CPU%, bytes/sec). The response now includes a
  `recommended_workers=` hint (queue-depth→worker-count pairs for 1, 32, 256, 4096 pending tasks)
  so Python and other clients can size reducer batches or pause when the server is saturated. The
  same response now adds `payload_cache_*` fields (entries, bytes, hits/misses, evictions, hit %,
  and an advisory bypass threshold) so adapters can auto-tune `CHEETAH_PAYLOAD_CACHE_*` values and
  skip caching multi-megabyte payloads. Python's hot-path adapter caches the stats for ~30s,
  derives a reducer page hint (256–2048 entries depending on CPU pressure), and `helpers/cheetah_cli`
  prints the suggested limit as `reducer_page_hint` for shell scripts.
- `PAIR_SUMMARY <prefix> [depth] [branch_limit]` is available for namespace analytics. It walks the
  trie beneath `prefix`, counts terminal entries, sums payload bytes via `main_keys` metadata, keeps
  min/max payload sizes and keys, and returns branch-level fan-out counts up to `depth` (default 1,
  unlimited with `-1`). Use it to emulate the `char_tree_similarity.py` heuristics directly inside
  the database: pick hot prefixes, stage rolling hashes, and decide which namespaces deserve GPU- or
  cache-backed reducers without iterating every payload in Python.
- `src/train.py` and `run.py` expose `--context-dimensions`, a comma-separated list of span ranges
  (e.g., `1-2,3-5`) or progressive lengths (e.g., `4,8,4`). Length specs auto-expand to contiguous
  spans starting at 1, and logs now append `(len=...)` so you can see the effective window widths.
  Selections live in `tbl_metadata` (and the cheetah metadata mirror) so repeat penalty tracking
  persists between runs and `Decoder` can down-weight word/sentence-length sequences that still leak
  through.
- MariaDB migrations are gone. Reset SQLite tables in place (or swap DB paths) and let cheetah's
  namespaces carry the hot/archive copies—no second store to reconcile or SQL bundle to ship.
- `--reset` only unlinks the SQLite file resolved via `--db`/`DBSLM_SQLITE_PATH` (defaults to
  `var/db_slm.sqlite3`). It never renames or truncates the cheetah namespace. Pick a distinct
  `DBSLM_CHEETAH_DATABASE` per run (or run `cheetah-db` cleanup commands) when you need isolated
  hot-path data instead of relying on `--reset`.
- `PAIR_PURGE <prefix> [page_size]` now handles namespace resets in one server-side command. The
  trainer’s `--reset` flag issues `PAIR_PURGE` for the hot namespaces (ctx/ctxv/topk/cnt/prob/cont/meta)
  so caches are cleared in seconds. If a deploy is still running an older cheetah binary, the
  trainer automatically falls back to the legacy `PAIR_SCAN` + `DELETE` loop.
- `RESET_DB [name]` removes every file under `cheetah_data/<name>` and reopens the database in-place.
  `src/train.py --reset` now tries `RESET_DB <DBSLM_CHEETAH_DATABASE>` first so whole-database nukes
  finish instantly; when the command is missing (older servers) it logs a downgrade note and returns
  to the namespace purge logic described above.
- `Makefile` now shells into `scripts/smoke_train.py`, which can iterate arbitrary scenario matrices,
  stream live metrics, and hand each scenario a dedicated SQLite + `DBSLM_CHEETAH_DATABASE`
  namespace so cheetah sessions can be paused and restarted independently.

### cheetah-db caching & SSD-wear guideline

- Launch `cheetah-server` with an explicit payload-cache budget whenever you expect repeated
  namespace hits (ingest, MKNS rebuilds, held-out decoder runs). The defaults
  (`CHEETAH_PAYLOAD_CACHE_ENTRIES=16384`, `CHEETAH_PAYLOAD_CACHE_MB=64`) cover average corpora, but
  bump the byte budget to 128–256 MB on larger hosts to eliminate the last SSD reads against the
  `values_*` tables.
- When profiling raw disk I/O or working on RAM-starved systems, disable the cache by exporting
  `CHEETAH_PAYLOAD_CACHE_ENTRIES=0` (or the MB/bytes variants). Re-enable it immediately afterward so
  normal workloads keep leveraging RAM instead of falling back to repeated SSD seeks.

- Per-scenario environment overrides (`DBSLM_SQLITE_PATH`, `DBSLM_CHEETAH_DATABASE`) keep SQLite and
  cheetah namespaces isolated, allowing the smoke train to stop one DB session and spin up another
  without restarting the Go service. Override the cheetah namespace manually by setting
  `DBSLM_CHEETAH_DATABASE` before launching any CLI if you need ad-hoc names outside the matrix.
- New helpers:
  - `scripts/start_cheetah_server.sh` / `scripts/stop_cheetah_server.sh` wrap the tmux fallback for
    launching/stopping the Go server when `screen` cannot stay attached inside WSL.
  - `scripts/run_cheetah_smoke.sh` enforces the cheetah-only smoke flags (tmp SQLite DB, metrics
    export, timeout), and `scripts/start_cheetah_smoke_session.sh` runs it inside a tmux session so
    progress can be tailed independently of PowerShell.
  - Always monitor the emitted log; kill the `cheetah_smoke` session if the tail stops advancing.
    Current failure to triage: `var/eval_logs/cheetah_smoke_train_20251112-190626.log` sticks on
    `datasets/emotion_data.json#chunk1` even though the server shows no errors.
### cheetah-only Archive

- Start `cheetah-db/cheetah-server` before running the Python CLI. `DBSLM_BACKEND` defaults to
  `cheetah-db`, so Level 1 lookups hit the Go service automatically.
- The Python bridge keeps reducer sockets alive across long-running queries: `CheetahClient` now
  tolerates up to ~30 seconds of inactivity while waiting for `PAIR_REDUCE` responses, so heavy
  count/probability pages no longer trip the 1-second TCP timeout. Increase
  `DBSLM_CHEETAH_TIMEOUT_SECONDS` only if you truly need longer windows.
- The trainer now refuses to silently fall back to SQLite when `DBSLM_BACKEND=cheetah-db`. If the
  Go server is down or you forgot to launch `cheetah-db/cheetah-server`, `src/train.py` exits with
  an error unless you explicitly pass `--backonsqlite` (intended only for emergency smoke reruns).
  Keep the compiled server running in parallel with every ingest/smoke session to avoid wasting
  runs on the wrong backend.
- Export `CHEETAH_HEADLESS=1` when launching the server inside WSL or a Windows terminal to disable
  the interactive CLI and leave the TCP loop running in the background. Typical pattern:
  `wsl.exe -d Ubuntu-24.04 -- screen -dmS cheetahdb bash -c 'cd /mnt/c/.../cheetah-db && env CHEETAH_HEADLESS=1 ./cheetah-server-linux'`.
  Always `screen -ls`/`screen -wipe` (or `pkill -f cheetah-server`) before rebuilding so the binary
  can be replaced cleanly.
- Pair trie inserts now allow prefix-sharing keys and chunked reducers/paginators are live, so the
  cheetah-only smoke ingest backlog is unblocked. Every `PAIR_SCAN`/`PAIR_REDUCE` response carries
  `next_cursor=x...` when additional pages exist, and the Python adapter follows those cursors
  automatically (`scan_namespace`, `iter_counts`, `iter_probabilities`, `iter_continuations`), so
  you can iterate huge namespaces without custom pagination loops.
- There is no SQL migration step or MariaDB destination anymore. Reset SQLite in place when you
  need a clean rebuild; cheetah already mirrors every context/top-K slice as part of the ingest loop.
- Watch `DBSLMEngine.cheetah_topk_ratio()` (or the training log line) to confirm cache coverage stays
  ≥90% so Top-K reads rarely fall back to SQLite.
- `sqlite` is **strictly a convenience/export format** now. Use it for short-lived corpus slicing,
  ad-hoc diffs, or when emitting `.sqlite3` bundles for downstream tools, but do not ship features or
  workflows that depend on SQLite-specific behavior. If you need a clean state, blow away the SQLite
  file with `--reset` and/or pick a new cheetah namespace—never attempt to keep long-running state in
  SQLite.
- `cheetah-db` (see `cheetah-db/`) is the real database. Always ensure `cheetah-db/cheetah-server`
  is running, set `DBSLM_BACKEND=cheetah-db` (or at least `DBSLM_CHEETAH_MIRROR=1` during local
  smoke tests), and double-check every new command or script prints the cheetah namespace it targets.
  The default train command in this repo assumes cheetah is healthy; if cheetah is down you must
  either fix it or pass `--backonsqlite` with an explicit rationale recorded in `NEXT_STEPS.md`.
  As of this pass:
  - the trie exposes `PAIR_SCAN` plus `PAIR_REDUCE counts`, so MKNS rebuilds and cache coverage
    metrics stream directly from Go without materializing temporary tables in SQLite;
  - the absolute vector ordering codec (`ctxv:` namespace) allows byte-identical context relativism,
    enabling nested queries + decoder fallbacks via `engine.context_relativism()`; and
  - metadata (context dimensions, decode presets, etc.) now lives in cheetah namespaces so new
    processes can cold-start with zero SQLite reads beyond the base schema.
  Keep `NEXT_STEPS.md` updated with those gaps and record interoperability details in
  `cheetah-db/README.md` for future agents, since the roadmap now aims to delete the remaining
  SQLite-only code paths once the reducers land.

## Next Steps

- Persist fork overrides + gossip snapshots so scheduler reassignments survive restarts and peers can
  catch up after downtime.
- Extend the messenger to stream actual shard payload diffs (values + prediction tables) so moving a
  fork also migrates its data, not just metadata.
- Integrate prediction-table updates into ingest/train loops (automatic `PREDICT_SET`/`PREDICT_CTX`
  invocations) so context-aware tables stay current without manual CLI batches.

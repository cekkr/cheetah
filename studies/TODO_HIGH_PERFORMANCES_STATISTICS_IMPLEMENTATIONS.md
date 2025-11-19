# TODO: High-performance Statistics Implementations

## Scope
- Evaluate the cheetah-db features listed in `AI_REFERENCE.md` with a focus on high-throughput statistical workloads **and** the data products that live inside each namespace (counts, rolling hashes, continuation totals, etc.).
- Mirror the README guidance around tree indexing so data-centric statistics (e.g., `char_tree_similarity.py` style substring filters) can be reused by other runtimes.
- Describe how the Go implementation plus the Python `src/db_slm` adapters should evolve (new commands, optional arguments) so typical statistical queries become one-shot server calls instead of bespoke loops.
- Capture TODOs that keep the statistical path fast (counts, probabilities, continuations, Top-K slices, context relativism) when reused beyond this repo.

## Priority statistical workloads
- **Counts & continuations** — `src/db_slm/adapters/cheetah.py` already calls `pair_reduce counts|continuations` via `iter_counts`/`iter_continuations`; research needs bulk namespace totals (per-order sums, histogram bins) without walking every payload in Python.
- **Probability tables & Top-K slices** — `publish_probabilities`, `fetch_topk`, and decoder biasing rely on dense `prob:*` payloads; future calculations include log-prob deltas, entropy, and quantile summaries per context.
- **Context relativism** — `pipeline.context_relativism` composes tree-shaped queries (mirroring `AbsoluteVectorOrder`) and needs branch-local statistics to prune improbable nodes early.
- **Similarity and rolling hashes** — `src/helpers/char_tree_similarity.py` keeps only significant substrings; the database equivalent is per-branch rolling hashes, substring counts, and bloom-like sketches per namespace to accelerate fuzzy matches and GPU reducers.
- **Operational analytics** — ingestion resets, MKNS rebuilds, and cache-hit tracking in `train.py` want single-command reports (counts per namespace, unique followers, continuation coverage) tailored for automation.

## Feature Review & Reuse Guidance

### 1. Payload locality, cache, and inline reducers
- `cheetah-db/cache.go` implements a size-aware LRU for `<value_size, table_id, entry_id>` tuples; `database.go:889-932` plugs it into `readValuePayload`. By copying payloads on Get/Add it remains thread-safe for arbitrary callers. Other programs can reuse the pattern by mirroring the 5-byte `ValueLocationIndex` and caching immediately after `READ`/`PAIR_REDUCE` without worrying about a shared backing slice.
- Pair reducers (`database.go:735-873`) now return payload bytes inline in the TCP response (base64 encoded once). Python’s `CheetahClient.pair_reduce` consumes this directly (`src/db_slm/adapters/cheetah.py:213-414`). This eliminates one `READ` call per entry, which is critical for high-volume statistical scans (full-order `prob:*` namespaces).
- `SYSTEM_STATS` now emits `payload_cache_*` metrics (entries, bytes, hits/misses, evictions, hit % plus an advisory bypass threshold) so Python/other runtimes can auto-tune `CHEETAH_PAYLOAD_CACHE_*` or skip caching multi-megabyte payloads when the cache would churn.

### 2. Parallel trie traversal bounded by live resource telemetry
- `ResourceMonitor` (`cheetah-db/resource_monitor.go`) samples CPU, goroutine count, and /proc IO once per interval and serves the last snapshot. `Database.PairScan` (`database.go:538-666`) asks `RecommendedWorkers(...)` before spawning traversal goroutines. The work-queue pattern (`parallelCollectPairEntries`) feeds entire subtries to workers while respecting cursor ordering.
- For other programs: drive large statistical scans (e.g., histogram exports) through `PAIR_SCAN` rather than ad-hoc tree walkers; let the server manage fan-out so clients stay single-threaded.
- TODO: export `RecommendedWorkers` as part of the `SYSTEM_STATS` payload so Python can request batched reducers sized to the current worker pool instead of hard-coding `DEFAULT_REDUCE_PAGE_SIZE`.

### 3. Statistical reducers for counts/probabilities/continuations
- The reducer pipeline mirrors `cnt:`, `prob:`, and `cont:` namespaces and walks them via the same pair trie (`AI_REFERENCE.md`, `database.go:705-873`). Each entry carries a base64 payload housing the serialized statistics so consumers can reconstruct totals without extra lookups.
- The Python adapter wraps these into `iter_counts`, `iter_probabilities`, and `iter_continuations` (`src/db_slm/adapters/cheetah.py:960-1105`), producing structured `Raw*Projection` objects. Any other program can follow the same namespace convention: `<kind>:<order_or_tag>:<context_hash>`.
- TODO: add streaming/iterator APIs on the Go side that return decoded structs (e.g., JSON) when `CHEETAH_JSON_STATS=1` for languages that prefer not to embed the serializer used by Python.

### 4. Context relativism and canonical vector ordering
- Context relativism hinges on `AbsoluteVectorOrder` (`src/db_slm/cheetah_vectors.py`) and the `ctxv:` namespace described in the AI reference. `database.go` stores canonical byte prefixes whenever ingest mirrors nested evidence; Python binds this via `pipeline.context_relativism` (`src/db_slm/pipeline.py:205-245`).
- Other runtimes can reuse `AbsoluteVectorOrder` (porting the simple deterministic encoding) to build byte prefixes matching what the Go server expects, then execute `PAIR_SCAN ctxv:<vector>` or `engine.context_relativism([...])` to fetch probabilistic projections.
- TODO: provide a small Go or C helper library that emits the same canonical ordering so non-Python clients do not need to reimplement the encoder.

### 5. Hot-path adapter and Top-K cache coverage
- `CheetahHotPathAdapter` (`src/db_slm/adapters/cheetah.py`) publishes contexts, Top-K slices, raw counts, and probability tables to the database and can iterate namespaces back into Python. It also tracks a Top-K hit ratio via the TCP metadata reported in `train.py` logs. This is the key bridge that turns cheetah-db into a general statistical backend rather than a project-specific cache.
- Other programs can implement the `HotPathAdapter` protocol (`src/db_slm/adapters/base.py`) to share the same semantics: mirroring inputs via `publish_*` and consuming them via `iter_*` plus `context_relativism`. The TCP protocol is purely textual, so any language with sockets can participate.
- TODO: expose a lightweight gRPC or binary framing alongside the newline protocol for workloads that need tens of thousands of reducer responses per second without the base64 overhead.

### 6. Monitoring and operational hooks
- `SYSTEM_STATS` (see `database.go:934-1012`) surfaces CPU, memory, goroutine, and IO rate metrics derived from `ResourceMonitor`. These are already suitable for spotting saturation before statistical calculations starve.
- Python can already hit this command through `CheetahClient.command`, but there is no helper; adding one would let analytics scripts dynamically slow down when the Go server is over-subscribed.
- TODO: wire a `get_system_stats()` helper into the Python adapter plus a CLI example so future programs can throttle long-running statistical exports based on real server pressure.

- **Tree-indexed namespace statistics**
  - README outlines the next frontier: leverage the deterministic trie to precompute namespace statistics (counts, rolling hashes, Top-K digests) the same way `char_tree_similarity.py` filters substrings. Because each branch has a stable offset, reducers or GPU walkers can `ReadAt` exact payloads without scanning.
  - TODOs below formalize the work: new commands that return branch-local summaries, optional reducer arguments that stream digests instead of full payloads, and helpers that emit rolling hashes for fuzzy namespace lookups. These should keep parity with the python helper semantics so statistical code can swap between in-memory and cheetah-backed char trees.

## Command & argument roadmap for statistical acceleration
- **`PAIR_SUMMARY <namespace> [options]` (new)** — emits aggregate statistics for a prefix: total contexts, follower sum, min/max counts, entropy estimates, rolling hashes. Options could include `--levels=2` (expand only `n` levels) or `--digest=topk|hashes`.
- **`PAIR_REDUCE ... mode` extensions** — add optional flags such as `payload=stats` to request pre-aggregated digests, `topk_limit=N` to inline only leading entries, or `hash=rolling` to include the trie-level hash without touching the payload.
- **`PAIR_SCAN ... with_counts` (optional argument)** — permit scanning namespaces while also returning child counts or payload sizes, enabling clients to build heat maps without separate reducer passes.
- **Metadata mirroring** — extend the existing metadata namespace with entries like `stats:ctx:<hash>` to store last-known summaries that can be reused by Python when running offline analyses.
- **Python adapter hooks** — add a `get_system_stats()` helper (consuming `recommended_workers`) plus dedicated `summary(namespace, depth)` and `iter_hashes(order)` methods so src/ code paths stop reimplementing reducers for simple statistics.

## Cross-program Usage Checklist (Python and beyond)
- Keep `DBSLM_BACKEND=cheetah-db` and launch `cheetah-server` with `CHEETAH_HEADLESS=1` so the TCP service is always reachable before running high-throughput analytics.
- Prefer namespace-aware reducers (`pair_reduce counts|probabilities|continuations`, `context_relativism`) instead of manual `READ`s; these paths already batch, parallelize, and cache payloads on the server.
- Respect pagination by replaying `next_cursor` in both Go (`PairScanAccumulator`) and Python (`CheetahClient.pair_scan/pair_reduce`); this keeps statistical exports chunked and prevents unbounded RAM growth.
- Monitor the Top-K hit ratio (`Pipeline.cheetah_topk_ratio`) to confirm that mirrored statistics are actually being served from cheetah rather than falling back to SQLite helpers.
- When extending the Python `HotPathAdapter`, reuse the provided serializers so statistical payloads remain byte-compatible with the Go cache (important for other consumers looking at the same namespace files).

## Open Tasks / Next Steps
1. **Expose resource-aware hints to clients.** ✅ `SYSTEM_STATS` emits queue-depth→worker-count hints derived from the resource monitor, and the Python hot-path adapter caches those stats to auto-tune reducer batch sizes (roughly 256–2048 entries per page based on live CPU pressure). CLI helpers now print the derived limit (`reducer_page_hint`) so shell scripts can throttle long-running exports without bespoke socket plumbing.
2. **Namespace summary commands.** ✅ `PAIR_SUMMARY <prefix> [depth] [branch_limit]` now streams aggregate counts, payload byte totals, and branch fan-out digests without hydrating every payload. Follow-up: add optional rolling hashes / Top-K digests alongside the structural stats.
3. **Optional reducer digests.** Allow `PAIR_REDUCE` to return compact statistics (entropy, CDF bins, hash windows) alongside or instead of raw payloads to accelerate Python-side analytics.
4. **Rolling hash / similarity mirrors.** Provide trie-level rolling hash metadata (per namespace and per depth) so fuzzy lookups and substring similarity heuristics have fast, server-side entry points.
5. **Python ergonomics.** ✅ Python CLI tools (`train.py`, `run.py`) now expose `--cheetah-system-stats` and `--cheetah-summary` hooks backed by adapter helpers, so analytics jobs can consume the new commands without bespoke socket calls.
6. **Shared canonical vector helpers.** Publish a tiny library (Go or Python wheel) containing `AbsoluteVectorOrder` encoders/decoders for reuse in other projects or languages.
7. **Binary/stateless protocol option.** Evaluate a Protobuf or msgpack framing to reduce base64 overhead for high-volume `PAIR_REDUCE` consumers (statistical crawlers, background jobs).
8. **JSON/stat-typed responses.** Offer an opt-in JSON encoder for reducers so programs that do not embed the DB-SLM serializer can still decode counts/probabilities/continuations cheaply.
- Consider extending the new summaries with rolling hash / Top-K digests; automated reducer batch sizing now rides on the SYSTEM_STATS worker hints inside the Python adapter + CLI helpers.

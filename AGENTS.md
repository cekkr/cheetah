# cheetah-db — AI Agent Reference

`cheetah-db` is a standalone, single-binary **Go** key/value + graph + prediction database server
(`package main` in [`src/`](src/), import path `cheetahdb/src`, module `cheetahdb`, Go 1.24). It stores
byte-encoded payloads partitioned by value
size, indexes namespace prefixes through an on-disk **pair trie**, and speaks a newline-delimited text
protocol over TCP and an interactive CLI. It was built to stage dense statistical datasets (n-gram
counts, probabilities, continuation metadata, context vectors) and later grew graph storage, matrix
prediction tables, and cluster-fork scheduling.

**Repository layout:** [`src/`](src/) holds the whole server (`package main`, all `*.go` and `*_test.go`
of the engine); [`gold/`](gold/) and [`demo/graph-nell/`](demo/graph-nell/) are separate `package main`
build targets; everything else at the root is docs, config, and the build script. Build with
`go build -o cheetah-server ./src`, test with `go test ./src`.

This file is the fast-access operational map for agents working in this repository. Read it before
editing, and update it in the same change that alters a documented fact (see
[Update triggers](#update-triggers)).

**What this repository is NOT:**

- It is **not** the Python "DB-SLM"/"lmdb" project that consumes this server. This repo contains **no
  Python**, no `train.py`/`run.py`, no SQLite, and no `DBSLM_*` behavior. The `src/` directory here is
  **Go server code**; it is *not* that project's `src/` package tree, so `src/train.py`,
  `src/db_slm/…` and `src/helpers/…` paths in prose docs are a vestige of the parent monorepo this was
  extracted from — treat those names as an *external client*, not code here. See
  [Vestigial parent-monorepo references](#pitfall-vestigial-references).
- It is **not** LMDB. The name collision is incidental; there is no `mmap` B-tree here.
- The [`gold/`](gold/) tree is a teaching prototype, not the shipping engine. The
  [`demo/graph-nell/`](demo/graph-nell/) tree is a benchmark **client**, not the server.

---

## Read order and sources of truth

Authority order for this repository (highest first). When sources disagree, inspect the code, fix the
mismatch within task scope, and update stale docs in the same change.

1. [`LICENSE`](LICENSE) — MIT license terms.
2. **Go source + tests** ([`src/`](src/)`/*.go`) — the executable contract and the ground
   truth for behavior. Tests in [`src/graph_test.go`](src/graph_test.go) and
   [`src/benchmark_test.go`](src/benchmark_test.go) assert the pieces they cover.
3. [`config.example.ini`](config.example.ini) — the authoritative shape of `config.ini` settings.
4. [`README.md`](README.md) — operator/user guide. Its
   [Command Reference](README.md#command-reference) now covers every command in the
   `ExecuteCommand` switch plus the three front-end ones, with the meaning of each and the
   distinctions between the look-alikes; still verify against
   [`ExecuteCommand`](src/database.go) before trusting a syntax detail.
5. [`CONCEPTS.md`](CONCEPTS.md) — original design intent and the context-relativism / reducer payload
   contracts. Written from the parent project's perspective; the `ctx:`/`ctxv:`/`cnt:`/`prob:` layouts
   it describes are *client conventions*, not enforced by this server (the server treats all prefixes
   as opaque bytes).
6. [`studies/author_notes.md`](studies/author_notes.md),
   [`studies/TODO_HIGH_PERFORMANCES_STATISTICS_IMPLEMENTATIONS.md`](studies/TODO_HIGH_PERFORMANCES_STATISTICS_IMPLEMENTATIONS.md)
   — design essays / research backlog. Aspirational; many TODOs are already done or belong to the
   Python client. [`studies/GRAPH_LLM.md`](studies/GRAPH_LLM.md) is the exception in that directory:
   it documents the LLM↔Cheetah learn/recall loop (memory tiers, teach/recall paths, `PREDICT_*`
   routing, adapter contract) from transcripts captured against this revision, so treat it as
   verified guidance rather than intent.
7. [`NEXT_STEPS.md`](NEXT_STEPS.md) — the roadmap, with a "Done" section that records what shipped
   and how it was verified. Treat the open items as intent and verify against code.

This file was previously named `AI_REFERENCE.md` (renamed in `168f67e`). External notes and the
parent monorepo may still refer to it under that name, or as `cheetah-db/AI_REFERENCE.md` — the
handbook here is `AGENTS.md`, and [`README.md`](README.md) links it as such.

---

## Collaboration and maintenance rules

- **Keep this file true to the checked-out revision.** Any edit that changes a command, on-disk
  format, config key, env var, file ownership, or feature status MUST update the matching section here
  in the same change. See [Update triggers](#update-triggers).
- **Record new roadmap work in [`NEXT_STEPS.md`](NEXT_STEPS.md)**, not as prose in this handbook.
- **Never commit runtime data.** [`.gitignore`](.gitignore) excludes the `cheetah-server` binary and
  `cheetah_data/*`. Do not add generated databases, `.table`/`.bin` files, or benchmark logs.
- **Format before committing.** The tree is currently `gofmt`-clean (`gofmt -l .` prints nothing).
  Run `gofmt -w` on files you touch and keep it clean; do not mass-reformat unrelated files in a
  behavior change.
- **Preserve unrelated working-tree changes.** Never use destructive git cleanup to simplify a task.
- **Server code lives in [`src/`](src/).** New engine `.go` files go there, not at the repository root;
  the root holds only docs, `config.example.ini`, `build.sh`, and the `gold/`/`demo/` build targets.
- **Tests must pass.** Run `go build ./...`, `go vet ./...`, and `go test ./src` before committing. The
  benchmark test is gated behind `CHEETAHDB_BENCH=1` and does not run by default.
- **Comments in this codebase are frequently Italian.** Match the surrounding language and density
  when editing; do not machine-translate existing comments as a side effect.

---

## Essential project principles

### Fixed-byte, offset-addressable storage

Every payload is located by arithmetic, never by scanning. A value's home is a
`<value_size, table_id, entry_id>` triple encoded as a 5-byte [`ValueLocationIndex`](src/types.go);
`READ` becomes a single `ReadAt`. **Consequence:** any change to the byte widths in
[`types.go`](src/types.go) (`ValueLocationIndexSize`, `MainKeysEntrySize`, `PairEntrySize`, …) silently
corrupts every existing `cheetah_data/` directory. Such a change requires a format-version bump and a
migration path, not an in-place edit.

### Namespaces are opaque byte paths through a trie

The server assigns **no meaning** to prefixes like `ctx:`, `prob:2`, or `graph/idx/`. It walks the raw
bytes of a key one branch-span at a time ([`pair_codec.go`](src/pair_codec.go)) and stores child pointers,
terminal keys, and jump segments in fixed-size [`PairTable`](src/tables.go) nodes. Semantic conventions
(`ctx:`/`ctxv:`/`cnt:` layouts in [`CONCEPTS.md`](CONCEPTS.md)) live entirely in clients. **Do not add
namespace-specific parsing to the trie core.**

### Prune aggressively; degrade, don't stall

Reducer and summary work is bounded by live CPU/IO telemetry
([`ResourceMonitor.RecommendedWorkers`](src/resource_monitor.go)), while a scan bounds itself by page
(see [`collectPairEntries`](src/database.go)); the payload cache
([`cache.go`](src/cache.go)) and managed-file sector cache ([`file_manager.go`](src/file_manager.go)) shed
memory under pressure; prediction training discards low-magnitude context weights
([`pruneEntryContextWeights`](src/prediction_table.go)). New heavy paths MUST cooperate with these back-off
mechanisms rather than spawning unbounded goroutines or holding every payload in RAM.

### One text protocol, two front-ends

CLI ([`main.go`](src/main.go)) and TCP ([`server.go`](src/server.go)) are thin loops that both delegate to the
same [`Database.ExecuteCommand`](src/database.go). Protocol semantics belong in `ExecuteCommand` (or the
handlers it calls), never in a front-end, so CLI and TCP never drift — **with the sole exception** of
the connection-scoped `DATABASE` / `RESET_DB` / `EXIT` commands, which are handled in the front-ends
because they mutate per-connection "current database" state.

---

## Critical implementation contracts

- **On-disk constants are a wire format.** [`types.go`](src/types.go) fixes `PairEntrySize = 11`
  (1 flag byte + 6 key bytes + 4 child bytes), `ValueLocationIndexSize = 5`,
  `EntriesPerValueTable = 65536`. Changing any of them breaks all existing databases; bump a format
  version and migrate instead.
- **Pair-trie entry flags are independent bits.** `FlagIsTerminal`, `FlagHasChild`, `FlagHasJump`,
  `FlagHidden` ([`types.go`](src/types.go)) coexist on one entry. A node can be *both* a terminal and a
  parent — this is what lets `ctx:` and `ctx:BERLIN` both hold values. Never treat "has child" as
  "not terminal" (regression risk in [`deletePairAt`](src/database.go) / [`insertPairAt`](src/database.go)),
  and always answer "does the key end on this entry?" before following its jump.
- **A node does not always start on a stride boundary.** Splitting a jump can leave a 1-byte branch
  with a continuation, so the child below it is offset by one byte. Every key walk must resolve its
  branch through [`selectPairBranch`](src/database.go), which falls back from the stride-aligned branch
  to the short one; hand-rolled `nextChunk` stepping loses keys on 2-byte databases.
- **`pair_index_bytes` is 1 or 2 and pinned per database at creation.** It sets the branch codec
  ([`pair_codec.go`](src/pair_codec.go)) stride, giving `∑ 256^i` logical branches per node. It is
  persisted in `pairs/format.dat` ([`pair_format.go`](src/pair_format.go)) and that marker is
  **authoritative on reopen** — a later config change cannot silently reinterpret an existing trie.
  Rebuild (`RESET_DB … pair_bytes=2`) to change it. Note [`Config.normalize`](src/config.go) coerces a
  `≤0` value to **2**, while [`defaultConfig`](src/config.go) ships **1**; an explicitly zeroed
  `pair_index_bytes` becomes 2, not 1.
- **Pair-node files are self-describing and adaptive.** Each `pairs/<hexid>.table` begins with a
  `PairHeaderSize` (12-byte) header (`"CHPT"`, version, mode, keyWidth, entry count —
  [`types.go`](src/types.go)) followed by either a sorted, binary-searched **LIST** body of
  `[branchKey|entry]` records (sparse nodes) or a direct-mapped **DENSE** array at
  `PairHeaderSize + branchIndex*PairEntrySize`. The 11-byte `PairEntry` layout is unchanged — only its
  container adapts. All of this is encapsulated in [`tables.go`](src/tables.go); callers keep addressing
  entries by branch index.
- **A node uses LIST only when its dense form exceeds `pair_list_max_bytes`.** `NewPairTable` computes
  `denseBytes = PairHeaderSize + branchCount*PairEntrySize` and sets `listEligible` only when that
  exceeds the budget. A 1-byte-stride node is **2,828 B** — already inside one 4 KiB filesystem block
  — so it is DENSE from creation (and preallocated) even with adaptive indexing on: listing it would
  save no space and only add search cost. In practice LIST applies to **2-byte-stride nodes only**.
  An eligible node densifies once it passes the byte budget, and never de-densifies.
  `pair_list_max_fill_percent` is an **optional** extra cap (percentage of branch capacity, default
  **0 = off**); at the default 4 KiB budget it can never bind, since 1% of 65,792 is 657 against a
  byte capacity of 292.
- **A dense node file is sparse: never bulk-`ReadAt` the whole span in one call.**
  [`ManagedFile.ReadAt`](src/file_manager.go) stops at the first sector entirely past EOF, so a single
  read across a hole silently truncates and hides every later entry. Use
  [`readSpanTolerant`](src/tables.go), which reads one sector at a time and tolerates `io.EOF`.
- **Payload cache must be invalidated on mutation.** `DELETE`/`EDIT` MUST call
  [`invalidatePayload`](src/database.go); the cache copies bytes on `Get`/`Add`
  ([`cloneBytes`](src/cache.go)) so callers may not mutate returned slices in place.
- **`EDIT` that changes payload length must relocate the value** to the correctly sized value table and
  recycle the old slot ([`Database.Edit`](src/commands.go)). Regression covered by
  [`TestEditResizesValues`](src/benchmark_test.go).
- **All pair/value table IO goes through the managed file layer.** [`ManagedFile`](src/file_manager.go)
  owns sector caching, the shared flush queue, fd-limit eviction, and checkpoints. Do not open those
  `.table` files with raw `os` calls; you would bypass dirty-sector flushing and the fd cap. Shutdown
  and [`FILE_CHECKPOINT`](src/database.go) drain this layer.
- **Every command answers on exactly one line.** The protocol is newline-delimited, so a multi-line
  response shifts every later answer on that connection. `LOG_FLUSH` used to break this and now
  returns its entries as a base64 JSON array in `payload=` (`formatLogFlushResponse` in
  [`database.go`](src/database.go), pinned by [`logger_test.go`](src/logger_test.go)); any new command with
  list-shaped output must do the same rather than emitting `\n`.
- **Reducers are registered, not hard-coded.** Add new `PAIR_REDUCE` modes in
  [`registerDefaultReducers`](src/reducers.go); the dispatcher resolves them by name. Do not extend the
  `ExecuteCommand` switch per reducer.
- **Graph keys use reserved control-byte prefixes.** `\x01gn:`, `\x02ge:`, `\x03go:`, `\x04gi:`,
  `\x05gt:` (the lexical term index, [`graph_recall.go`](src/graph_recall.go)) and `graph/idx/`
  ([`graph.go`](src/graph.go)) share the trie with user data. Never emit user keys under these
  prefixes. `GRAPH_QUERY` MUST anchor its left node by ID to stay index-backed
  ([`executeGraphQuery*`](src/graph.go)).
- **The term index is derived, never authoritative.** `\x05gt:<token>/<node>` entries are rebuilt from
  the node records at any time (`GRAPH_TERM_INDEX action=rebuild`) and recall degrades to exact-id
  seeds when they are missing. Node upsert/delete keep them in sync
  ([`graphSyncNodeTerms`](src/graph_recall.go)/[`graphDropNodeTerms`](src/graph_recall.go)); a new node
  write path must call them or its nodes become unreachable by free-text seed.
- **Prediction tables persist as fixed-byte `CHPREDTB` files**, not JSON. JSON appears only on the
  CLI/TCP wire ([`prediction_table.go`](src/prediction_table.go)); legacy `.json` tables are auto-migrated
  on first open.

---

## Architecture and data/control flow

All file names below are relative to [`src/`](src/).

```
                         ┌─────────────── main.go (CLI loop) ───────────────┐
client ── TCP ──►  server.go (per-conn loop)                                │
                         │  DATABASE / RESET_DB / EXIT handled here (front-end scope)
                         ▼
                  engine.go  ── GetDatabase(name) ──►  cheetah_data/<name>/  (lazy, cached)
                         ▼
              database.go : ExecuteCommand(line)   ← single command router
                 ├─ commands.go        Insert/Read/Edit/Delete, PairSet/Get/Del/Purge
                 ├─ tables.go          MainKeys / Values / Recycle / PairTable  ──► file_manager.go ──► disk
                 │                        └─ cache.go (payload LRU),  jump_store.go (suffix collapse)
                 ├─ reducers.go        PAIR_REDUCE counts/probs/continuations + graph degree/triangle/pagerank
                 ├─ graph.go           GRAPH_* nodes/edges/adjacency/query
                 │    └─ graph_recall.go  GRAPH_RECALL/SIMILAR/TERM_INDEX (associative recall)
                 ├─ prediction_*.go    PREDICT_* tables + context matrices
                 └─ cluster_*.go       CLUSTER_*/FORK_ASSIGN topology, gossip
                         ▲
              resource_monitor.go feeds worker sizing to scans/reducers/flush pool
```

- **Process boundary:** one Go process, one TCP listener (`0.0.0.0:4455` default) plus an interactive
  CLI (disabled with `CHEETAH_HEADLESS=1`). Concurrency is per-connection goroutines; databases are
  shared and internally locked.
- **Storage ownership:** each logical database is a directory under `cheetah_data/`. [`Engine`](src/engine.go)
  multiplexes them and guarantees clean shutdown flush.
- **External services:** none required. Optional peer TCP connections exist only when cluster gossip is
  configured (`CHEETAH_NODE_ID` + `CLUSTER_UPDATE`).

---

## Linked source tree and file reference

Every meaningful tracked file has its own subsection. The server sources all live in [`src/`](src/);
[`gold/`](gold/) and [`demo/graph-nell/`](demo/graph-nell/) are separate build targets covered at the
end. Generated/runtime paths (`cheetah-server`,
`cheetah_data/…`) are shown in code font without links because they are intentionally untracked.

### Entry points and orchestration

#### [`src/main.go`](src/main.go)

Boots the process: loads config, starts the [`ResourceMonitor`](src/resource_monitor.go) and
[`Engine`](src/engine.go), launches the TCP server goroutine, installs signal-based graceful shutdown, and
runs the CLI (unless `CHEETAH_HEADLESS=1`). Change this file for startup wiring and CLI-level command
handling.

- **Key functions:**
  - `main` — wiring order (config → monitor → engine → TCP goroutine → shutdown hook → CLI).
  - `runCLI` — the interactive read/eval loop; owns `DATABASE`, `RESET_DB`, `EXIT`, and delegates
    everything else to `currentDB.ExecuteCommand`.
  - `setupGracefulShutdown` — closes the engine + monitor on SIGINT/SIGTERM.
- **Common mistakes:** A new connection-scoped command must be added to **both** `runCLI` here and
  `handleConnection` in [`server.go`](src/server.go); adding it to only one silently diverges CLI and TCP.

#### [`src/server.go`](src/server.go)

The TCP front-end. Accepts connections, optionally enables OS keep-alives, reads newline-delimited
commands, and routes them exactly like the CLI (`DATABASE`/`RESET_DB` locally, else `ExecuteCommand`).

- **Key symbols:** `TCPServer`, `NewTCPServer`, `Start` (listener + keep-alive), `handleConnection`
  (per-connection current-DB state + command loop).
- **Common mistakes:** No prompt is written over TCP; responses are single `\n`-terminated lines. Keep
  responses one line — multi-line payloads break line-oriented clients.

#### [`src/engine.go`](src/engine.go)

Multi-tenant database registry. Lazily constructs and caches [`Database`](src/database.go) handles under
`basePath/<name>`, applies per-name overrides, and closes all databases on shutdown.

- **Key symbols:** `Engine`, `GetDatabase` (lazy create + cache), `ResetDatabase` (close + `RemoveAll`
  + drop from map), `SetDatabaseOverrides`, `DefaultDatabaseName`, `Close`.
- **Shutdown is idempotent end to end.** `Engine.Close` empties the registry, so a second call does
  nothing and a later `GetDatabase` reopens instead of handing back a closed handle;
  `Database.Close` is guarded by a `sync.Once` (it returns the first call's error), and so are
  `FileManager.Close` and `ClusterMessenger.Stop`, whose stop channels used to be closed twice.
  Signal shutdown, CLI `EXIT` and `Engine.Close` all land on the same handles, so keep any new
  shutdown path safe to call twice.
- **Common mistakes:** `ResetDatabase` deletes the directory on disk; callers must re-`GetDatabase`
  afterward (the front-ends do). It only resets one named database, never the whole data dir.

#### [`src/config.go`](src/config.go)

Loads settings from `config.ini` (path overridable via `CHEETAH_CONFIG_PATH`), applies environment
overrides, normalizes/clamps, and parses inline `key=value` database overrides for
`DATABASE`/`RESET_DB`.

- **Key symbols:** `Config`, `DatabaseConfig`, `DatabaseOverrides`; `defaultConfig`, `loadConfig`,
  `assignConfigValue` (`[server]`/`[database]`/`[tuning]` sections), `applyEnvOverrides`, `normalize`,
  `mergeDatabaseConfig`, `parseDatabaseTarget`.
- **Config keys** (`config.ini`): `[server] listen_addr, data_dir, default_database,
  keepalive_seconds|tcp_keepalive_seconds`; `[database] pair_bytes|pair_index_bytes,
  payload_cache_entries, payload_cache_mb, payload_cache_bytes, adaptive_pair_index`;
  `[tuning] max_pair_tables, pair_list_max_bytes, pair_list_max_fill_percent`.
- **Common mistakes:** `normalize` clamps `pair_index_bytes` into `[1,2]` but maps `≤0`→**2** (not the
  `defaultConfig` value of 1). All env keys are enumerated in [Configuration reference](#configuration-reference).

#### [`src/types.go`](src/types.go)

The on-disk format constants and the 5-byte value pointer. This is the wire format — see
[Critical contracts](#critical-implementation-contracts).

- **Key symbols:** size constants (`ValueLocationIndexSize`, `MainKeysEntrySize`,
  `EntriesPerValueTable`, `PairEntry*`, `PairTablePreallocatedSize`); flag bits (`FlagIsTerminal`,
  `FlagHasChild`, `FlagHasJump`, `FlagHidden`); the adaptive pair-node container constants
  (`PairFileMagic`, `PairFormatVersion`, `PairHeaderSize`, `PairModeList`, `PairModeDense`) and the
  per-database marker magic (`PairFormatFileMagic`, `PairFormatFileVersion`); `ValueLocationIndex`
  with `Encode`/`DecodeValueLocationIndex` (note: `TableID` is truncated to 24 bits on disk).
- **Common mistakes:** `Encode` returns 5 bytes packing a 3-byte table id + 2-byte entry id; do not
  assume a full 32-bit table id survives a round trip.

### Core storage engine

#### [`src/database.go`](src/database.go)

The heart of the engine (~4,000 lines). Owns the `Database` struct, the **central command router**
`ExecuteCommand`, the pair-trie insert/lookup/delete/scan/summary machinery, reducer orchestration,
and the glue handlers for prediction, cluster, and graph commands. Most feature work touches this file.

- **Router:** `ExecuteCommand` — the single `switch` mapping every command (except front-end
  `DATABASE`/`RESET_DB`) to a handler. This is the canonical command inventory.
- **Lifecycle / tables:** `NewDatabase` (creates `pairs/` + `pair_jumps/`, wires cache, file manager,
  scheduler, reducers, prediction store), `Close`, `getValuesTable`, `getRecycleTable`, `getPairTable`,
  `pairTableCache` (fd-bounded LRU of open `PairTable`s via `resolvePairTableLimit`).
- **Trie entry codec:** `entryHasTerminal/entryHasChild/entryHasJump/entryIsHidden`,
  `setEntryTerminal/setEntryChild/setEntryJump` — the bit-level accessors over an 11-byte entry.
- **Trie mutation:** `insertPairAt`, `insertThroughJump`, `splitJumpWithCommonPrefix`,
  `splitJumpIntoChild`, `insertSuffixWithContinuation`, `deletePairAt`, `deleteWithinJump`,
  `promoteChildToJump`, `collectSingleBranchPath` — the jump-node collapse/split logic. All key walks
  pick their branch through `selectPairBranch`, and delete reports emptiness per **node**
  (`PairTable.IsEmpty`), never per entry. High regression density; see
  [Prefix overlaps](#pitfall-jump-nodes).
- **Scan / summary / reduce:** `PairScanWithOptions`, `PairSummaryWithOptions`, `handlePairReduce`,
  `reduceWithPayload`, `collectPairEntries`/`walkPairTable` (scan),
  `parallelSummarizePairEntries`/`walkPairSummary` (summary), prefix resolution
  (`resolveScanPrefix`/`resolveSummaryPrefix`, `selectPairBranch`, `readBranchEntry`,
  `branchMatchesPartial`), cursor helpers (`comparePrefixToCursor`, `nextCursorForPrefix`),
  accumulators, and the async reduce handlers.
- **The two walks are deliberately different.** A **scan** is ordered and sequential
  (`orderedBranches` → smallest branch first) so it can stop as soon as the page is settled; a
  **summary** has to visit everything anyway, so it stays parallel, sizing its task queue on the
  branch fan-out and draining inline (`select` + `default`) when the queue is full — a blocking send
  there deadlocked `PAIR_SUMMARY` forever.
- **Handler glue:** `handlePredict*`, `handleCluster*`, `handleForkAssign`, `systemStatsResponse`,
  `buildForkTransferPayload`/`applyForkTransferPayload`, `parseFileCheckpointArgs`.
- **Depends on:** every other core file. **Tests:** [`benchmark_test.go`](src/benchmark_test.go)
  (edit/resize, throughput), [`graph_test.go`](src/graph_test.go) (via graph handlers).
- **Common mistakes:** `RECYCLE` and a standalone `CURSOR` command are **absent** from the switch
  despite [`README.md`](README.md) listing them — do not assume a command exists because a doc mentions
  it. Cursor continuation is positional: `PAIR_SCAN <prefix> <limit> <cursor>`.

#### [`src/commands.go`](src/commands.go)

The primitive KV + pair-mapping operations invoked by `ExecuteCommand`.

- **Key functions:** `Insert`/`persistPayload` (size-partitioned write + recycle reuse), `Read`, `Edit`
  (relocates on size change), `Delete` (tombstone + recycle + cache invalidate), `PairSet`,
  `PairSetHidden`, `PairGet`, `PairDel`, `PairPurge`/`purgePairEntries` (batched namespace wipe).
- **Depends on:** [`tables.go`](src/tables.go), [`helpers.go`](src/helpers.go), [`cache.go`](src/cache.go).
- **Common mistakes:** `Insert` validates that a `INSERT:<n>` declared size matches the payload; the
  size partitions the value table, so a wrong size lands the payload in the wrong file.

#### [`src/tables.go`](src/tables.go)

The four on-disk table abstractions. Each maps a logical structure onto files (via
[`file_manager.go`](src/file_manager.go) for values/pairs).

- **Key types:** `MainKeysTable` (per-key metadata, striped locks), `ValuesTable` (fixed-width blobs,
  async write loop), `RecycleTable` (LIFO tombstone stack per value size), `PairTable` (a single trie
  node in the adaptive LIST/DENSE container), plus the `pairTableTracker` interface the fd-cache
  implements.
- **`PairTable` key symbols:** `NewPairTable` (takes `adaptive`/`listMaxBytes`/`listMaxFillPercent`,
  derives `listEligible`, and preallocates the array for every node that starts dense), `loadHeader`/`writeHeaderLocked`, `ReadEntry`/`WriteEntry`
  (mode-dispatched; a missing branch reads back as a zero entry, never `io.EOF`),
  `writeListLocked` (sorted insert / in-place replace / record delete), `densifyLocked` (LIST→DENSE,
  clearing the old LIST region so unpopulated dense slots read zero), `listSearchLocked` (binary
  search), `PopulatedBranchIndices` (ordered iterator used by every enumeration path),
  `readSpanTolerant`, `branchKeyWidth`, `IsEmpty`, `Snapshot`.
- **Depends on:** [`file_manager.go`](src/file_manager.go). **Tests:**
  [`pair_adaptive_test.go`](src/pair_adaptive_test.go); also exercised indirectly by
  [`benchmark_test.go`](src/benchmark_test.go).
- **Common mistakes:** `PairTable` handles are reference-counted/idle-closed; call `ReleaseFile` paths
  through the cache rather than closing files directly. In-memory `mode`/`count` survive fd eviction
  (only the `*ManagedFile` is released), so the header is read once at open — never re-read it under
  a read lock. Do not bulk-read a dense span with one `ReadAt` (see the sparse-file contract above).

#### [`src/helpers.go`](src/helpers.go)

Small value/key utilities used across the engine.

- **Key functions:** `parseValue` (plaintext or `x<hex>` prefix decoding — the shared input decoder for
  pair keys), `readValueSize`/`writeValueSize`, `loadHighestKey`/`findNewHighestKey` (main-keys
  bookkeeping), `getAvailableLocation` (recycle-first slot allocation).
- **Common mistakes:** `parseValue` is where `x…` hex keys are interpreted; any new command taking a
  binary prefix should reuse it rather than re-implementing hex handling.

#### [`src/cache.go`](src/cache.go)

Bounded LRU payload cache keyed by `<value_size, table_id, entry_id>`.

- **Key symbols:** `payloadCache`, `newPayloadCacheFromConfig`, `Get`/`Add`/`Invalidate`,
  `evictIfNeeded`, `Stats` (feeds `SYSTEM_STATS`), `advisoryBypassBytesLocked`, `cloneBytes`.
- **Common mistakes:** entries are copied in and out; do not retain or mutate the returned slice. Size
  budget is entries **and** bytes — both caps apply.

#### [`src/pair_codec.go`](src/pair_codec.go)

Translates key bytes into trie branch indices for the configured stride.

- **Key symbols:** `pairBranchCodec`, `newPairBranchCodec(chunkBytes)`, `branchIndexFromChunk`,
  `decode`, `walkKey` (drives one branch per 1- or 2-byte chunk).
- **Common mistakes:** the codec encapsulates the `pair_index_bytes` stride; trie code should walk keys
  via `walkKey` instead of hand-rolling byte stepping, or 2-byte databases break.

#### [`src/pair_format.go`](src/pair_format.go)

Pins and persists a database's pair-trie container format in `pairs/format.dat`, and guards against
opening a legacy (headerless) directory.

- **Key symbols:** `pairFormat` (stride / adaptive / listMaxBytes / listMaxFillPct),
  `resolvePairFormat` (marker wins
  if present, else derive from config and write it), `loadPairFormat`, `writePairFormat`,
  `pairDirHasTableFiles`.
- **Behavior:** a `pairs/` directory holding `*.table` files but **no** marker is refused with
  `incompatible_pair_format_rebuild_required` rather than silently misread — the operator rebuilds
  with `RESET_DB`. A fresh directory gets a marker written from config.
- **Common mistakes:** the marker is authoritative on reopen, so changing `pair_index_bytes` or
  `adaptive_pair_index` in `config.ini` does **not** affect an existing database (by design — the old
  behavior silently reinterpreted on-disk data). `RESET_DB <name> [pair_bytes=…] [adaptive_pair_index=…]`
  is the way to adopt new settings.

#### [`src/jump_store.go`](src/jump_store.go)

Persists jump nodes (collapsed unique suffixes) in `pair_jumps/jumps.bin` + `index.bin`, with legacy
`.jump` file back-fill.

- **Key symbols:** `JumpNode`, `createJump`, `loadJump`/`loadJumpFromIndexLocked`, `writeJump`,
  `deleteJump`, `ensureJumpStoreLocked`, `loadJumpFromLegacyFileLocked` (migration),
  `idToIndex`/`decodeJumpAt`.
- **Common mistakes:** the single-file store replaced a millions-of-inodes `.jump`-per-file scheme;
  don't reintroduce per-node files.

#### [`src/file_manager.go`](src/file_manager.go)

The managed file layer: sector-cached, flush-queued IO shared by all value/pair tables, with global
memory-pressure eviction, an fd cap, and a checkpoint controller.

- **Key symbols:** `FileManager` (flush worker pool sized by `CHEETAH_FLUSH_WORKERS`/CPU, policy loop,
  fd limiter, `ForceCheckpoint`), `ManagedFile` (`ReadAt`/`WriteAt` over a `sectorEntry` cache,
  `markDirty`, `queueFlush`, idle handle close/reopen), `acquireHandle`/`syncHandle` (the only ways
  to touch `ManagedFile.file`), `cachePolicyConfig` + `loadCachePolicyFromEnv` (the `CHEETAH_CACHE_*`
  knobs).
- **Handle contract:** `ManagedFile.file` is guarded by `handleMu` (a `sync.RWMutex`). Every read or
  write pins it through `acquireHandle`, which opens the file if needed and keeps the lock held until
  the returned release func runs; `closeHandle`/`forceCloseHandle` take the write lock, so a
  concurrent idle close, fd-cap eviction, or `FILE_CHECKPOINT close_handles` can no longer swap the
  descriptor mid-IO. Never read the field directly — pass the `*os.File` down (`readWithCache`,
  `writeWithCache`, `getSector`, `ensureSector` all take it as a parameter) and fsync via
  `syncHandle`. Lock order is `handleMu` → `cacheMu`/`pendingMu`, never the reverse.
- **Sector contract:** `cacheMu` guards the sector *map* and the `dirty` flag; the *contents* of a
  `sectorEntry` are guarded by its own `dataMu`, because two `cacheMu` read locks do not exclude each
  other — two writers to one sector, or a writer against the flusher's copy, would race. Go through
  `sectorEntry.readInto`/`writeFrom`/`clone` rather than touching `entry.data` directly, and do not
  nil out `data` on eviction: dropping the entry from the map is enough, and a reader may still hold
  the pointer.
- **Common mistakes:** this is the only correct path to the backing files; bypassing it drops dirty
  data and defeats the fd cap that prevents "too many open files".

#### [`src/fd_limit_unix.go`](src/fd_limit_unix.go) / [`src/fd_limit_windows.go`](src/fd_limit_windows.go)

Build-tagged `fileDescriptorSoftLimit()` used to derive the default open-`PairTable` cap. Unix reads
`RLIMIT_NOFILE`; Windows returns a safe constant. Edit the pair whose platform you target.

### Reducers and async jobs

#### [`src/reducers.go`](src/reducers.go)

The reducer registry and the graph-specific reducers.

- **Key symbols:** `ReducerRegistry` (`Register`/`Resolve`), `registerDefaultReducers` (maps
  `counts/count/probabilities/probs/backoffs/continuations` to the inline-payload reducer and
  `degree/triangle/pagerank_seed` to graph reducers), `reduceGraphDegree`, `reduceGraphTriangles`,
  `reduceGraphPageRankSeeds`, `decodeGraphAdjacencyEntry`.
- **Tests:** [`TestGraphReducersDegreeTriangleAndPageRankSeed`](src/graph_test.go).
- **Common mistakes:** register new modes here; the count/prob/continuation reducers are payload
  pass-throughs (they stream the stored bytes) — the *meaning* of those bytes is a client contract.

#### [`src/reduce_jobs.go`](src/reduce_jobs.go)

In-memory job manager backing `PAIR_REDUCE_ASYNC`/`_STATUS`/`_FETCH` (progress %, state, results,
next cursor). Jobs are process-local and not persisted.

#### [`src/predict_jobs.go`](src/predict_jobs.go)

In-memory job manager for `PREDICT_INHERIT_ASYNC`/`_STATUS`/`_FETCH` (merged/skipped/failed counters).
Also process-local.

### Prediction tables

#### [`src/prediction_table.go`](src/prediction_table.go)

The matrix-prediction engine (~1,800 lines): fixed-byte `CHPREDTB` table format, context-matrix
evaluation/training, and the simulated GPU merge path.

- **Key symbols:** `PredictionTable`, `ContextMatrix`, `PredictionEntry`/`PredictionValue`/
  `ContextWeight`; `loadBinaryLocked`/`persistLocked`/`loadLegacyJSONLocked` (format + migration);
  `SetPrediction`, `Evaluate`, `Train` (forward/backward weight update), `InheritValue`,
  `ApplyContextAdjustment`; `deepenContextMatrix` + the `vector*` helpers (mean/variance/RMS/tanh
  derived layers, gated by `CHEETAH_PREDICT_DEEPEN`); `pruneEntryContextWeights` (drops negligible
  weights, threshold `CHEETAH_PREDICT_PURGE_THRESHOLD`); async flush worker
  (`CHEETAH_PREDICT_FLUSH_MILLIS`).
- **Common mistakes:** on-disk tables are binary; JSON only appears on the wire. Context matrices and
  window specs cross the protocol as base64-encoded JSON.

#### [`src/prediction_manager.go`](src/prediction_manager.go)

Per-database registry of named prediction tables, sanitizing table names into
`prediction_<name>.table` paths.

- **Key symbols:** `PredictionManager`, `Get` (open-or-create), `tablePaths`, `sanitizeTableName`,
  `ListTables`, `Close`.

### Graph store

#### [`src/graph.go`](src/graph.go)

Property-graph storage and the `GRAPH_QUERY` engine (~2,800 lines) over four reserved trie namespaces
plus a property index.

- **Key symbols:** namespace prefix constants (`graphNodePrefix … graphEdgeIndexPrefix`), records
  `GraphNodeRecord`/`GraphEdgeRecord`; command handlers `handleGraphNodeSet/Get/Del`,
  `handleGraphEdgeSet`, `handleGraphEdgeSetBatch`, `handleGraphNeighbors`, `handleGraphDegree`,
  `handleGraphNeighborTypes`, `handleGraphQuery`; query engine `parseGraphQuery`, `graphQueryPlan`,
  `executeGraphQuerySingleHop`, `executeGraphQueryMultiHop` (bounded HOPS + BRANCH_LIMIT + COST_LIMIT),
  and the secondary-index path `graphIndexedEdgeCandidates`/`graphScanIndexedEdgeIDs`.
- **Tests:** [`graph_test.go`](src/graph_test.go) — lifecycle, parser rules, batch upsert, multi-hop
  bounds/cost, reverse single hop, property secondary index.
- **Common mistakes:** the left node of a `MATCH` must be ID-anchored; wildcard-left queries are
  intentionally rejected to keep execution index-backed. Reverse queries keep that anchor on the left
  and flip the arrow (`(id='x')<-[:t]-(*)`), which means **every direction-dependent branch must
  mirror both endpoints** — see [direction mirroring](#pitfall-graph-direction).

#### [`src/graph_recall.go`](src/graph_recall.go)

Associative recall — the "hippocampus" path. Turns a handful of terms into the whole neighbourhood
worth exploring, ranked, with the evidence for each item, so a model can pick instead of guessing the
next query.

- **Key symbols:** the reserved term-index namespace `graphTermIndexPrefix` (`\x05gt:`) with
  `graphTermPairKey`/`graphTermScanPrefix`/`graphNodeIndexTokens`/`graphSyncNodeTerms`/
  `graphDropNodeTerms`/`graphEnsureTermEntry`/`graphTermCandidates`/`graphRebuildTermIndex`; lexical
  helpers `graphRecallTokens`/`graphRecallTokenSet`/`graphRecallJaccard`; scoring
  `graphRecallAffinity` (weight × confidence), `graphRecallNoisyOr`, `graphRecallNovelty`; option
  parsing `graphParseRecallOptions`/`graphParseRecallExpansion`/`graphRecallResolveSynonymTypes`/
  `(*graphRecallOptions).applyTypeFilter`; seed resolution `graphResolveRecallSeeds`/
  `graphResolveRecallTerm`/`graphSynonymsOf`; traversal `graphRecallLinks`, `graphRecallSpread`,
  `(*graphRecallRun).touch`/`path`/`associations`; handlers `handleGraphRecall`, `handleGraphSimilar`
  (with `graphSimilarMatches`), `handleGraphTermIndex`.
- **Depends on:** [`graph.go`](src/graph.go) (adjacency scan, node/edge records, pair-payload helpers)
  and [`graph_uncertainty.go`](src/graph_uncertainty.go) (`graphEffectiveConfidence`/`Modality`, and the
  modality scale, which `precision=` reuses so `precision=probable` means 0.75).
  **Tests:** [`graph_recall_test.go`](src/graph_recall_test.go).
- **Common mistakes:** activation is combined across seeds with **noisy-OR**, so a node can pass
  `precision` even when no single seed reaches it — the frontier is therefore pruned at
  `precision / seeds` (floored at `graphRecallMinActivation`), never at `precision`; tightening that
  cut silently drops convergences. `distance` is **conceptual depth** (synonym hops cost a hop but no
  depth), which is not the `hops` reported per source. Traversal never hydrates node records — labels
  are read only for the items that survive the limit.

#### [`src/graph_uncertainty.go`](src/graph_uncertainty.go)

First-class uncertainty and ambiguity on edges: the modality scale (words ↔ numbers) and the
`GRAPH_AMBIGUITY_*` commands.

- **Key symbols:** `graphModalityScale` (the five ordered anchors `ruled_out .25 possible .75 certain`)
  and `graphModalityAliases`; `graphModalityByName`, `graphModalityForConfidence` (nearest anchor),
  `graphModalityRank`, `graphParseConfidenceToken`, `graphResolveUncertaintyArgs` (the
  `confidence=`/`modality=` pair, with the "not passed vs passed" distinction);
  `graphEffectiveConfidence`/`graphEffectiveModality` (an undeclared edge reads as `certain`);
  `graphParseAmbiguityOptions`, `graphParseAmbiguityShare`, `graphDistributeAmbiguity` (probability vs
  relative-share reading), `graphRoundConfidence`; handlers `handleGraphAmbiguitySet/Get/Resolve` and
  `graphCollectAmbiguityGroup`.
- **Depends on:** [`graph.go`](src/graph.go) (record, upsert, adjacency scan). **Tests:**
  [`graph_uncertainty_test.go`](src/graph_uncertainty_test.go).
- **Common mistakes:** `GraphEdgeRecord.Confidence` is a `*float64` because `0` means `ruled_out` and
  must stay distinct from "never declared". Inside a group, option shares may exceed 1 (they are
  relative and get normalized); a standalone `confidence=` may not. Groups are anchored to one node's
  adjacency — there is no global group index.

### Cluster coordination

#### [`src/cluster_scheduler.go`](src/cluster_scheduler.go)

Consistent-hash fork scheduler. Derives a deterministic `fork_id` per prefix, maps forks to nodes over
a hash ring, and persists topology to `cluster_topology.json`.

- **Key symbols:** `ClusterTopology`/`ClusterNode`, `ForkScheduler`, `load`/`UpdateTopology`
  (persist + rebuild ring), `AssignFork`, `deriveForkID`, `ForceAssignment` (the `CLUSTER_MOVE`
  override), `Snapshot`, `recordForkObservation`.
- **Common mistakes:** `load` restores **topology only**; the `overrides` (forced assignments) and
  `samples` maps are **not** persisted, so `CLUSTER_MOVE` reassignments are lost on restart (see
  [Known Gaps](#known-gaps)). Standalone fork tracking is off unless `CHEETAH_TRACK_STANDALONE_FORKS`.

#### [`src/cluster_gossip.go`](src/cluster_gossip.go)

Peer messenger: heartbeats and `fork_move` broadcasts (including the built `forkTransferPayload`) to
nodes registered via `CLUSTER_UPDATE`.

- **Key symbols:** `clusterMessage` (carries an optional `Payload`), `ClusterMessenger`,
  `NotifyForkMove`, `sendHeartbeat`, `sendMessage` (dials peers and issues `CLUSTER_GOSSIP json=…`).
- **Common mistakes:** local node id comes from `CHEETAH_NODE_ID` (falls back to hostname). Gossip is
  best-effort fire-and-forget; peers must have registered addresses.

### Resource monitoring and logging

#### [`src/resource_monitor.go`](src/resource_monitor.go)

Samples CPU/goroutines/IO/memory and advises worker counts.

- **Key symbols:** `ResourceMonitor`, `ResourceSnapshot`, `Snapshot`, `RecommendedWorkers`,
  `buildWorkerHints`/`computeRecommendedWorkers` (queue-depth → worker-count hints in `SYSTEM_STATS`).
- **Depends on:** platform samplers below.

#### [`src/resource_samples_unix.go`](src/resource_samples_unix.go) / [`src/resource_samples_windows.go`](src/resource_samples_windows.go)

Build-tagged samplers for process CPU time, system CPU, `/proc/self/io`, and memory. Unix reads
`/proc`; Windows returns "unsupported" for the Linux-only stats. Edit per platform.

#### [`src/logger.go`](src/logger.go)

Leveled logging with an in-memory ring buffer feeding `LOG_FLUSH`.

- **Key symbols:** `LogLevel`, `parseLogLevel` (`CHEETAH_LOG_LEVEL`), `LogBuffer` (ring, `Flush`),
  `logErrorf`/`logInfof`/`logVerbosef`, package `logSink`.
- **Common mistakes:** verbose (level 3) logging summarizes args/responses to avoid dumping payloads;
  keep new log calls at the right level so hot paths stay quiet by default. The `LOG_FLUSH` response
  itself is formatted in [`database.go`](src/database.go) (`formatLogFlushResponse`), **not** here, and
  must stay on one line — entries travel as a base64 JSON array.

### Tests

#### [`src/graph_test.go`](src/graph_test.go)

Unit tests for the graph subsystem: edge lifecycle + query, parser rules, batch upsert (+
continue-on-error), multi-hop bound/cost, property secondary index, and the three graph reducers.
Includes the `assertCommandPrefix` / `decodePairReducePayloads` helpers reused for reducer assertions.

#### [`src/graph_recall_test.go`](src/graph_recall_test.go)

Associative recall: multi-seed convergence and the `min_sources=2` view, novelty ordering (a distant
two-seed node beats a one-seed node at the same distance), free-text seeds resolved lexically and
through alias edges (and `expand=exact` turning both off), the precision gate against declared edge
confidence (`precision=probable` = 0.75, like `edge.confidence`), `GRAPH_SIMILAR` on shared neighbours
vs shared words, the term-index lifecycle (auto-maintained, label removal, node delete,
`CHEETAH_GRAPH_TERM_INDEX=0` + rebuild + drop), and budget exhaustion answering `truncated=1` on one
line. Provides `newRecallTestDB`, `seedRecallGraph`, `recallPayload`, `findAssociation`.

#### [`src/pair_scan_test.go`](src/pair_scan_test.go)

Scan/summary contracts that are independent of the container format:
`TestPairScanMidChunkPrefix` (prefixes that end inside a branch at stride 2 return the same sets as
stride 1, for `PAIR_SCAN` and `PAIR_SUMMARY`), `TestPairScanPrefixParityAcrossStrides` (the same
contract over ~300 prefixes of a deterministic random key set — it caught 141 wrong prefixes before
the fix), and `TestPairSummaryDrainsSaturatedQueue` (the summary walk must finish with a task queue
too small for the fan-out — the old blocking send hung there), `TestPairScanCursorPagination`
(paginating a prefix returns exactly the keys of a single scan, in order, for page sizes from 1 to
larger than the key set), and `TestPairSetGetDeleteRoundTrip`
(set/get/delete/scan over the same overlapping key set: the stride-2 case fails whenever insert,
lookup and delete disagree on which branch continues a key). Provides `mustInsertPair`, which writes
a real payload because `PAIR_SUMMARY` reads value sizes, and `overlappingWords`.

#### [`src/file_manager_test.go`](src/file_manager_test.go)

`TestManagedFileConcurrentHandleLifecycle` — reads, writes, `Flush`, `ForceCheckpoint(CloseHandles)`
and `forceCloseHandle` in parallel on one `ManagedFile`, in a cache-disabled (`direct`) and a
cache-enabled (`cached`) variant. Run it with `-race`: `direct` fails without the `handleMu`
protection of `ManagedFile.file`, `cached` without the per-`sectorEntry` `dataMu`.

#### [`src/lifecycle_test.go`](src/lifecycle_test.go)

Shutdown lifecycle: `TestDatabaseCloseIsIdempotent` (three consecutive `Close` calls) and
`TestEngineCloseIsIdempotent` (double `Engine.Close`, then a `GetDatabase` that must reopen and still
read its data). Both panicked with "close of closed channel" before the stop channels were guarded.

#### [`src/benchmark_test.go`](src/benchmark_test.go)

`TestEditResizesValues` (correctness of size-changing edits) plus `TestCheetahDBBenchmark`, a
throughput harness gated by `CHEETAHDB_BENCH=1` and tuned by `CHEETAHDB_BENCH_DURATION/_WORKERS/
_VALUE_SIZE`. Writes logs a client would rotate under `var/eval_logs/` (untracked).

### Separate build targets (not part of the server binary)

#### [`gold/basic.go`](gold/basic.go)

A self-contained **reference prototype** (`package main` in `gold/`) illustrating the original
value-table + recycle idea. `Read`/`Edit` are stubs ("not fully implemented"). It builds independently
(`go build ./gold`) but is **not** the shipping engine and shares no code with it. Use it as a concept
reference only; do not implement features here.

#### [`demo/graph-nell/`](demo/graph-nell/)

A benchmark/evaluation **client** (`package main`) that drives a running `cheetah-server` over TCP
against the NELL dataset. [`main.go`](demo/graph-nell/main.go) ingests edges via `GRAPH_EDGE_SET_BATCH`,
benchmarks `GRAPH_NEIGHBOR_TYPES`, and scores probability/implicit-correlation prediction quality;
`run(cfg)` returns the `summaryReport` (so a test can assert on it) while `main` just prints it.
[`run.sh`](demo/graph-nell/run.sh) and [`README.md`](demo/graph-nell/README.md) document flags;
`reports/` holds committed JSON/CSV run artifacts. Uses
[`studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv`](studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv).

[`main_test.go`](demo/graph-nell/main_test.go) adds two layers of coverage: (a) fast, hermetic **unit
tests** for the evaluation/loader math (`rocAUC`/`averagePrecision`/`precisionAtK`, `buildModels`,
`splitEdges`, `loadNELLEdges`, `rerankImplicitTopK`, token helpers) that run in the normal `go test`
sweep, and (b) `TestGraphNELLEndToEnd` — a **real-execution** test gated behind `CHEETAH_NELL_E2E=1`
that builds the server binary (`go build … cheetahdb/src`), boots it headless on an ephemeral port with an isolated data
dir, drives the full `run()` pipeline over TCP against a small synthetic NELL dataset, and asserts on
the returned report plus a direct post-run `GRAPH_QUERY`. Perf note learned here: early NELL edges are
node-diverse, so ingest is new-node/new-file bound (~30–40 edges/s) and only reaches ~600+ edges/s once
nodes are reused — keep automated runs to a few hundred edges.

### Runtime / generated (never edited or committed)

- `cheetah-server` — the built binary (`.gitignore`d).
- `cheetah_data/<db>/` — per-database directory: `main_keys.table`, `values_<size>_<id>.table`,
  `values_<size>.recycle.table`, `pairs/<hexid>.table` + `pairs/next_id.dat` + `pairs/format.dat`,
  `pair_jumps/{jumps.bin,index.bin,next_id.dat}`, `prediction_<name>.table`, `cluster_topology.json`.
  Source of truth is the engine; these are outputs.

---

## Features and recurring development pitfalls

### Key/value store — Shipped

- **Behavior:** `INSERT`/`READ`/`EDIT`/`DELETE` over size-partitioned value tables with slot recycling
  and a payload LRU. Keys are numeric main-key offsets.
- **Flow & owners:** `ExecuteCommand` → [`commands.go`](src/commands.go) → [`tables.go`](src/tables.go) →
  [`file_manager.go`](src/file_manager.go); cache via [`cache.go`](src/cache.go).
- **Tests:** [`TestEditResizesValues`](src/benchmark_test.go). **Gaps:** none known.

### Pair-trie namespaces + scan/summary — Shipped

- **Behavior:** `PAIR_SET`/`PAIR_GET`/`PAIR_DEL`/`PAIR_SET_HIDDEN` bind byte prefixes to keys;
  `PAIR_SCAN` streams ordered slices with cursors; `PAIR_SUMMARY` aggregates fan-out/payload stats;
  `PAIR_PURGE` batch-wipes a namespace. Unique suffixes collapse into jump nodes.
- **Flow & owners:** [`database.go`](src/database.go) (`insertPairAt`, `PairScanWithOptions`,
  `PairSummaryWithOptions`) + [`pair_codec.go`](src/pair_codec.go) + [`jump_store.go`](src/jump_store.go).
- **Constraints:** cursor continuation is positional (`PAIR_SCAN <prefix> <limit> <cursor>`);
  `include_hidden=1` surfaces hidden terminals. **Gaps:** rolling-hash/Top-K digests are roadmap
  ([`NEXT_STEPS.md`](NEXT_STEPS.md)).
- **A page is a page: complete, ordered, and resumable.** `PAIR_SCAN` keeps the smallest `limit+1`
  keys above the cursor in a bounded heap and prunes any branch whose path already exceeds the
  largest kept — a subtree can only contain keys ≥ its path, so the cut is sound whatever the visit
  order. The walk therefore never stops on "enough results collected", which is what used to leave
  holes: pages are exactly `limit` long until the last one, strictly increasing, and the next cursor
  is set whenever (and only whenever) more keys exist. Anything that collects scan results must go
  through `pairScanAccumulator.add`/`shouldPrune` rather than counting on its own.
- **Prefixes are byte-granular, node chunks are not.** A prefix does not have to land on a node
  boundary: at stride 2 an odd-length prefix ends *inside* a branch, and a jump split can leave a
  1-byte branch mid-key. `resolveScanPrefix`/`resolveSummaryPrefix` therefore return the unconsumed
  bytes as a **partial**, which [`branchMatchesPartial`](src/database.go) applies as a filter on the seed
  node's branches (1-byte branches must equal it, 2-byte branches must start with it), while
  [`selectPairBranch`](src/database.go) handles the unaligned-node case for every walk. Any new prefix
  walk must reuse those helpers instead of assuming `len(prefix) % stride == 0`.

### Adaptive pair-node indexing — Shipped

- **Behavior:** each trie node picks its own physical container. Sparse nodes are a sorted,
  binary-searched LIST of `[branchKey|entry]` records; a node densifies into the direct-mapped array
  once it passes `pair_list_max_bytes` (4096), and never reverts.
  Enabled by default (`adaptive_pair_index`), disable for the legacy always-dense profile. The stride
  and the flag are pinned per database in `pairs/format.dat`.
- **Why:** a 2-byte-stride node reserved ~707 KB (65,792 slots × 11 B) even when holding a handful of
  children, and every enumeration scanned all 65,792 slots. Measured on a 20k-key workload
  (`TestAdaptivePairIndexBenchmark`), **stride 2**: `pairs/` **776.5 MiB → 4.7 MiB apparent**
  (−99.4%), **~81–87 MiB → 4.5 MiB allocated** (−94%; the fixed figure varies with sparse-file
  allocation), full-trie enumeration **1.6–1.9 s → 5 ms** (−99.7%). Insert and lookup are within
  run-to-run noise — the win is storage and enumeration, not point-operation speed. Both modes
  visited an identical 21,124 entries.
- **Where it does not apply:** **stride 1**. An earlier revision did list narrow nodes and measured
  no size win (24.7 MiB either way) plus *slower* enumeration (110ms → 145ms over 6,331 nodes).
  Narrow nodes are now excluded by the `denseBytes > pair_list_max_bytes` rule, so stride 1 keeps the
  original dense behaviour exactly — re-measured at 156ms vs 149ms, i.e. parity within noise.
  **Consequence:** `adaptive_pair_index` is a no-op for 1-byte databases, which is the
  [`defaultConfig`](src/config.go) stride; the savings require `pair_index_bytes = 2`.
- **Flow & owners:** [`pair_format.go`](src/pair_format.go) (pin/guard) → [`database.go`](src/database.go)
  (`NewDatabase` resolves the format, `getPairTable` passes it) → [`tables.go`](src/tables.go)
  (`PairTable` container + `PopulatedBranchIndices`). Enumeration callers
  (`walkPairTable`, `walkPairSummary`, `collectSingleBranchPath`) iterate populated branches only
  (the scan walk in key order, via `orderedBranches`).
- **Tests:** [`pair_adaptive_test.go`](src/pair_adaptive_test.go), benchmark
  [`pair_adaptive_bench_test.go`](src/pair_adaptive_bench_test.go) (`CHEETAHDB_ADAPTIVE_BENCH=1`).
- **Constraints:** existing pre-format databases must be rebuilt (`RESET_DB`); there is no in-place
  migration.

### Reducers — Shipped

- **Behavior:** `PAIR_REDUCE <mode> <prefix>` streams inline base64 payloads; async variants poll.
  Modes: `counts/probabilities/continuations` (payload pass-through) and graph
  `degree/triangle/pagerank_seed`.
- **Owners:** [`reducers.go`](src/reducers.go), [`reduce_jobs.go`](src/reduce_jobs.go),
  `handlePairReduce`/`reduceWithPayload` in [`database.go`](src/database.go).
- **Tests:** [`TestGraphReducersDegreeTriangleAndPageRankSeed`](src/graph_test.go).

### Graph store + `GRAPH_QUERY` — Shipped

- **Behavior:** typed nodes/edges with adjacency indexes, degree/neighbor-type stats, batch edge
  upsert, and a bounded multi-hop pattern query with `WHERE`/`HOPS`/`BRANCH_LIMIT`/`COST_LIMIT`/
  `RETURN`/`LIMIT`/`CURSOR`, including an `edge.props.*` secondary index.
- **Owners:** [`graph.go`](src/graph.go). **Tests:** [`graph_test.go`](src/graph_test.go).
- **Constraints:** left node ID-anchored; reserved `\x01..\x04` + `graph/idx/` prefixes; a reverse
  `<-[:t]-` pattern anchors on the left node and reads the `adj/in` index.

### Associative recall ("hippocampus") — Shipped

- **Behavior:** `GRAPH_RECALL seeds=a,b,…` spreads activation from every seed at once and returns each
  reached node with its score, the seeds that reached it, the conceptual `distance`, a `novelty`
  score and the `via` evidence path (per edge: weight, confidence, modality). Activation combines
  across seeds in **noisy-OR**, so a node two seeds reach outranks either seed's own neighbours —
  `min_sources=2` returns only those convergences, which is the "unexpected correlation" view.
  Seeds resolve by exact id, by lexical overlap through the `\x05gt:` term index, and through declared
  synonym edges. `GRAPH_SIMILAR id=<node>` answers "what else is like this" from shared neighbours
  (distributional) and shared id words (lexical). `GRAPH_TERM_INDEX action=stats|rebuild|drop`
  maintains the index.
- **Owners:** [`graph_recall.go`](src/graph_recall.go); node-write hooks in
  [`graph.go`](src/graph.go) (`handleGraphNodeSet`, `handleGraphNodeDel`, `graphEnsureNode`).
  **Tests:** [`graph_recall_test.go`](src/graph_recall_test.go).
- **Constraints:** every walk is bounded by `branch_limit` (per node/direction) and `budget`
  (hydrated edges); exhausting either answers `truncated=1` rather than stalling. `hops` caps at 6.
  Term-index maintenance on write is switchable with `CHEETAH_GRAPH_TERM_INDEX=0`; a database written
  with it off (or created before this feature) needs `GRAPH_TERM_INDEX action=rebuild` before free-text
  seeds resolve — exact ids and synonym edges work regardless.
- **Gaps:** no consolidation (recall never writes back what it discovered), no async variant, and the
  index weighs every token equally — all three are open items in [`NEXT_STEPS.md`](NEXT_STEPS.md).

### Edge uncertainty + ambiguity — Shipped

- **Behavior:** an edge carries `confidence` (0–1), `modality` (a word on the ordered scale
  `ruled_out < unlikely < possible < probable < certain`, with aliases) and `ambiguity` (the group of
  mutually exclusive readings it belongs to). Either notation sets both. `GRAPH_AMBIGUITY_SET/GET/
  RESOLVE` write, read and collapse a whole alternative set; `WHERE` gained `edge.confidence`
  (number *or* word), `edge.modality` (word for `=`/`!=`, rank for the ordering operators) and
  `edge.ambiguity`.
- **Owners:** [`graph_uncertainty.go`](src/graph_uncertainty.go) + the record/upsert/predicate paths in
  [`graph.go`](src/graph.go). **Tests:** [`graph_uncertainty_test.go`](src/graph_uncertainty_test.go).
- **Constraints:** an edge that declares nothing reads as `certain`; belief fields **persist across a
  partial upsert** (unlike `weight`, which resets to 1.0) and are cleared with `confidence=-`; a group
  is normalized only when written through the `GRAPH_AMBIGUITY_*` commands and is anchored to one
  node's adjacency.

### Prediction tables + context matrices — Shipped (GPU path simulated)

- **Behavior:** `PREDICT_SET/QUERY/TRAIN/CTX/INHERIT(+batch/async)/BACKEND/BENCH` over fixed-byte
  tables with context-matrix weighting and multi-window merges.
- **Owners:** [`prediction_table.go`](src/prediction_table.go), [`prediction_manager.go`](src/prediction_manager.go),
  [`predict_jobs.go`](src/predict_jobs.go).
- **Constraints:** the "GPU" backend is `webgpu-simulated` (CPU fan-out), not a real WebGPU binding.
  **Gaps:** driving `PREDICT_TRAIN` from ingest is a *client-side* TODO, not a server gap.

### Cluster fork scheduling — Shipped topology / partial migration

- **Behavior:** `CLUSTER_UPDATE`/`CLUSTER_STATUS`/`FORK_ASSIGN`/`CLUSTER_MOVE`/`CLUSTER_GOSSIP` place
  forks on nodes; `CLUSTER_MOVE` builds a `forkTransferPayload` (trie + prediction entries) and gossips
  it to peers.
- **Owners:** [`cluster_scheduler.go`](src/cluster_scheduler.go), [`cluster_gossip.go`](src/cluster_gossip.go),
  cluster handlers in [`database.go`](src/database.go).
- **Gaps:** forced-assignment **overrides are not persisted** across restarts; only topology survives.

<a id="pitfall-vestigial-references"></a>
### Pitfall: vestigial parent-monorepo references

- **Symptom / wrong assumption:** docs mention `cd cheetah-db`, `cheetah-db/AGENTS.md` (or its old
  name `AI_REFERENCE.md`), `src/train.py`, `src/helpers/char_tree_similarity.py`, `DBSLM_BACKEND`, or
  SQLite fallback — an agent assumes those paths exist here and wastes time looking.
- **Cause:** this repo was extracted from a `cheetah-db/` subdirectory of a Python project; prose docs
  ([`CONCEPTS.md`](CONCEPTS.md), `studies/*`) still speak from that vantage.
- **Safe pattern:** treat all Python/SQLite/`DBSLM_*`/`cheetah-db/`-subpath references as an external
  client. The server here is the whole product; there is no nested `cheetah-db/`, and a
  `cheetah-db/AGENTS.md` path in those notes means *this* file at the repository root.
- **`src/` is a name collision, not a match.** This repo now keeps its **Go** server under
  [`src/`](src/); the client project also has a `src/`, holding Python. A `src/train.py`, `src/db_slm/…`
  or `src/helpers/…` path is always the external client — nothing under `src/` here is Python.

<a id="pitfall-doc-command-drift"></a>
### Pitfall: documented commands that don't exist

- **Symptom:** scripting `RECYCLE <value_size>` or a standalone TCP `CURSOR <token>` returns
  `ERROR,unknown_command`; `EDIT:<size> <key> <payload>` does too.
- **Cause:** older revisions of [`README.md`](README.md) listed the first two, and its synopsis gave
  `EDIT` an `INSERT`-style `:<size>` suffix. [`ExecuteCommand`](src/database.go) implements none of
  them: `CURSOR` exists only as a *clause inside* `GRAPH_QUERY`, and `EDIT` is matched by exact
  equality (only `INSERT` is matched by prefix), taking its size from the payload. All three are
  corrected in the current README.
- **Safe pattern:** the authoritative command list is the `ExecuteCommand` switch plus the front-end
  `DATABASE`/`RESET_DB`/`EXIT`. Continue a scan with `PAIR_SCAN <prefix> <limit> <cursor>`.

<a id="pitfall-jump-nodes"></a>
### Pitfall: prefix overlaps in the trie (jump collapse/split)

- **Symptom:** a key that shares a prefix with another is stored but unreadable, or a delete takes
  siblings (or a whole subtree) with it. Four such defects existed and are now fixed and pinned by
  [`TestJumpTerminalOverlaps`](src/pair_adaptive_test.go) +
  [`TestPairSetGetDeleteRoundTrip`](src/pair_scan_test.go): a strict-prefix key being rejected, a
  terminal beside a jump reading back as not-found, delete dropping a sibling, and — at stride 2
  only — `PAIR_GET` missing keys parked behind an unaligned branch.
- **Causes, all one rule broken:** terminal, child and jump are *independent* flags, and a node's
  chunks do not always start where the stride says.
  - A key can end on an entry that also carries a child or a jump, so **every** path must test "does
    the key end here?" **before** following the jump (`lookupPairAt`, `insertPairAt`,
    `deletePairAt`); a jump beside a terminal only holds longer keys.
  - Splitting a jump re-inserts the old tail with `insertSuffixWithContinuation`, which for an
    odd-length tail parks a **1-byte branch with a continuation** — from there down the child node
    starts at an odd offset. Readers must fall back to that short branch when the stride-aligned one
    is empty; [`selectPairBranch`](src/database.go) is the single place that decides, and lookup,
    insert, delete and prefix resolution all go through it.
  - The second return value of `deletePairAt`/`deleteWithinJump` means "**this node** is now empty"
    (`PairTable.IsEmpty`), never "this entry is now empty" — the caller deletes the child table on
    it, so the entry-level answer wipes live siblings.
  - `collectSingleBranchPath` refuses to collapse a path through an entry that is terminal *and*
    continues: a jump carries exactly one terminal, the one at its end.
- **Safe pattern:** never infer one flag from another; route every key walk through
  `selectPairBranch`; exercise prefix-sharing insert+get+delete cycles on **both** strides when
  touching [`database.go`](src/database.go) trie mutation or [`jump_store.go`](src/jump_store.go).
  **Status:** working, with randomized cross-stride coverage; regression risk stays high.

<a id="pitfall-graph-direction"></a>
### Pitfall: `GRAPH_QUERY` direction mirroring

- **Symptom:** `MATCH (id='luna')<-[:owns]-(*)` returned `matches=0` at one hop while
  `GRAPH_NEIGHBORS id=luna direction=in type=owns` returned the edge — and the *same* query with
  `HOPS 1..2` worked, because only the single-hop path was wrong.
- **Cause:** on `direction=in` the scan prefix is the target's `adj/in` index, so every returned edge
  has `To == plan.Left.ID` and `From ==` the far endpoint. `executeGraphQuerySingleHop` mirrored
  `rightIDFilter` and the right-hand node id but still matched `plan.Left` against `edge.From`, which
  an ID-anchored left pattern can never satisfy.
- **Safe pattern:** the `MATCH` patterns are **positional** (left = anchor, right = far endpoint) while
  predicates are **edge-oriented** (`from.id`/`to.id` are the record's own fields). Any branch that
  reads an endpoint must derive both from `plan.Direction` in one place:
  `leftNodeID, rightNodeID := edge.From, edge.To` then swap when `direction == "in"`. Pinned by
  [`TestGraphQueryReverseSingleHopMatchesInAdjacency`](src/graph_test.go), which asserts the reverse
  single hop equals the `GRAPH_NEIGHBORS direction=in` result.

<a id="pitfall-cli-tcp-parity"></a>
### Pitfall: CLI/TCP command divergence

- **Symptom:** a connection-scoped command works over TCP but not the CLI (or vice-versa).
- **Cause:** `DATABASE`/`RESET_DB` are duplicated in [`main.go`](src/main.go) `runCLI` and
  [`server.go`](src/server.go) `handleConnection`; everything else routes through `ExecuteCommand`.
- **Safe pattern:** put new logic in `ExecuteCommand` whenever possible; if it must be
  connection-scoped, edit both front-ends together.

---

## Interface ownership map

The protocol surface is the `ExecuteCommand` switch in [`database.go`](src/database.go) (authoritative)
plus the two front-end handlers. There is no generated API manifest.

| Command(s) | Owner |
| --- | --- |
| `DATABASE`, `RESET_DB`, `EXIT` (connection-scoped) | [`main.go`](src/main.go) `runCLI`, [`server.go`](src/server.go) `handleConnection`, [`engine.go`](src/engine.go) |
| `INSERT`, `READ`, `EDIT`, `DELETE` | [`commands.go`](src/commands.go) |
| `PAIR_SET(_HIDDEN)`, `PAIR_GET`, `PAIR_DEL`, `PAIR_PURGE` | [`commands.go`](src/commands.go) |
| `PAIR_SCAN`, `PAIR_SUMMARY` | [`database.go`](src/database.go) (`PairScanWithOptions`, `PairSummaryWithOptions`) |
| `PAIR_REDUCE(_ASYNC/_STATUS/_FETCH)` | [`database.go`](src/database.go) + [`reducers.go`](src/reducers.go) + [`reduce_jobs.go`](src/reduce_jobs.go) |
| `GRAPH_NODE_*`, `GRAPH_EDGE_*`, `GRAPH_NEIGHBORS`, `GRAPH_DEGREE`, `GRAPH_NEIGHBOR_TYPES`, `GRAPH_QUERY` | [`graph.go`](src/graph.go) |
| `GRAPH_AMBIGUITY_SET/GET/RESOLVE` | [`graph_uncertainty.go`](src/graph_uncertainty.go) |
| `GRAPH_RECALL`, `GRAPH_SIMILAR`, `GRAPH_TERM_INDEX` | [`graph_recall.go`](src/graph_recall.go) |
| `PREDICT_*` | [`prediction_table.go`](src/prediction_table.go), [`prediction_manager.go`](src/prediction_manager.go), [`predict_jobs.go`](src/predict_jobs.go) |
| `CLUSTER_UPDATE/STATUS/MOVE/GOSSIP`, `FORK_ASSIGN` | [`cluster_scheduler.go`](src/cluster_scheduler.go), [`cluster_gossip.go`](src/cluster_gossip.go) |
| `SYSTEM_STATS`, `LOG_FLUSH`, `FILE_CHECKPOINT` | [`database.go`](src/database.go), [`resource_monitor.go`](src/resource_monitor.go), [`logger.go`](src/logger.go), [`file_manager.go`](src/file_manager.go) |

Full argument syntax and response grammar live in [`README.md`](README.md#command-reference); verify
any command against the switch before relying on the README.

---

## Build, run, test, and debug

Prerequisite: Go 1.24+ (module declares `go 1.24.4`; developed against the 1.25 toolchain). No CGO,
no external services.

Build the server (produces the untracked `cheetah-server`):

```bash
bash build.sh            # release build (-s -w, -trimpath); --clean/--debug/--verbose available
```
```bash
go build -o cheetah-server ./src    # equivalent plain build
```

Build everything including the demo/gold targets:

```bash
go build ./...
```

Run:

```bash
./cheetah-server                 # interactive CLI + TCP listener on 0.0.0.0:4455
```
```bash
CHEETAH_HEADLESS=1 ./cheetah-server   # TCP only, no CLI (use under screen/tmux)
```

Test, vet, format:

```bash
go test ./src                    # unit tests (fast; benchmark stays gated off)
```
```bash
go test -race ./src              # required for the ManagedFile handle-lifecycle test
```
```bash
go vet ./... && gofmt -l .       # both silent on a clean tree
```

Throughput benchmark (writes a client-rotated log; long-running):

```bash
CHEETAHDB_BENCH=1 go test -run TestCheetahDBBenchmark -count=1 -v ./src
```

Adaptive pair-index comparison (storage + throughput, adaptive vs always-dense, both strides;
~4 min at the default 20k keys, tune with `CHEETAHDB_ADAPTIVE_BENCH_KEYS`):

```bash
CHEETAHDB_ADAPTIVE_BENCH=1 go test -run TestAdaptivePairIndexBenchmark -count=1 -v -timeout 1800s ./src
```

Cross-compile (Windows builds cleanly):

```bash
GOOS=windows go build -o cheetah-server.exe ./src
```

Run the NELL graph demo against a **running** server:

```bash
go run ./demo/graph-nell --host 127.0.0.1 --port 4455 --database graph_nell_demo --reset-db \
  --dataset studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv
```

Run the automated end-to-end graph pipeline test instead — it builds and boots the server itself
(no manual server, no big dataset; it generates a small synthetic one) and asserts on the result:

```bash
CHEETAH_NELL_E2E=1 go test -run TestGraphNELLEndToEnd -count=1 -v ./demo/graph-nell
```

Debug: set `CHEETAH_LOG_LEVEL=3` (or `debug`) for command/reducer/trie traces; call `SYSTEM_STATS`
for live CPU/IO/cache metrics; `LOG_FLUSH` to dump the in-memory log ring; `FILE_CHECKPOINT` to force
a mid-run flush.

**No release/packaging/migration tooling exists** in this repo beyond `build.sh`; distribution is the
single binary.

### Configuration reference

Settings resolve as: `config.ini` (or `CHEETAH_CONFIG_PATH`) → environment overrides → normalize.

Environment variables read by the server (all verified in-tree):

- **Server:** `CHEETAH_CONFIG_PATH`, `CHEETAH_LISTEN_ADDR`, `CHEETAH_DATA_DIR`, `CHEETAH_DEFAULT_DB`,
  `CHEETAH_TCP_KEEPALIVE_SECONDS`, `CHEETAH_MAX_PAIR_TABLES`, `CHEETAH_PAIR_INDEX_BYTES`,
  `CHEETAH_HEADLESS`, `CHEETAH_LOG_LEVEL`.
- **Adaptive pair index:** `CHEETAH_ADAPTIVE_PAIR_INDEX` (default on; accepts `1/0`, `true/false`,
  `yes/no`, `on/off`), `CHEETAH_PAIR_LIST_MAX_BYTES` (default 4096),
  `CHEETAH_PAIR_LIST_MAX_FILL_PERCENT` (default 0 = off). All only apply when a database is
  **created** — thereafter `pairs/format.dat` wins ([`pair_format.go`](src/pair_format.go)).
- **Payload cache:** `CHEETAH_PAYLOAD_CACHE_ENTRIES`, `CHEETAH_PAYLOAD_CACHE_MB`,
  `CHEETAH_PAYLOAD_CACHE_BYTES` (any `=0` disables caching).
- **Managed-file cache** ([`file_manager.go`](src/file_manager.go)): `CHEETAH_FLUSH_WORKERS`,
  `CHEETAH_CACHE_IDLE_SECONDS`, `CHEETAH_CACHE_FORCE_SECONDS`, `CHEETAH_CACHE_SWEEP_SECONDS`,
  `CHEETAH_CACHE_STATS_SECONDS`, `CHEETAH_CACHE_PRESSURE_HIGH`, `CHEETAH_CACHE_PRESSURE_LOW`,
  `CHEETAH_CACHE_WRITE_WEIGHT`, `CHEETAH_CACHE_READ_WEIGHT`.
- **Graph term index** ([`graph_recall.go`](src/graph_recall.go)): `CHEETAH_GRAPH_TERM_INDEX`
  (default on; `0/false/no/off/disable(d)` turns off the automatic maintenance on node write —
  `GRAPH_TERM_INDEX action=rebuild` indexes regardless, since it is an explicit request).
- **Prediction:** `CHEETAH_PREDICT_DEEPEN`, `CHEETAH_PREDICT_FLUSH_MILLIS`,
  `CHEETAH_PREDICT_PURGE_THRESHOLD`, `CHEETAH_PREDICT_MERGER`.
- **Cluster:** `CHEETAH_NODE_ID`, `CHEETAH_TRACK_STANDALONE_FORKS`.
- **Benchmark (test only):** `CHEETAHDB_BENCH`, `CHEETAHDB_BENCH_DURATION`,
  `CHEETAHDB_BENCH_WORKERS`, `CHEETAHDB_BENCH_VALUE_SIZE`.
- **Demo end-to-end (test only):** `CHEETAH_NELL_E2E=1` gates
  [`TestGraphNELLEndToEnd`](demo/graph-nell/main_test.go), which builds + boots the server and drives
  the demo over TCP. Read by the test harness, **not** by the server.

`DBSLM_*` and `CHEETAH_REDUCE_*`/`CHEETAH_PAIR_REGISTER_*`/`CHEETAH_PREDICT_INHERIT_ASYNC` variables
seen in old docs are **client-side**; the server does not read them.

---

## Test ownership map

| Subsystem / contract | Focused test |
| --- | --- |
| Size-changing `EDIT` relocates + recycles | [`TestEditResizesValues`](src/benchmark_test.go) |
| Adaptive container ≡ always-dense (set/get/scan/delete, both strides) | [`TestAdaptiveMatchesFixed`](src/pair_adaptive_test.go) |
| LIST→DENSE densify + ordered `PopulatedBranchIndices` over a sparse body | [`TestPairTableListToDense`](src/pair_adaptive_test.go) |
| LIST insert/replace/delete ordering + count | [`TestPairTableListDelete`](src/pair_adaptive_test.go), [`TestAdaptivePairListLifecycle`](src/pair_adaptive_test.go) |
| Legacy-directory guard + format marker pinned across reopen | [`TestPairFormatGuardRejectsLegacy`](src/pair_adaptive_test.go), [`TestPairFormatPinnedAcrossReopen`](src/pair_adaptive_test.go) |
| Adaptive vs fixed storage/throughput comparison | [`TestAdaptivePairIndexBenchmark`](src/pair_adaptive_bench_test.go) (`CHEETAHDB_ADAPTIVE_BENCH=1`) |
| Prefix overlaps: terminal beside a jump, sibling-safe delete, strict-prefix keys | [`TestJumpTerminalOverlaps`](src/pair_adaptive_test.go) |
| Set/get/delete/scan round trip over overlapping keys, both strides | [`TestPairSetGetDeleteRoundTrip`](src/pair_scan_test.go) |
| Prefixes ending mid-branch at stride 2 (scan + summary) | [`TestPairScanMidChunkPrefix`](src/pair_scan_test.go), [`TestPairScanPrefixParityAcrossStrides`](src/pair_scan_test.go) |
| `PAIR_SUMMARY` completes with a saturated task queue | [`TestPairSummaryDrainsSaturatedQueue`](src/pair_scan_test.go) |
| Cursor pagination returns every key once, any page size | [`TestPairScanCursorPagination`](src/pair_scan_test.go) |
| `ManagedFile` handle + sector-cache lifecycle under concurrent IO (`-race`) | [`TestManagedFileConcurrentHandleLifecycle`](src/file_manager_test.go) |
| Repeated shutdown: `Database.Close` / `Engine.Close` are idempotent | [`TestDatabaseCloseIsIdempotent`](src/lifecycle_test.go), [`TestEngineCloseIsIdempotent`](src/lifecycle_test.go) |
| Throughput / concurrency under load | [`TestCheetahDBBenchmark`](src/benchmark_test.go) (gated by `CHEETAHDB_BENCH=1`) |
| Graph edge lifecycle + query | [`TestGraphEdgeLifecycleAndQuery`](src/graph_test.go) |
| `GRAPH_QUERY` parser rules | [`TestParseGraphQueryRules`](src/graph_test.go) |
| Batch edge upsert (+ continue-on-error) | [`TestGraphEdgeSetBatchAndDegree`](src/graph_test.go), [`TestGraphEdgeSetBatchContinueOnError`](src/graph_test.go) |
| Multi-hop bounds + cost limits | [`TestGraphQueryMultiHopBoundsAndCost`](src/graph_test.go) |
| Reverse (`<-[:t]-`) single hop matches `direction=in` adjacency | [`TestGraphQueryReverseSingleHopMatchesInAdjacency`](src/graph_test.go) |
| Confidence/modality round trip, default `certain`, persistence across partial upserts | [`TestGraphConfidence*`](src/graph_uncertainty_test.go) |
| Modality predicates compare by rank; ambiguity groups set/get/resolve/drop and share distribution | [`TestGraphModalityPredicateOrdering`/`TestGraphAmbiguity*`](src/graph_uncertainty_test.go) |
| Batch edge upsert carries confidence (number or word) and group tags | [`TestGraphEdgeBatchCarriesUncertainty`](src/graph_uncertainty_test.go) |
| Multi-seed convergence outranks single-seed neighbours; `min_sources=2` keeps only bridges | [`TestGraphRecallConvergenceAcrossSeeds`](src/graph_recall_test.go) |
| Novelty prefers a distant multi-seed node over a near single-seed one | [`TestGraphRecallNoveltyPrefersDistantConvergence`](src/graph_recall_test.go) |
| Free-text seeds resolve lexically + through alias edges; `expand=exact` disables both | [`TestGraphRecallResolvesLexicalTermsAndSynonyms`](src/graph_recall_test.go) |
| Recall precision gates on declared edge confidence; `via` carries the modality | [`TestGraphRecallHonoursPrecisionAndConfidence`](src/graph_recall_test.go) |
| Exhausted recall budget answers `truncated=1` on one line instead of stalling | [`TestGraphRecallBudgetDegradesInsteadOfStalling`](src/graph_recall_test.go) |
| Distributional similarity: shared neighbours and shared id words | [`TestGraphSimilarSharesContextAndWords`](src/graph_recall_test.go) |
| Term index maintained on write, dropped with the node, rebuildable, switchable | [`TestGraphTermIndexLifecycle`](src/graph_recall_test.go) |
| `LOG_FLUSH` answers on one line and leaves the next response aligned | [`TestLogFlush*`](src/logger_test.go) |
| Edge-property secondary index | [`TestGraphPropertySecondaryIndexAndPredicate`](src/graph_test.go) |
| Graph reducers (degree/triangle/pagerank_seed) | [`TestGraphReducersDegreeTriangleAndPageRankSeed`](src/graph_test.go) |
| Graph-NELL demo eval/loader math (AUC/AP/P@K, models, split, loader) | [`TestRankingMetrics`/`TestBuildModels`/`TestLoadNELLEdges`/…](demo/graph-nell/main_test.go) |
| End-to-end graph pipeline over TCP (build+boot server, ingest→query→predict, gated) | [`TestGraphNELLEndToEnd`](demo/graph-nell/main_test.go) (`CHEETAH_NELL_E2E=1`) |

**Known test gaps:** no focused coverage for prediction-table train/inherit, cluster
scheduling/gossip, the payload cache, or cursor pagination edge cases. Jump split/promote cycles are
now exercised indirectly by the randomized overlap tests, not by a targeted unit test. Add tests alongside changes in those areas. The graph subsystem now has both in-process unit
coverage ([`graph_test.go`](src/graph_test.go)) and a gated real-execution path over TCP
([`demo/graph-nell/main_test.go`](demo/graph-nell/main_test.go)).

---

## Data, security, privacy, and compatibility boundaries

- **Canonical vs. derived:** everything under `cheetah_data/<db>/` is canonical database state written
  by the engine; the `cheetah-server` binary, benchmark logs, and demo `reports/` are derived. Only
  `cheetah_data/*` and the binary are `.gitignore`d.
- **On-disk compatibility:** the byte formats in [`types.go`](src/types.go) and the `CHPREDTB` prediction
  format are unversioned wire contracts. The pair-node container **is** versioned (`"CHPT"` header +
  `PairFormatVersion`) and pinned per database by `pairs/format.dat` (`"CHPF"`). Legacy migrations
  that *do* exist: per-file `.jump` → `pair_jumps/*.bin` ([`jump_store.go`](src/jump_store.go)), and
  prediction `.json` → binary ([`prediction_table.go`](src/prediction_table.go)). The pre-header pair
  format has **no** migration: such a directory is refused at open and must be rebuilt with
  `RESET_DB`. New format changes need an explicit version byte and a read-old/write-new path.
- **Trust / validation:** the protocol is unauthenticated plaintext over TCP on `0.0.0.0:4455` by
  default. There is **no auth, TLS, or access control** — bind it to a trusted network or loopback.
  `CLUSTER_GOSSIP` accepts base64 JSON from peers and applies fork payloads; only enable clustering
  among trusted nodes.
- **Secrets:** none are stored or expected. Do not add credentials to `config.ini` or this handbook.
- **Resource limits:** open pair-table handles are capped (`CHEETAH_MAX_PAIR_TABLES`/`RLIMIT_NOFILE`);
  caches shed under memory pressure. New unbounded caches or handle pools violate the degradation
  principle.

---

## Current status and known gaps

### Shipped

- KV store, pair-trie namespaces with jump-node compression, cursored `PAIR_SCAN`/`PAIR_SUMMARY`/
  `PAIR_PURGE`.
- Reducers: `counts`/`probabilities`/`continuations` + graph `degree`/`triangle`/`pagerank_seed`,
  sync and async.
- Graph store with batch upsert and bounded multi-hop `GRAPH_QUERY` incl. edge-property secondary
  index.
- Associative recall: multi-seed `GRAPH_RECALL` with noisy-OR convergence, `GRAPH_SIMILAR`, and the
  `\x05gt:` lexical term index behind `GRAPH_TERM_INDEX`.
- Prediction tables with context-matrix train/query/inherit and CPU merge path.
- Managed-file layer, payload cache, resource monitor, `SYSTEM_STATS`/`LOG_FLUSH`/`FILE_CHECKPOINT`.
- Cluster topology registration, fork assignment, gossip, and `CLUSTER_MOVE` payload transfer.

### Experimental / scaffold

- **`webgpu-simulated` prediction backend** — CPU fan-out standing in for real WebGPU
  ([`prediction_table.go`](src/prediction_table.go)).
- **[`gold/basic.go`](gold/basic.go)** — reference prototype with stubbed `Read`/`Edit`; not wired to
  the server.
- **Async job managers** ([`reduce_jobs.go`](src/reduce_jobs.go), [`predict_jobs.go`](src/predict_jobs.go)) are
  in-memory only; jobs vanish on restart.

<a id="known-gaps"></a>
### Known gaps

- **Cluster fork overrides are not persisted** — `CLUSTER_MOVE` reassignments are lost on restart
  ([`cluster_scheduler.go`](src/cluster_scheduler.go) `load`). First open item in
  [`NEXT_STEPS.md`](NEXT_STEPS.md) after the roadmap items that shipped.
- **`PAIR_PURGE` is racy** — `purgePairEntries` ([`commands.go`](src/commands.go)) deletes entries from
  parallel goroutines while `deletePairAt` mutates the trie unlocked. Fails ~6 runs in 8 on 8 keys
  under one prefix, either with `remove pairs/<n>.table: no such file or directory` /
  `jump reload limit exceeded`, or silently, reporting `purged=N` with entries still scannable.
  Full repro in [`NEXT_STEPS.md`](NEXT_STEPS.md).
- **`PREDICT_*` error responses omit the `ERROR,` prefix** — the handlers return `err.Error()` raw
  (`inherit_sources_missing`), breaking the otherwise universal `SUCCESS`/`ERROR,` classification.
- **Command-surface redundancy** — 51 dispatcher commands, of which the async trios, the `_BATCH`
  forms, the four hydration levels of one adjacency/trie walk, and the five erasure verbs are the
  same operation in different envelopes. Documented per command in
  [`README.md`](README.md#telling-the-look-alikes-apart); the micro-command + alias plan is in
  [`NEXT_STEPS.md`](NEXT_STEPS.md).
- **Thin tests** for prediction, cluster, and the payload cache (see test gaps above).

### Near-term priorities (from [`NEXT_STEPS.md`](NEXT_STEPS.md))

1. Recall consolidation: persist repeatedly co-activated pairs as derived edges (plus a forgetting
   rule), an async recall variant, and frequency-weighted / misspelling-tolerant term matching.
2. Persist cluster fork overrides + gossip snapshots so reassignments survive restarts.
3. Ship full fork *data* (not just the current metadata/payload subset) when reassigning shards.
4. Optional reducer digests (entropy/CDF/rolling hashes) and trie-level rolling-hash mirrors.

---

## Task start and handoff checklist

**Before editing:**

1. Read the file's subsection above and the [critical contracts](#critical-implementation-contracts)
   it touches; check `git status` and preserve unrelated changes.
2. If touching the protocol, read [`ExecuteCommand`](src/database.go) — it is the command source of truth,
   not [`README.md`](README.md).
3. If touching on-disk formats/`types.go`, plan a format version + migration; do not edit constants in
   place.
4. For trie/jump changes, walk [`database.go`](src/database.go) mutation paths and
   [`jump_store.go`](src/jump_store.go); for graph changes, read [`graph.go`](src/graph.go) and its tests.

**Before committing:**

1. `go build ./...`, `go vet ./...`, `go test ./src` pass; run `CHEETAHDB_BENCH=1 go test -run
   TestCheetahDBBenchmark` if you touched hot paths.
2. `gofmt -w` the files you changed.
3. Update this handbook's affected sections (source tree, interface map, features, status, config) in
   the same commit; record new roadmap work in [`NEXT_STEPS.md`](NEXT_STEPS.md).
4. Confirm no `cheetah_data/`, binaries, or logs are staged.

<a id="update-triggers"></a>
### Update triggers

| Change made | Required handbook update |
| --- | --- |
| Add/move/rename/delete a `.go` file | Add/rewrite/remove its subsection in [source tree](#linked-source-tree-and-file-reference); fix the [interface map](#interface-ownership-map). |
| Add/change a command | Update `ExecuteCommand` note, [interface map](#interface-ownership-map), and (if connection-scoped) both front-ends. |
| Add/change a reducer | Update [`reducers.go`](src/reducers.go) subsection + reducer feature entry. |
| Add/change a feature | Update [Features](#features-and-recurring-development-pitfalls) with status/owners/tests/gaps. |
| Add a config key / env var | Update [`config.go`](src/config.go) subsection + [Configuration reference](#configuration-reference). |
| Change an on-disk format | Update [contracts](#critical-implementation-contracts) + [data boundaries](#data-security-privacy-and-compatibility-boundaries) + migration note. |
| Add/move a test | Update [test ownership map](#test-ownership-map) and known gaps. |
| Fix/discover a recurring bug | Add or consolidate a pitfall entry; keep active defects under [Known Gaps](#known-gaps). |
| Implement a `NEXT_STEPS.md` item | Move it to Shipped only after code + test verification. |

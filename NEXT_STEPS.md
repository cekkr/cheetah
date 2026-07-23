# To do:
- **Key allocation: shard the counter only if inserts actually contend.** Reuse of deleted keys shipped (see *Done*); this is the remaining half.
  Sharding the counter to avoid conflicts under massive concurrent setting (proposed: 12 high bits, 6 for the table and 6 for the thread, reassigned in real time under load rather than pinned for the run) cannot be applied to the key as it stands: with the top 12 bits set the row offset reaches 2^64 × 9 B ≈ 144 EiB, past what a single sparse file can address on APFS or ext4. The workable form shards the file *with* the key — `key = lane<<52 | seq`, stored in `main_keys_<lane>.table` at offset `seq * 9` — which keeps addressing O(1) and gives each lane its own file lock and its own free list. Two constraints follow from letting the lane bits float instead of being a fixed identity: the sequence counter must belong to the **lane**, not to the thread, so a thread that takes over lane 7 continues lane 7's count (otherwise two holders of the same 12-bit value collide, which is the exact failure the scheme exists to prevent) — meaning a lane needs a single-holder lease, not just a label; and the split must stay a power of two per level for the bits to remain decodable. Measure before building it, though: the counter is one atomic add and is not the bottleneck. The serialization in the insert path is `MainKeysTable`'s 1024 FNV-striped locks and the single `*os.File` behind them, so the win would come from per-lane files, not per-lane counters.
  **The shape to build, when it is built.** Off by default, behind a per-database switch (`sharded_key_slots`, plus `key_slot_bits` defaulting to 12 — config/env/override, same three-way pattern as `adaptive_pair_index`). A key becomes `[slot: n bits][sequence: 64-n bits]`, and *within a slot the sequence increments densely from 0*, popping that slot's free list first, so no row of any file is left unused — the density that the current single counter gives globally is preserved per slot rather than abandoned. The slot value itself is **chosen arbitrarily, not assigned as a fixed table/thread id**, and may be traded for another in the middle of a run as load moves; nothing downstream may read meaning into it. At n = 12 that is 4,096 slots of 2^52 keys each.
  Four things this has to get right. **A random slot value cannot simply be drawn** — with 4,096 slots and 64 concurrent writers, the birthday bound puts two writers on the same slot about 39% of the time, and two writers incrementing the same slot's counter privately is precisely the collision the scheme exists to prevent. Drawing must therefore mean *claiming a random free slot from a pool* (single holder, released on handover), which is the lease from the paragraph above with a random probe instead of a round-robin one. **Each slot needs its own `main_keys_<slot>.table` and its own `main_keys_<slot>.recycle.table`** — the row offset is `sequence * MainKeysEntrySize` inside its own file, which is what keeps addressing O(1) and is the only way the top bits can be set at all. **Keys stop being small dense integers**: no disk is wasted, but the *numbers* spread across 2^64, so anything that assumes a compact key space — client-side arrays, `INSERT` responses read as counters, the eventual `loadHighestKey` equivalent — has to be checked first. **It is a creation-time decision, not a runtime one**: an existing database's keys are offsets into one file and cannot be reinterpreted, so the flag and `n` belong in a pinned marker like `pairs/format.dat`, refused on reopen with different values rather than silently honoured.
- **Hippocampus, consolidation step.** `GRAPH_RECALL` reads; nothing writes back. Record which nodes co-activate across repeated recalls and, past a threshold, persist the pair as a derived edge (`recalled_with`, low confidence, `props.derived=1`) so an association discovered twice becomes cheap the third time. Needs a decay/forgetting rule too, or the graph fills with its own echoes.
- **Async recall for wide fan-outs.** `GRAPH_RECALL` is synchronous and budget-bounded: on a hub-heavy graph a deep recall returns `truncated=1` instead of the full picture. Add `GRAPH_RECALL_ASYNC/_STATUS/_FETCH` on the `reduce_jobs.go` model so a wide sweep can run past a single round-trip.
- **Term index: frequency weighting and misspellings.** Every matching token weighs the same today, so a generic word (`concept`, `city`) pulls as hard as a rare one, and the per-token candidate scan is capped blind. Keep a per-token count in the index to weigh matches by rarity (IDF), and add a trigram or edit-distance fallback so a misspelled seed still lands.
- **Recall over the episodic tier.** Seeds resolve to graph nodes only. The sentences that produced those nodes live in ordinary pair namespaces (`studies/GRAPH_LLM.md` §2.1) and are invisible to recall — resolve seeds to prefixes there too, so a recall can return the source sentence beside the edge.
- **Learned decay.** `decay` is one number for every relation type. The prediction tables already learn which relations lead somewhere useful; feed that back so `has_breed` and `mentioned_near` do not spread the same amount.
- **Optional: per-node stride widening.** The adaptive container already removes the memory cost of a wide stride. The remaining half of "dynamic indexing" is letting an individual hot node widen its stride (1 → 2 bytes) to halve subtree depth, which requires re-chunking that node's children and jump segments — high regression risk in the trie mutation paths, so it was deliberately deferred.
- Persist cluster fork overrides and gossip snapshots to disk so scheduler reassignments survive restarts and peers can recover state after downtime.
- Extend the cluster messenger to ship actual fork data (trie payloads + prediction tables) when reassigning shards, not just metadata.
- Add optional reducer digests (entropy / CDF / rolling hashes) and trie-level rolling-hash mirrors for fast Top-K / similarity scans.
- Wire the `demo/graph-nell` end-to-end execution test (`CHEETAH_NELL_E2E=1`) into CI, and optionally add a large-slice variant that runs a bounded pass over the real NELL dataset when it is present.
- Focused tests for prediction train/inherit, cluster scheduling/gossip, and the payload cache — the thinnest areas left after the trie work.

# Command surface: collapse the repeats into micro-commands, keep the names as aliases

The protocol grew one feature at a time, and it shows: 51 dispatcher commands (the count at the time
this was written; the surface has not shrunk — eleven of them are now *aliases* over `JOB` and `DEL`,
which is the point: nothing was taken away), of which a large
minority are **the same operation in a different envelope**. The cost is not code size, it is
meaning — `PAIR_SUMMARY`, `GRAPH_DEGREE` and `GRAPH_QUERY … RETURN count` all mean "count without
hydrating", and nothing in their names says so, so a caller picks by memory instead of by intent.
[`README.md`](README.md#telling-the-look-alikes-apart) now documents each command's meaning and the
look-alike distinctions; this entry is the plan to make the surface itself say them.

## What actually repeats

| Repeated shape | Commands | What differs | Status |
| --- | --- | --- | --- |
| **async envelope** | `PAIR_REDUCE_ASYNC/_STATUS/_FETCH`, `PREDICT_INHERIT_ASYNC/_STATUS/_FETCH` | nothing but the job manager they poll — two independent implementations of submit/poll/fetch with slightly different response fields | **done** — one `JOB` envelope, one manager ([`jobs.go`](src/jobs.go)); both trios are aliases |
| **batch envelope** | `GRAPH_EDGE_SET` vs `_SET_BATCH`, `PREDICT_INHERIT` vs `_BATCH` | one item inline vs `items=<base64 json[]>`; the per-item work is identical | open |
| **one walk, four hydration levels** | `PAIR_SCAN` / `PAIR_SUMMARY` / `PAIR_REDUCE`, and `GRAPH_NEIGHBORS` / `GRAPH_DEGREE` / `GRAPH_NEIGHBOR_TYPES` / `GRAPH_QUERY … RETURN count` | how much of each visited entry is read and what is emitted: names, a count, an aggregate, full payloads | open — the next step |
| **one erasure, six spellings** | `DELETE`, `PAIR_DEL`, `PAIR_PURGE`, `GRAPH_NODE_DEL cascade=`, `GRAPH_EDGE_DEL`, `RESET_DB` | only the selector (one key, one name, a prefix, a record + its indexes, a directory) — the verb is the same verb | **done** for five of six — one `DEL` with the selector in the arguments; `RESET_DB` stays front-end scoped |
| **point read / point write** | `PAIR_GET`/`PAIR_SET`(`_HIDDEN`), `GRAPH_NODE_GET`/`_SET`, `GRAPH_EDGE_GET`/`_SET`, `PREDICT_SET` | which namespace the key lives in, and whether one flag bit is set | open |
| **placement read** | `CLUSTER_STATUS` vs `FORK_ASSIGN` | whole map vs one prefix — a selector, not a command | open |

Three argument dialects sit on top of that (positional for KV/`PAIR_*`, `key=value` for
`GRAPH_*`/`PREDICT_*`/`CLUSTER_*`, a clause language for `GRAPH_QUERY`), so two commands doing the
same thing often cannot even be written the same way.

## The decomposition

Factor every command into three orthogonal parts and give each part its own token:

```text
<verb>  <target>            <view / modifiers>
GET     pairs|graph|values  ─
SET     …                   hidden=1 · items=<base64 json[]>
DEL     …                   cascade=1 · recursive=1
SCAN    …                   view=names|count|stats|types|payload:<reducer> · limit/cursor/depth
JOB     submit|status|fetch  ─  (one envelope over any bounded SCAN/SET)
```

- **`SCAN` with a `view=`** absorbs `PAIR_SCAN`/`PAIR_SUMMARY`/`PAIR_REDUCE` and
  `GRAPH_NEIGHBORS`/`GRAPH_DEGREE`/`GRAPH_NEIGHBOR_TYPES`. They already share the walk in code —
  `PairScanWithOptions`, `walkPairSummary` and `reduceWithPayload` differ in their accumulator, which
  is exactly what `view=` would select. `view=payload:<reducer>` reuses the existing reducer registry
  ([`reducers.go`](src/reducers.go)), so the extension point stays where it is.
- **`JOB`** replaces both async trios: `JOB submit <any bounded command>` → `job=<id>`, then
  `JOB status <id>` / `JOB fetch <id>`. One job manager, one progress contract, and any future
  long command (`GRAPH_RECALL_ASYNC`, first item in this file) gets an async form for free instead
  of a third copy of the same three commands.
- **`items=`** as a modifier rather than a `_BATCH` command name; the single-item form stays as the
  degenerate case.
- **`DEL` with a selector** covers all five erasures, with the scope explicit in the arguments
  (`DEL values key=<n>` vs `DEL pairs prefix=<p> payloads=1` vs `DEL graph node=<id> cascade=1`)
  instead of implied by the verb's name. This is where meaning is lost most often today.
- **Leave `GRAPH_QUERY` alone.** Its clause language is a genuinely different surface, not an
  envelope; folding it into `SCAN` would trade one clear grammar for a pile of flags. `GRAPH_RECALL`
  and `GRAPH_SIMILAR` likewise: they are distinct operations, not views.

## Recomposition as aliases — the compatibility half

Nothing above is worth breaking a client for. Every current name stays, re-expressed as an **alias**:
a legacy command name plus an argument rewriter plus a response formatter, registered at startup in
one table the way `registerDefaultReducers` registers reducers. `ExecuteCommand` resolves the alias,
runs the micro-command, and renders the legacy response.

Two constraints make the formatter non-optional:

- **Response field names are a wire contract.** `purged=`, `matches=`, `degree=`, `count=`,
  `next_cursor=`, `job=` are parsed by external clients (the Python adapter in
  `/Users/riccardo/Sources/GitHub/lmdb` reads them positionally in places). An alias must reproduce
  its legacy response byte-for-byte; only the internals change.
- **One command, one line.** The single-line rule applies to micro-commands and aliases alike;
  list-shaped output still travels as `payload=<base64>`.

Aliases also become the place where the three argument dialects are absorbed: micro-commands accept
`key=value` only, and the positional forms survive exclusively inside the alias rewriters
(`PAIR_SCAN <prefix> <limit> <cursor>` → `SCAN pairs prefix=… limit=… cursor=…`).

## Suggested order

1. ~~**`JOB`**~~ — done, see *Done*. `GRAPH_RECALL_ASYNC` is now a `jobCommand` registration away.
2. ~~**`DEL`**~~ — done, see *Done*.
3. **`SCAN` + `view=`** — the largest win and the largest regression surface (it touches the scan,
   summary and reducer walks); do it only with the pair-trie test suite green on both strides. The
   groundwork is in place: `pairReduceResponseFields` already renders through `microResponse`, so a
   `view=payload:<reducer>` result and the legacy `PAIR_REDUCE` line come out of one function.
4. **`GET`/`SET`** — mostly cosmetic once the above land; may be deferred indefinitely.

Verification for each step is the same: the alias must produce, for every legacy command in
[`README.md`](README.md#command-reference), a response identical to the current one. The
golden-response suite exists now ([`src/command_alias_test.go`](src/command_alias_test.go)) — extend
it *before* the `SCAN` refactor, with expectations captured from a server running the current code,
the way the `DEL`/`JOB` expectations were.

# Done (implemented + verified by tests — do not re-add):
- **Equal-size `INSERT`s now reserve distinct value slots before the asynchronous write.** Each
  `ValuesTable` seeds an atomic high-water mark from its file once at open and `ReserveEntry`
  advances it before `WriteAt` queues bytes. `getAvailableLocation` still consumes recycled slots
  first, then asks value tables for a fresh reservation; it no longer re-stats a file that the
  writer has not grown yet. The live failure was three acknowledged six-byte inserts all pointing
  at `EntryID 0` and all reading the last payload; after the fix they occupy three slots and remain
  distinct after close/reopen — `TestEqualSizeInsertsReserveDistinctValueSlots`
  ([`src/key_recycle_test.go`](src/key_recycle_test.go)).
- **Concurrent `PAIR_SET` no longer loses acknowledged mappings.** All pair-trie mutations now share
  `pairMutationMu`: insert and delete both split/promote jump nodes and rewrite common ancestors, so
  protecting only purge-side deletion left multi-connection writers racing. The live DB-SLM-shaped
  repro acknowledged 512 writes under `cnt:1:` but retained only 57; the regression test now
  inserts 256 shared-prefix keys concurrently and verifies every point read plus the full scan on
  both strides — `TestConcurrentPairSetSharedAncestors`
  ([`src/pair_scan_test.go`](src/pair_scan_test.go)).
- **Micro-commands, first two verbs: `JOB` and `DEL`, with every legacy name kept as an alias.** The decomposition described above now has its machinery: a micro-command registry ([`src/micro_command.go`](src/micro_command.go)), an alias table ([`src/command_alias.go`](src/command_alias.go)) and a registry of commands runnable inside `JOB` ([`src/jobs.go`](src/jobs.go)), all three built once by `ensureCommandRegistries` the way `registerDefaultReducers` builds the reducer registry. `ExecuteCommand` consults micro-commands, then aliases, then its own switch; a name that lives in the first two no longer has a switch branch, so there is exactly one implementation per command.
  **`JOB`** (`submit`/`status`/`fetch`, [`src/micro_job.go`](src/micro_job.go)) replaced the two job managers with one. `reduce_jobs.go` and `predict_jobs.go` are deleted; a `microJob` carries state, `completed`/`total`, named counters and a *structured* result (`[]microField`), which is what lets one manager serve families whose response fields differ. Ids keep their old shape with a **per-kind** sequence (`reduce_1`, `predict_inherit_1`) — a shared counter would renumber ids a client may have stored. Adding an async form to a new long command is now one `jobCommand` registration, not a third trio.
  **`DEL`** ([`src/micro_del.go`](src/micro_del.go)) is one erasure verb with the scope written out: `DEL values key=`, `DEL pairs key=`, `DEL pairs prefix=… [payloads=0|1]`, `DEL graph node=… [cascade=1]`, `DEL graph from=… to=…`. It also gained something no legacy spelling could say — `payloads=0` unlinks names from the trie and leaves the values addressable by absolute key (`PairPurgeWithOptions`). `RESET_DB` deliberately stays out: it is front-end scoped because it changes what the connection points at.
  **Compatibility is pinned, not asserted.** Expectations were captured from a server running the pre-refactor code and only then re-run against the aliases: all eleven legacy names (`DELETE`, `PAIR_DEL`, `PAIR_PURGE`, `GRAPH_NODE_DEL`, `GRAPH_EDGE_DEL`, `PAIR_REDUCE_ASYNC/_STATUS/_FETCH`, `PREDICT_INHERIT_ASYNC/_STATUS/_FETCH`) answer byte for byte what they answered before, success and error paths alike, including `DELETE` on an unwritten row still answering `ERROR,key_not_found` *and* propagating `io.EOF`, and the async fetch still mirroring the synchronous line exactly — `TestLegacyDeleteAliasesAreByteIdentical`, `TestLegacyReduceJobAliasesAreByteIdentical`, `TestLegacyPredictJobAliasesAreByteIdentical`, `TestJobIDSequencesStayPerFamily` ([`src/command_alias_test.go`](src/command_alias_test.go)), plus `src/micro_command_test.go` for the new surface; green under `-race`. Two details worth keeping: an alias formats only success and `PENDING` (error tokens are deliberately the same words, with a small `ErrorTokens` remap where `JOB` says `job_not_found` and the trios said `reduce_job_not_found`), and binary values cross into the micro dialect as `x<hex>` — it splits tokens on whitespace, so a pair key with a space survives no other way.
- **`PREDICT_*` failures now carry the `ERROR,` prefix.** `handlePredictTrain`/`handlePredictInherit` and friends returned `err.Error()` verbatim, so a failed inherit answered a bare `prediction_entry_not_found` — neither success nor error to a client classifying on the prefix, while every other family prefixed its failures. Fixed at the dispatcher boundary (`normalizeCommandResponse`, [`src/database.go`](src/database.go)) rather than handler by handler, so it holds for every present and future handler: any response opening with none of `SUCCESS`/`ERROR`/`PENDING` becomes `ERROR,<reason>`. This is client-visible, which is why it shipped with the alias work — `TestPredictFailuresCarryTheErrorPrefix`, `TestNormalizeCommandResponse`.
- Deleted keys are reused. A `DELETE` on a key in the middle of `main_keys` used to leak its row for the life of the database: only the *highest* key came back, via `findNewHighestKey` walking the counter down. Freed rows now go on a `main_keys.recycle.table` free list, which `persistPayload` pops before falling back to the counter (`nextKey`/`releaseKey`, [`src/helpers.go`](src/helpers.go)). Consequences worth knowing: `highestKey` is now a **high-water mark that never descends**, so `loadHighestKey` restores it from the file size in O(1) instead of scanning backwards over trailing tombstones — and it has to, because the old "highest live key" answer would restart the counter underneath rows the free list has already claimed and hand them out twice. `findNewHighestKey` is gone with it. A database upgrading from before the free list gets its already-deleted rows collected once, on first open, by a blocked sequential pass over `main_keys` (`seedKeyRecycle`, logged at INFO); without it the feature would only ever reclaim keys deleted *after* the upgrade. `Delete` also zeroes the row *before* pushing either free list, so a failure half-way leaks a row instead of handing out a row that is still live, and the failed-write path in `persistPayload` returns the key to the free list rather than decrementing the shared counter, which could previously hand a live row to the next inserter. — `TestDeletedKeysAreReused`, `TestKeyReuseSurvivesReopen`, `TestKeyFreeListSeededFromExistingDatabase`, `TestConcurrentInsertDeleteNeverHandsOutALiveKey` (`src/key_recycle_test.go`), green under `-race`; end-to-end over TCP, 5 deleted keys came back to the next 5 inserts and the sixth resumed at the high-water mark.
- `RecycleTable` no longer wraps at 65,536 entries, and serves both free lists. The depth counter was a `uint16` at offset 0 that `Push` incremented unchecked: entry 65,536 rolled it to zero, `Pop` declared the list empty, and 64Ki recorded slots leaked while the values table kept growing. The file is now self-describing (`"CHRL"` magic, version, entry size, `uint64` depth in a 16-byte header) and the same type backs both the value-slot list (5-byte entries) and the key list (8-byte entries). Legacy headerless files are migrated in place on open, carrying their entries over, rather than being refused or dropped. The depth is held in memory and only the 8 changed bytes are rewritten per mutation, so `Pop` costs one read plus one write instead of the two reads plus a write it used to; the ordering is chosen so a crash always leaks a record rather than handing one out twice — `TestRecycleTableHoldsMoreThanTheLegacyCap`, `TestRecycleTableMigratesLegacyFile`.
- `ValueLocationIndex.Encode` no longer truncates the table id. It wrote the id with `PutUint32(buf, TableID)` and then overwrote `buf[3:5]` with `EntryID`, so the id's low byte was destroyed and every location decoded as `TableID / 256` — 1 through 255 all read back as 0. It was latent only because the first table holds 65,536 entries: the moment a size class opened `values_<size>_1.table`, its reads were served from table 0. Now written as a plain 3-byte field, with `getAvailableLocation` refusing to allocate past `MaxValueTableID` instead of silently truncating. Databases that never passed one table per size class are byte-identical before and after — `TestValueLocationRoundTrip`.
- `PAIR_PURGE` race (lost and stranded entries): pair-trie mutation is serialized by the same
  `pairMutationMu` now used for inserts. `deletePairValue` holds it for the whole `deletePairAt`
  walk, so node collapse, jump promotion and `deletePairTable`'s `os.Remove` cannot interleave
  between goroutines deleting keys that share an ancestor. `purgePairEntries` keeps its fan-out
  unchanged and the expensive half — `Database.Delete` of the payload — remains parallel. Before:
  8 keys under one prefix failed **5 of 8 trials** over TCP; after: 24 of 24 trials clean —
  `TestPairPurgeSharedAncestors` ([`src/pair_scan_test.go`](src/pair_scan_test.go)), both strides,
  green under `-race`.
- Associative recall ("hippocampus"): `GRAPH_RECALL` spreads activation from several seed terms at once and returns every node above a requested precision with the seeds that reached it, the conceptual distance, the evidence path (weight + confidence + modality per edge) and a novelty score, so a model can pick what to explore instead of guessing the next query. Activation from different seeds combines in noisy-OR, which makes a node reached by two seeds outrank both single-seed neighbours — `min_sources=2` isolates exactly those convergences. Seeds resolve three ways: exact id, lexical overlap through the new `\x05gt:` term index (maintained on node upsert/delete, rebuildable with `GRAPH_TERM_INDEX action=rebuild`, switchable off with `CHEETAH_GRAPH_TERM_INDEX=0`), and declared synonym edges (`synonym`/`alias`/`same_as`/`aka`/`abbreviation`/`acronym`), which cost a hop but no conceptual distance. `GRAPH_SIMILAR` adds distributional similarity — same neighbours, or same words in the id. Everything is bounded by `branch_limit`/`budget` and degrades with `truncated=1` rather than stalling — `src/graph_recall.go`, `src/graph_recall_test.go`.
- Prefix-overlap trie defects (data loss), all four fixed and pinned by `TestJumpTerminalOverlaps` (`src/pair_adaptive_test.go`) and `TestPairSetGetDeleteRoundTrip` (`src/pair_scan_test.go`), both strides. Terminal, child and jump are independent flags, so lookup/insert/delete settle "does the key end here?" before following a jump; a node does not always start on a stride boundary, so all key walks pick their branch through `selectPairBranch`, which falls back to the 1-byte branch a jump split can leave behind; delete reports emptiness per node (`PairTable.IsEmpty`), not per entry, which is what made it drop live subtrees; and `collectSingleBranchPath` refuses to collapse a path through an entry that is terminal *and* continues. A prefix-free key set is no longer a requirement.
- `PAIR_SUMMARY` deadlock: `walkPairSummary` now enqueues through the same `select` + `default` → inline-drain helper as the scan walk and sizes its queue on the branch fan-out — `TestPairSummaryDrainsSaturatedQueue`.
- Cursor pagination: pages are complete, ordered and resumable. The accumulator keeps the smallest `limit+1` keys above the cursor in a bounded heap and prunes any branch whose path exceeds the largest kept, and the scan walk is ordered (smallest branch first) so it stops at the page instead of aborting at "enough results" — `TestPairScanCursorPagination`. Measured on 20k keys at stride 2: a full pagination at `limit=256` returned 3,344 of 20,000 keys, now returns all 20,000; single-page latency unchanged.
- Prefix scans at `pair_index_bytes = 2`: prefixes ending mid-branch resolve through a partial filter (`branchMatchesPartial`) — `TestPairScanMidChunkPrefix`, `TestPairScanPrefixParityAcrossStrides`.
- `ManagedFile` concurrency: `ManagedFile.file` is guarded by `handleMu` (pinned for the whole operation via `acquireHandle`), and each `sectorEntry` guards its own payload with `dataMu` — `TestManagedFileConcurrentHandleLifecycle` under `-race`.
- Idempotent shutdown: `Database.Close`, `FileManager.Close`, `Engine.Close` and `ClusterMessenger.Stop` are safe to call repeatedly — `TestDatabaseCloseIsIdempotent`, `TestEngineCloseIsIdempotent`.
- Adaptive per-node pair-trie indexing: a node whose dense array would exceed `pair_list_max_bytes` stores its entries as a sorted, binary-searched list while sparse and densifies into the direct-mapped array past that budget (optional extra cap via `pair_list_max_fill_percent`, default off); self-describing `"CHPT"` node header, per-database `pairs/format.dat` marker (authoritative on reopen, refuses legacy directories), `adaptive_pair_index` config/env/override switch, and `PopulatedBranchIndices` so enumeration is O(populated) instead of O(branch capacity) — `src/types.go`, `src/tables.go`, `src/pair_format.go`, `src/database.go`, `src/config.go`; `src/pair_adaptive_test.go`, `src/pair_adaptive_bench_test.go`. Measured on 20k keys at stride 2: `pairs/` 776.5 MiB → 4.7 MiB apparent (−99.4%), ~81–87 MiB → 4.5 MiB allocated (−94%), full-trie enumeration 1.6–1.9s → 5ms (−99.7%); insert/lookup within run-to-run noise. Narrow (1-byte-stride) nodes are excluded from the list container by the `denseBytes > pair_list_max_bytes` rule — their dense form is 2,828 B, already inside one filesystem block — so stride 1 keeps its original dense behaviour exactly.
- Multi-hop `GRAPH_QUERY` with branch pruning + early-stop cost limits — `src/graph.go`, `TestGraphQueryMultiHopBoundsAndCost`.
- Graph reducers `degree` / `triangle` / `pagerank_seed` streaming from `adj/out`/`adj/in` — `src/reducers.go`, `TestGraphReducersDegreeTriangleAndPageRankSeed`.
- Edge-property secondary indexes (`graph/idx/<prop>/<value>/...`) for `WHERE edge.props.*` — `src/graph.go`, `TestGraphPropertySecondaryIndexAndPredicate`.
- Real-execution graph pipeline test: boots the actual server over TCP and drives the full `demo/graph-nell` ingest → query → predict flow — `demo/graph-nell/main_test.go` (`TestGraphNELLEndToEnd`), plus unit coverage for the demo's evaluation/loader math.

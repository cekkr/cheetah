# cheetah

Ultra-rapid key/value database engine being refit to serve as the low-latency adapter. Implements a byte-oriented storage
engine in Go; this directory tracks the work needed to evolve it into a drop-in backend (`backend =
cheetah`) for the DB-SLM stack.

## Why it exists

- **Bridge for extreme-latency workloads.** SQLite remains the reference implementation, but it tops
  out when Level 1 counts, caches, and Level 2 conversation traces all compete for the same file.
  `cheetah` is meant to absorb those hot paths with a persistent, mmap-friendly layout and a
  purpose-built TCP/CLI front end.
- **Byte-faithful keying.** Each entry is cataloged by byte length and stored inside homogenous
  tables (`values_<size>_<tableID>.table`), keeping lookups O(1) with deterministic offsets.
- **Tree-indexed pair table.** The `pairs/` directory pre-allocates 256-entry pages that behave like
  a trie. This powers rapid pair/sequence lookups and will become the substrate for context hashes
  and vector-matrix anchors once the adapter is wired into DB-SLM.

## Current architecture snapshot

- `main.go` boots both a TCP server (`0.0.0.0:4455`) and a local CLI, multiplexing commands across
  multiple logical databases (`engine.GetDatabase(name)`).
- `engine.go` lazily instantiates `Database` handles under `cheetah_data/<dbname>` and guarantees
  graceful shutdown of every open table.
- `database.go` orchestrates CRUD operations, recycling, and the pair trie:
  - `MainKeysTable` (`main_keys.table`) stores the 6-byte entries that describe value size +
    location.
  - `ValuesTable` files store fixed-width blobs grouped by byte length and table ID; offsets are
    computed by entry index so reads never have to scan variable-length segments.
  - `RecycleTable` files keep tombstoned slots per byte length so inserts can reuse space without
    compacting.
  - `PairTable` nodes under `pairs/` encode byte-wise trees whose leaves can store absolute keys or
    child pointers, giving us a hardware-friendly trie for contextual lookups.
- `server.go` accepts newline-delimited commands over TCP (same grammar as the CLI) so we can script
  adapters before we embed the engine directly.

## Adapter goals for DB-SLM

To register `cheetah` as a DB adapter alongside SQLite and MariaDB (`DBSLMSettings.backend`),
the engine must grow the following behaviors:

1. **Rapid statistical computation + caching.** Level 1 relies on large fan-out aggregations (counts,
   continuation tables, quantized probabilities, Top-K ranks). `cheetah` must expose batched
   reducers or server-side stored procedures so the Python side can trigger recomputation without
   copying data back to SQLite first.
2. **Byte-based contextualization.** DB-SLM contexts are byte-tokenized hashes. The pair tables need
   to surface APIs that can auto-sort dynamic sets (e.g., context hashes → next token IDs) so
   probabilities can be refined by traversing deeper into the trie without retraining.
3. **Multi-file brute-force sweeps.** Level 2/3 materializations often need to scan entire matrix
   orderings. The engine must coordinate scans across parallel files, stream results, and keep hot
   references stable even when data is sharded across disks.

Document every milestone in `AI_REFERENCE.md` and flag missing capabilities in `NEXT_STEPS.md` so the
adapter status stays visible to future maintainers.

## Build & run

```bash
cd cheetah
go test ./...        # once tests exist
bash build.sh        # produces ./cheetah-server
./cheetah-server     # starts TCP server + interactive CLI
```

Example CLI session:

```
[cheetah_data/default]> INSERT:5 hello
SUCCESS,key=1
[cheetah_data/default]> READ 1
SUCCESS,size=5,value=hello
```

Over TCP you can send the same commands (newline-terminated). Use `DATABASE <name>` to switch logical
stores in either mode.

## Directory map

- `commands.go` – user-facing operations (INSERT/READ/EDIT/DELETE/PAIRSET/PAIRGET/etc.).
- `tables.go` – on-disk table implementations (`MainKeysTable`, `ValuesTable`, `RecycleTable`,
  `PairTable`).
- `types.go` – constants and binary layouts shared across tables.
- `helpers.go` – low-level utilities (value allocations, binary encoding helpers).
- `server.go` / `main.go` – TCP server + CLI wiring.

## Next steps

- Design the DB-SLM adapter boundary (likely a lightweight RPC with commands for context registry,
  Level 1 counts, Level 2 logs, and Level 3 concept tables).
- Port at least one hot path (e.g., `tbl_l1_context_registry`) to `cheetah` and validate that
  rebuild times beat the SQLite baseline.
- Extend the pair trie APIs so they can return ordered slices keyed by byte-range filters—this is the
  prerequisite for “rapid key contextualization” described in `CONCEPTS.md`.
- Add regression tests once the adapter contract is defined; keep them focused on byte-ordering and
  deterministic offsets so refactors remain safe.

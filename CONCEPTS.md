This the base code of project "cheetah" for a ultra-rapid fast access database. It has to be re-adapted to work as database engine for LMDB project with the name of "cheetah-db". 

At its base, cheetah allows to extremly fast access to data from a key using a perfectly compartmentalized key cataloging by byte size, fast tree access algorithms, and various levels of caching.

For the "lmdb" version to be implemented, it must be adapted as best as possible to the needs of the Python project, further implementing:
- Rapid statistical computation (and associated caching)
- Rapid key contextualization: this requires byte-based tokenization of values ​​and efficient auto-sorting of dynamic sets (more specific probabilities can be obtained by delving deeper into the context of the prediction, without requiring ad hoc retraining on the Python side) of vector matrices and associated probabilistic links.
- This requires brute-force analysis of the data relationships in the DB (as well as efficient single-ordering of matrices to maintain fixed references), maximizing the rapid data access speed and the use of multiple hard disk files to retrieve and compare data and obtain more relevant statistical results.

### Context relativism contract

- Python now emits every context twice: once under `ctx:<hash>` (payload = order + token ids) and
  again under `ctxv:<vector>` where `<vector>` is the Absolute Vector Order encoding of the nested
  token structure. Deterministic ordering is critical so that a client-provided tree such as
  `[[[12,45],[43,21]], [[643,23]]]` hashes to the exact same byte slice no matter how it was
  originally ingested.
- `PAIR_SCAN ctxv:` already provides everything needed to service `engine.context_relativism()`:
  iterate the namespace, look up the referenced payload via `READ`, and re-use the Top-K entry via
  `PAIR_GET topk:<order>` when present. Future Go-native reducers can inline that second hop.
- Regression checklist:
  1. Insert a deterministic context tree (e.g., `[ [ [1,2], [3,4] ], [ [5] ] ]`) twice and verify
     both writes land on the same `ctxv:` key and return the same payload.
  2. Exercise `PAIR_SCAN ctxv:` with a `limit` and cursor token to confirm pagination never skips or
     repeats keys. The CLI exposes the `next_cursor` token to make this visible.
  3. Use `PAIR_REDUCE counts cnt:<order>` followed by `PAIR_SCAN ctxv:` to ensure each relativistic
     projection can hydrate a `topk:` payload without additional SQLite calls.

### Level 2/3 metadata mirrors

- The Python bridge now mirrors higher-level metadata into cheetah so a restarted trainer or decoder
  can warm its caches without reading SQLite. All entries live under the `meta` namespace using the
  `PAIR_SET meta x<key>` path (see `CheetahHotPathAdapter.write_metadata`).
- Keys and layouts (visible as `meta:l2:*` entries because the Python adapter writes `l2:*` keys
  through the `meta` namespace automatically):
  - `meta:l2:stats:<conversation_id>` → JSON object with `message_count`, `user_turns`,
    `assistant_turns`, `started_at`, `updated_at`.
  - `meta:l2:corr:<conversation_id>` → JSON array containing the most recent correction digests, each
    shaped as `{"correction_id": "...", "payload": {...}}`.
  - `meta:l2:bias:<conversation_id | __global__>` → JSON array of bias presets mirroring
    `tbl_l2_token_bias` rows (`pattern`, `token_id`, `q_bias`, `expires_at`). Legacy
    `meta:meta:l2:*` entries (written by older trainers) continue to load because Python now
    canonicalizes keys before reading/writing.
- When these entries exist the decoder skips the SQL round-trip entirely; the mirroring happens
  synchronously whenever the conversation log, correction store, or bias table mutates.
- Regression plan:
  1. Log a conversation turn, then `PAIR_GET meta x6c32...` (stats key) and assert the counts tick up.
  2. Record a correction and ensure the `meta:l2:corr:` array is trimmed to the configured window.
  3. Insert a global bias row and confirm both `meta:l2:bias:__global__` and prefix scans return the
     JSON payload without needing SQLite.

### Reducer RPCs

- Reducer responses now inline their payloads:
  `SUCCESS,reducer=<mode>,count=N,items=<hex_key>:<abs_key>:<base64_payload>`. Clients never need to
  issue a follow-up `READ`.
- `PAIR_REDUCE counts cnt:<order>` walks the `cnt:` namespace and returns the serialized follower
  list encoded by `CheetahSerializer.encode_counts` (version byte, order, follower count, followed by
  `(token_id, count)` tuples in big-endian form).
- `PAIR_REDUCE probabilities prob:<order>` streams quantized log-probabilities and backoff alphas.
  Each payload includes `version (1 byte)`, `order (1 byte)`, `entry_count (uint16)`, then per-entry
  `(token_id uint32, q_logprob uint8, backoff_alpha uint16 | 0xFFFF for None)`.
- `PAIR_REDUCE continuations cont:` exposes Level 1 continuation metadata. Payload layout:
  `version (1 byte)`, `token_id (uint32)`, `num_contexts (uint32)`.
- Upcoming reducers (`PAIR_REDUCE cache`, Level 2 bias presets, queue-drain snapshots) should follow
  the same command grammar (`PAIR_REDUCE <mode> <prefix> [limit]`) and document their payload
  layouts here so both the CLI and TCP clients can decode them safely.
- Reducer regression plan:
  1. Issue `PAIR_REDUCE counts cnt:3 10` and verify both CLI and Python decode the inline payload
     without performing a follow-up `READ`.
  2. Saturate `PAIR_REDUCE probabilities prob:4` so the response spans multiple pages and ensure the
     cursor resumes on the next call.
  3. Spot-check `PAIR_REDUCE continuations cont:` against SQLite (`tbl_l1_continuations`) to confirm
     the mirrored follower counts stay in sync with rebuilds.

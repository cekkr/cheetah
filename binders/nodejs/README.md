# cheetah-db — Node.js binder

A dependency-free CommonJS client for `cheetah-server`. The protocol is
newline-delimited text over TCP, so this needs nothing but `net`.

```js
const { CheetahPool, kv } = require('cheetah-db');

const pool = new CheetahPool({ host: '127.0.0.1', port: 4455, database: 'app' });
await pool.connect();

await kv.putJson(pool, 'user:42', { name: 'Ada' }, { upsert: true });
console.log(await kv.getJson(pool, 'user:42'));

await pool.close();
```

Requires Node 18+.

## Layers

Each is usable on its own; higher ones are conveniences over lower ones.

| Module | What it owns |
| --- | --- |
| `protocol` | Pure codec. `buildCommand`, `buildKeyValueCommand`, `parseResponse`, `parseItems`, `parseCursor`, `decodePayload`, `encodeArgument`, `rawArgument`. No socket, no state. |
| `client` | `CheetahClient` (one socket, FIFO response matching, pipelining, reconnect) and `CheetahPool` (spread, lease, broadcast). `{binary: true}` switches the socket to the byte-wise protocol. |
| `binary` | The byte-wise transport: frames, value type tags, `BinarySession`, `encodeCommandLine`/`decodeResponse`, `encodeRequest` for explicitly typed arguments. |
| `alias` | `ALIAS` — what a client cannot derive on its own: `listCommands`, `listArgumentKeys`, `resolveCommand`, `describeTypes`, `tableProfile`, `listProfiles`, `aliasDigest`, `loadSession`. |
| `kv` | The two-step write and its reads: `putValue`/`getValue`, `putJson`/`getJson`, `putJsonBatch`, `insert`, `editAbsoluteKey`, `readAbsoluteKey`, `pairSet` (with `hidden`), `deletePair`, `deleteValue`, `purgePrefix`, `pairSummary`, `scanPage`/`scanPrefix`/`scanAll`. |
| `graph` | Nodes and edges: `setNode` (labels, props, `references`), `getNode`, `deleteNode`, `setEdge`, `getEdge`, `deleteEdge`, `setEdgeBatch`. Adjacency: `neighbors`, `neighborsAll`, `neighborTypes`, `degree`. Queries: `query`, `recall`, `recallBatched`, `recallAsync`, `fetchRecall`, `awaitRecall`, `similar`, `termIndex` (including `weighted`/`tokens`/`trigrams` stats). Recall results expose base `decay`, adaptive `cacheDecay`, `decayRelations`, and `decayProfile`. Plus a pure `build*` for each command, for callers assembling their own batch. |
| `records` | Multi-field tables: `define`, `alter`, `compact`, `schema`, `tables`, `setRow`, `getRow`, `scanPage`/`scanRows`/`scanAll`, `deleteRow`, `dropTable`, plus `fieldSpec` and a pure `build*` for each command. |
| `jobs` | Detached commands over the `JOB` envelope: `submit`, `status`, `fetch`, `results`, `awaitJob`, `supportsJobApi`. |
| `batch` | `BATCH` — one command, N argument sets, one round trip: `buildBatch`, `runBatch`, `runBatchChunked`, `runBatchAsync`, `batch`, `parseBatchResponse`, plus `CommandBatcher`, the coalescer `CheetahClient` runs by itself. |
| `predict` | Prediction tables: `setValue`, `query`, `train`, `contextAdjust`, `inherit`, `inheritBatch`, `backend`, `bench`. |
| `admin` | The server and the registry of databases, not the data: `createDatabase`, `configureDatabase`, `listDatabases`, `useDatabase`, `resetDatabase`, `systemStats`, `logFlush`, `fileCheckpoint`, `clusterUpdate`/`clusterStatus`/`clusterMove`, `forkAssign`. |
| `keys` | Key-building primitives: fixed-width `hex`/`unhex`, `sha1`, and integer `quantize`/`bucketize`/`bucketSweep`. |
| `vocabulary` | `TokenVocabulary` — a persisted string → uint32 allocator, both directions. |
| `database` | `CheetahDatabase` — the plumbing an application writes around all of the above. Subclass it; its owned pool accepts the same `binary` transport option as `CheetahClient`/`CheetahPool`. |
| `server` | `startServer`/`ensureServerBinary` — spawn a server for development and tests. |

## Speaking bytes instead of text

The wire is text by default: `42` costs two bytes, `0.25` four, a 32-byte key
sixty-four in hex. Pass `binary` and the same connection sends **frames**
instead — the command as a 2-byte index, each value in its own type.

```js
const client = new CheetahClient({ port: 4455, database: 'app', binary: true });
await client.connect();
// or state the widths you want as defaults:
new CheetahClient({ port: 4455, binary: { uint: 4, float: 4 } });
```

**Nothing above the socket changes.** `kv`, `graph`, `records`, `predict`,
`admin` and `CheetahDatabase` keep building command lines; the client transcodes
them and turns each response frame back into the canonical line. A command added
to the server tomorrow works over binary the day it exists, because the server
decodes a frame into that same line before routing it.

What you get on connect is a `BinarySession` (`client.binary`): the negotiated
widths, the command index, the argument-key dictionary, and the digest to check
a cached copy against — all of it delivered by the handshake, so no extra round
trip.

```js
const alias = require('cheetah-db/lib/alias');

client.binary.commandId('RECORD');            // → its 2-byte index
client.binary.matchesDigest((await alias.aliasDigest(client)).digest);  // → true
```

Two things stay true in binary mode, and one changes:

- a `key=value` value still may not contain whitespace — the frame is refused
  rather than truncated. Use the `bytes` type (`x<hex>` on the line); in binary
  it costs nothing;
- numbers are typed automatically when a token re-renders identically, so `42`
  travels as one byte and `007` stays a string;
- **widths are always stated explicitly when transcoding.** A width-0 tag means
  "the server resolves it" — from the table's profile or the session — and only
  a caller that has loaded that profile (`alias.tableProfile`) can predict it.
  Getting it wrong is a misread frame, not a rounding error.

A table's numeric widths live on the server, not here, because two processes
writing the same table must encode it the same way:

```js
await alias.tableProfile(client, 'ngram', { uint: 4, float: 4 });
await alias.tableProfile(client, 'ngram');   // → resolved widths + what is declared
```

## Things the protocol will do to you

These are not style preferences; each one is a silent failure the binder exists
to prevent.

- **There are no request ids.** Responses match commands by arrival order on a
  connection. A sequence that must not be interleaved — any read-modify-write —
  has to lease a connection with `pool.withConnection`, not use `pool.send`. A
  command that times out takes its connection down with it, because abandoning
  its slot in the queue would misalign every later response.
- **A write is two round trips.** `INSERT` stores bytes and returns an absolute
  key; `PAIR_SET` binds a name to it. `kv.putValue` does both. For bulk work use
  `kv.putJsonBatch`, which is one request per page instead of two per record —
  the difference is large, since ingestion is round-trip bound rather than
  payload bound.
- **`value=` owns the rest of the response line.** A `READ` payload is
  unescaped, so it legitimately contains commas. Splitting the whole line on `,`
  corrupts every JSON value you read back.
- **A leading `x` means hex.** The server decodes any argument starting with `x`
  as a hex string, so a key spelled `x:thing` is unaddressable in its bare form.
  `encodeArgument` escapes it; pick namespaces that do not start with `x`.
- **A `next_cursor` must go back verbatim**, through `rawArgument`. Encoding it
  again hex-encodes the hex, and the server resumes from a prefix that does not
  exist — a sweep that quietly returns its first page instead of failing.
- **`GRAPH_*` splits `key=value` tokens on whitespace.** No value may contain a
  space, so anything free-form travels base64 (`graph.encodeJsonArgument`).
  `GRAPH_RECALL` also caps seeds at 32 per call; `recallBatched` batches above
  that and merges with the same noisy-OR the server uses within a batch.
- **Recall decay diagnostics are outputs, not extra client policy.** `recall` returns
  `decay`, `cacheDecay`, `decayRelations` and `decayProfile`; teach relation-specific
  decay through the existing prediction helpers with table `graph_recall_decay` and
  `carry`/`stop` values.
- **Omitting a graph field preserves it; clearing it is a different spelling.**
  `GRAPH_NODE_SET` keeps stored labels/props/references when the argument is
  absent, so `references: undefined` is "leave them alone" and `references:
  null` is the `-` that empties them. Passing `[]` writes an empty list, which
  is a third thing again. The same asymmetry is why flags must be `1`/`0`: a
  JavaScript `false` stringified as `false` is not a flag the server reads.
- **A batch is not a transaction.** `GRAPH_EDGE_SET_BATCH` reports
  `requested`/`applied`/`failed` and can succeed with edges missing;
  `setEdgeBatch` returns that accounting rather than a boolean, and its optional
  `chunkSize` is off by default because splitting a list changes what a partial
  failure leaves behind.
- **The canonical command line is a latin1 byte spelling** for byte
  transparency, so UTF-8 payloads are transcoded at both edges
  (`kv.toWire`/`kv.fromWire`). Text sockets write that spelling directly;
  binary sockets preserve those same bytes while framing it. Skipping that —
  or re-encoding the latin1 spelling as UTF-8 in a transcoder — mangles any
  non-ASCII string you store.
- **Keys are an index, not a label.** In a pair trie the key bytes *are* the
  index, so changing a key layout means rebuilding the database. Use fixed-width
  lowercase hex for numeric segments (`keys.hex`) — a scan is byte-ordered, and
  `String(n)` sorts `10` before `9`.
- **Bucket continuous values in integers.** `keys.quantize` then
  `keys.bucketize`. In floats, `(v - tol) / width` lands on `224.99999999999997`
  where exact arithmetic gives `225`, which silently widens a tolerance sweep
  from two buckets to three.

## Record tables — several fields, one row

A pair name maps to exactly one payload, so describing one thing with several
quantities used to mean several names (`cnt:<k>`, `prob:<k>`, `meta:<k>`): three
trie entries, three payloads, three round trips, and nothing keeping them
consistent. A record table declares those quantities once, with a byte width
each, and packs them into one row.

```js
const { records } = require('cheetah-db');

await records.define(pool, 'ngram', 'cnt:uint:4,prob:float:4,label:string:12');
await records.setRow(pool, 'ngram', 'berlin', { cnt: 42, prob: 0.25, label: 'city' });

// A write patches only the fields it names; the others keep their bytes.
await records.setRow(pool, 'ngram', 'berlin', { cnt: 43 });
await records.getRow(pool, 'ngram', 'berlin');   // { cnt: 43, prob: 0.25, label: 'city' }

for await (const row of records.scanRows(pool, 'ngram', { prefix: 'be', limit: 500 })) {
    console.log(row.key, row.fields);
}

// Occasional predicates reduce rows inside the server; repeated ones can opt
// into a derived index without changing the result/paging contract.
await records.selectAll(pool, 'ngram', { field: 'cnt', op: 'gt', value: 100, budget: 4096 });
await records.configureIndex(pool, 'ngram', 'cnt', { action: 'create' });
for await (const row of records.selectRows(pool, 'ngram', { field: 'cnt', op: 'gte', value: 100 })) {
    console.log(row.key, row.fields.cnt);
}
```

Three properties of the family are worth internalising, because they decide how
you use it:

- **`alter` never rewrites a row.** Field offsets never move: an added field is
  appended, a dropped field's bytes stay as dead space. A row written before an
  `add` therefore reads `null` for the new field — *not* `0`, which nobody wrote
  — until the next `setRow` brings it up to the current width.
- **`compact` is the only call that touches rows.** It reclaims what a drop left
  behind, and it is explicit for that reason. It bumps the table's generation and
  briefly doubles its footprint while copying.
- **Field indexes are optional derived state.** `selectPage` reports `scanned`,
  `indexed`, and a cursor either way; create an index only when repeated predicates
  justify maintaining an extra entry on every changed row.
- **A field name is an argument.** `RECORD set table=t key=k <field>=<value>` puts
  field names in the same namespace as the command's own modifiers, so `table`,
  `key`, `fields`, `limit`, `cursor` and friends are refused — by this binder at
  `define`/`setRow` time, before the wire.

Values follow the same escaping rule as everywhere else: text holding a space or
starting with `x` travels as `x<hex>`, and the binder does it for you. `bytes`
fields read back padded to their declared width, because a fixed-width field has
no length of its own.

## Batching, and the batching you do not have to ask for

Cheetah is round-trip bound under bulk work, and `BATCH` is the server's general
answer: one command name, any target command, one request.

```js
const { batch } = require('cheetah-db');

// Raw argument lines — whatever you would have written as single commands.
const bound = await batch.runBatch(pool, 'PAIR_SET', keys.map((k, i) => `ctx:${i} ${k}`));
bound.applied; bound.failed; bound.results[0].ok;   // one parsed response per item

// Object items take the key=value dialect, over modifiers shared by all of them.
await batch.runBatch(pool, 'GRAPH_EDGE_SET', edges, { shared: { type: 'knows' } });

// Big enough to detach: submitted as a job, results delivered as they are produced.
await batch.runBatchAsync(pool, 'PAIR_SET', millionsOfLines, {
    onResult: (response, index) => { /* arrives while the job runs */ },
    onProgress: (snapshot) => console.log(snapshot.progress),
});
```

`BATCH` is **not** a transaction — items apply in order and independently — so the
result carries `applied`/`failed`/`firstError` rather than throwing, and
`continueOnError` (default `true` here) decides whether one bad item stops the
rest. The binder explicitly sends `continue_on_error=1` because the server's
raw-command default is stop-on-error. An item that never ran is `null` in
`results`, positionally aligned.

**You usually do not have to call any of it.** `CheetahClient` watches for bulk
work: once a command has been issued `threshold` times inside `windowMs` it is
*hot*, and the calls a caller starts in one turn of the event loop are folded
into a single `BATCH`, each getting its own response back. Nothing above
notices. A caller that awaits every command in turn can never have two
outstanding, so it never batches and pays nothing — it is bursts, not volume,
that the coalescing can act on.

```js
const client = new CheetahClient({
    autoBatch: {
        enabled: true,      // false restores the exact pre-batching wire behavior
        threshold: 8,       // calls of one command inside windowMs to go hot
        windowMs: 200,
        idleMs: 2000,       // silence that cools a command back down
        maxSize: 256,       // flush at this many items…
        maxBytes: 512 * 1024,   // …or this many bytes of line
        minSize: 2,         // a queue smaller than this is sent as plain commands
        flushMs: 0,         // 0 = end of tick; >0 holds the queue that long
        commands: null,     // allowlist of command names, or null for all
        exclude: [...],     // never batch these (see AUTO_BATCH_EXCLUDED)
        onBatch: (info) => {},
    },
});
client.batchStats;   // { batched, batches, direct }
client.flush();      // write a queued batch right now
```

Order is preserved end to end: anything that cannot join the pending queue
flushes it first, and the server applies a batch's items in order. Excluded by
default are the connection-scoped commands, `BATCH`/`JOB` themselves, and the
administrative ones (`SYSTEM_STATS`, `LOG_FLUSH`, `FILE_CHECKPOINT`, the
`CLUSTER_*` family, `FORK_ASSIGN`) — batching those buys nothing and only makes
a failure harder to read.

## Databases, jobs and the server

```js
const { admin, graph, jobs } = require('cheetah-db');

// A database of its own, with settings that override the server's [database]
// section for this database alone and persist next to its data.
await admin.createDatabase(pool, 'bench', {
    pair_bytes: 2,
    sharded_key_slots: true,
    key_slot_bits: 12,
    payload_cache_mb: 256,
});
await admin.configureDatabase(pool, 'bench', { payload_cache_mb: 128, graph_cache_sample: 0.5 });
await admin.listDatabases(pool);        // name, path, loaded, adHoc, settings

// A sweep too long for one round trip.
const jobId = await jobs.submit(pool, 'PAIR_REDUCE counts ctx: 4096');
const result = await jobs.awaitJob(pool, jobId, { pollIntervalMs: 2000 });

// Wide recall returns the same kind of retrieval handle. Omitting budget asks
// the server for its maximum bounded detached sweep.
const recallId = await graph.recallAsync(pool, { seeds: ['cat:luna', 'person:marco'], hops: 4 });
const recalled = await graph.awaitRecall(pool, recallId, { pollIntervalMs: 2000 });

await admin.systemStats(pool);          // gauges; `NA` reads as null, never 0
```

`createDatabase` refuses a name that already exists — that refusal is the whole
difference from `useDatabase`, which opens-or-creates and would silently adopt a
populated directory *and* ignore the settings you passed, since trie geometry is
decided when the directory is made. Main-key sharding is creation-time geometry
too; changing `sharded_key_slots` or `key_slot_bits` requires `resetDatabase`.

## `CheetahDatabase`

The class exists because every application ends up writing the same six things
around the free functions: pool construction, a connect that refuses a database
written by an incompatible codec, a close that only closes what it owns, a
read-modify-write that cannot interleave with itself, collision-checked id
allocation, and payload accounting per namespace. It holds those and nothing
about any particular schema.

```js
const { CheetahDatabase } = require('cheetah-db');

class ArticleStore extends CheetahDatabase {
    constructor(options = {}) {
        super({ ...options, layout: { key: 'cfg:article_layout', version: 1 } });
        this.articles = new Map();
    }

    // Runs on a leased connection after the layout check.
    async onConnect() {}

    // Called when reset() drops the database under you.
    clearCaches() {
        this.articles.clear();
    }

    async put(article) {
        const id = await this.allocateRandomId((candidate) => `a:${candidate}`);
        // Cheetah has no transaction spanning several writes, so a completion
        // marker is the commit: write incomplete, write the parts, then flip it.
        await this.putJson(`a:${id}`, { ...article, complete: false }, { upsert: true });
        await this.putJsonBatched(article.sections.map((section, at) => ({
            key: `as:${id}/${at}`,
            payload: section,
        })));
        await this.putJson(`a:${id}`, { ...article, complete: true }, { upsert: true });
        return id;
    }

    async hits(word) {
        return this.recall({ seeds: [`w:${word}`], hops: 1, type: 'mention' });
    }
}
```

Inherited: `connect`/`close`/`reset`, `withConnection`, `getValue`/`putValue`,
`getJson`/`putJson`/`putJsonBatched`, `deletePair`, `scan`/`scanAll`/`scanJson`,
`mutateJson`, `allocateRandomId`, `timestamp`, `pairSummary`/`namespaceSummary`,
and the graph surface (`setNode`, `getNode`, `setEdgeBatch`, `degree`, `recall`,
`recallAsync`, `fetchRecall`, `awaitRecall`).

Hooks for subclasses: `onConnect(conn)` and `clearCaches()`.

`layout` is optional but strongly advised. A trie key layout is not
self-describing: a codec that changed its segment widths reads an older database
as keys that simply match nothing, which looks like an empty result rather than
an error. The marker turns that into a loud failure on connect.

## Running a server for tests

```js
const { startServer } = require('cheetah-db');

const server = await startServer({ port: 4467, dataDir: '/tmp/cheetah-test' });
// … talk to it on server.host:server.port …
await server.stop();
```

It builds `cheetah-server` (`cheetah-server.exe` on Windows) from this repository if the binary is
missing, which needs a Go toolchain. The platform-specific name is important because Windows cannot
spawn the extensionless Go output. `graphTermIndex` and `pairIndexBytes` are left unset unless
you pass them, so the server's own configuration decides; note that
`pairIndexBytes` is adopted when a database directory is **created** and pinned
from then on, so setting it against an existing database does nothing.

## Tests

```bash
node --test test/*.test.js
```

No dev dependency: the runner is Node's own. The suite covers the protocol
codec, the key primitives, the `GRAPH_*`/`RECORD`/`JOB`/`PREDICT_*`/admin command
spellings, the binary codec and the `ALIAS` layer, and `CheetahDatabase` against
an in-memory stand-in that speaks the same line protocol. It does not prove the server answers that way — the Go suite
(`go test ./src`) does, and so does the live round-trip:

```bash
CHEETAH_INTEGRATION=1 node --test test/integration.test.js
```

That one builds the server **only if the binary is missing**, so a stale
`cheetah-server` at the repository root silently tests an old protocol: rebuild
it (`go build -o cheetah-server ./src`) when a command answers
`ERROR,unknown_command` there.

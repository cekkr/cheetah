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
| `client` | `CheetahClient` (one socket, FIFO response matching, pipelining, reconnect) and `CheetahPool` (spread, lease, broadcast). |
| `kv` | The two-step write and its reads: `putValue`/`getValue`, `putJson`/`getJson`, `putJsonBatch`, `deletePair`, `scanPage`/`scanPrefix`/`scanAll`. |
| `graph` | Nodes and edges: `setNode` (labels, props, `references`), `getNode`, `deleteNode`, `setEdge`, `getEdge`, `deleteEdge`, `setEdgeBatch`. Adjacency: `neighbors`, `neighborsAll`, `neighborTypes`, `degree`. Queries: `query`, `recall`, `recallBatched`, `similar`, `termIndex`. Plus a pure `build*` for each command, for callers assembling their own batch. |
| `keys` | Key-building primitives: fixed-width `hex`/`unhex`, `sha1`, and integer `quantize`/`bucketize`/`bucketSweep`. |
| `vocabulary` | `TokenVocabulary` — a persisted string → uint32 allocator, both directions. |
| `database` | `CheetahDatabase` — the plumbing an application writes around all of the above. Subclass it. |
| `server` | `startServer`/`ensureServerBinary` — spawn a server for development and tests. |

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
- **The socket speaks latin1** for byte transparency, so UTF-8 payloads are
  transcoded at both edges (`kv.toWire`/`kv.fromWire`). Skipping that mangles
  any non-ASCII string you store.
- **Keys are an index, not a label.** In a pair trie the key bytes *are* the
  index, so changing a key layout means rebuilding the database. Use fixed-width
  lowercase hex for numeric segments (`keys.hex`) — a scan is byte-ordered, and
  `String(n)` sorts `10` before `9`.
- **Bucket continuous values in integers.** `keys.quantize` then
  `keys.bucketize`. In floats, `(v - tol) / width` lands on `224.99999999999997`
  where exact arithmetic gives `225`, which silently widens a tolerance sweep
  from two buckets to three.

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
and the graph surface (`setNode`, `getNode`, `setEdgeBatch`, `degree`, `recall`).

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

It builds `cheetah-server` from this repository if the binary is missing, which
needs a Go toolchain. `graphTermIndex` and `pairIndexBytes` are left unset unless
you pass them, so the server's own configuration decides; note that
`pairIndexBytes` is adopted when a database directory is **created** and pinned
from then on, so setting it against an existing database does nothing.

## Tests

```bash
node --test test/*.test.js
```

No dev dependency: the runner is Node's own. The suite covers the protocol
codec, the key primitives, and `CheetahDatabase` against an in-memory stand-in
that speaks the same line protocol. It does not prove the server answers that
way — the Go suite (`go test ./src`) does.

# cheetah-db — Python binder

A dependency-free client for `cheetah-server`. The protocol is newline-delimited
text over TCP, so this needs nothing but the standard library.

```python
from cheetah_db import CheetahClient, kv

with CheetahClient("127.0.0.1", 4455, database="app") as conn:
    kv.put_json(conn, "user:42", {"name": "Ada"}, upsert=True)
    print(kv.get_json(conn, "user:42"))
```

Requires Python 3.10+. There is no package on PyPI: add `binders/python` to
`sys.path` (or install it with `pip install -e binders/python`) and import
`cheetah_db`.

## Layers

Each is usable on its own; higher ones are conveniences over lower ones.

| Module | What it owns |
| --- | --- |
| `protocol` | Pure codec. `build_command`, `build_key_value_command`, `parse_response`, `parse_items`, `parse_cursor`, `decode_payload`, `decode_transport_payload`, `encode_argument`, `raw_argument`. No socket, no state. |
| `hosts` | Where a client should actually dial: `0.0.0.0` → `127.0.0.1`, and the WSL host's address as a fallback candidate. |
| `client` | `CheetahClient` (one socket, lock-serialized request/response, reconnect, inactivity grace) and `ThreadLocalClientPool` (one socket per thread). |
| `kv` | The two-step write and its reads: `put_value`/`get_value`, `put_json`/`get_json`, `put_bytes`/`get_bytes`, `put_values_batch` (`PAIR_PUT_BATCH`), `delete_pair`, `pair_purge`, `scan_page`/`scan_prefix`/`scan_all`, `pair_summary`. |
| `graph` | The whole `GRAPH_*` surface: nodes, edges (single and batch), `neighbors`/`degree`/`neighbor_types`, `query`, `recall`/`recall_batched`, `similar`, `term_index`, and the ambiguity trio. |
| `records` | Multi-field tables: `define`, `alter`, `compact`, `schema`, `tables`, `set_row`, `get_row`, `scan`/`iter_rows`, `delete_row`, `drop_table`, plus `field_spec`. |
| `jobs` | The `JOB` micro-command: `submit`, `status`, `fetch`, `await_job`, `supports_job_api`. |
| `predict` | Prediction tables: `set_value`, `query`, `train`, `context_adjust`, `inherit`, `inherit_batch`, `backend`, `bench`. |
| `admin` | The server and the registry of databases, not the data: `create_database`, `list_databases`, `use_database`, `reset_database`, `system_stats`, `log_flush`, `file_checkpoint`, `cluster_*`, `fork_assign`. |
| `keys` | Key-building primitives: fixed-width `hex_segment`/`unhex`, `sha1`, and integer `quantize`/`bucketize`/`bucket_sweep`. |
| `vocabulary` | `TokenVocabulary` — a persisted string → uint32 allocator, both directions. |
| `database` | `CheetahDatabase` — the plumbing an application writes around all of the above. Subclass it. |
| `server` | `start_server`/`ensure_server_binary` — spawn a server for development and tests. |

## Things the protocol will do to you

These are not style preferences; each one is a silent failure the binder exists
to prevent.

- **A write is two round trips.** `INSERT` stores bytes and returns an absolute
  key; `PAIR_SET` binds a name to it. `kv.put_value` does both. For bulk work
  use `kv.put_values_batch`, which is one request per page instead of two per
  record. It does not make the server faster — the per-record work is unchanged
  — so it pays off when the client cannot pipeline, or the link has latency.
- **`value=` owns the rest of the response line.** A `READ` payload is
  unescaped, so it legitimately contains commas. Splitting the whole line on `,`
  corrupts every JSON value you read back.
- **A leading `x` means hex.** The server decodes any argument starting with `x`
  as a hex string, so a key spelled `x:thing` is unaddressable in its bare form.
  `encode_argument` escapes it; pick namespaces that do not start with `x`. The
  same rule applies **inside** `PAIR_PUT_BATCH` items, which is why a base64
  payload that happens to begin with `x` is hex-escaped there too.
- **A `next_cursor` must go back verbatim**, through `raw_argument`. Encoding it
  again hex-encodes the hex, and the server resumes from a prefix that does not
  exist — a sweep that quietly returns its first page instead of failing.
- **`GRAPH_*` splits `key=value` tokens on whitespace.** No value may contain a
  space, so anything free-form travels base64 (`protocol.encode_json_argument`),
  and `build_key_value_command` raises rather than sending a value that would be
  truncated. Free-text `GRAPH_RECALL` seeds travel as `base64:<…>`.
  `GRAPH_RECALL` also caps seeds at 32 per call; `graph.recall_batched` batches
  above that and merges with the same noisy-OR the server uses within a batch.
- **`GRAPH_NODE_SET references=` replaces the stored list**, it does not merge.
  Read the node back and write the union if you mean to extend provenance; `-`
  clears it.
- **`GRAPH_RECALL` excludes seed nodes from their own answer** unless
  `include_seeds=True`, so hydrating references without it returns nothing for
  the node the query is actually about.
- **Text travels raw, binary travels base64.** `INSERT` takes the rest of the
  line as bytes, so UTF-8 documents need no wrapping, but an arbitrary byte
  string cannot cross a newline-delimited protocol: use `kv.put_bytes`, and
  `protocol.decode_transport_payload` to unwrap reducer output written that way.
- **Keys are an index, not a label.** In a pair trie the key bytes *are* the
  index, so changing a key layout means rebuilding the database. Use fixed-width
  lowercase hex for numeric segments (`keys.hex_segment`) — a scan is
  byte-ordered, and `str(n)` sorts `10` before `9`.
- **Bucket continuous values in integers.** `keys.quantize` then
  `keys.bucketize`. In floats, `(v - tol) / width` lands on `224.99999999999997`
  where exact arithmetic gives `225`, which silently widens a tolerance sweep
  from two buckets to three.
- **Jobs live in memory.** `JOB fetch` consumes the job and a restart loses it,
  so treat `job_not_found` as terminal rather than retrying.

## Record tables — several fields, one row

A pair name maps to exactly one payload, so describing one thing with several
quantities used to mean several names (`cnt:<k>`, `prob:<k>`, `meta:<k>`): three
trie entries, three payloads, three round trips, and nothing keeping them
consistent. A record table declares those quantities once, with a byte width
each, and packs them into one row.

```python
from cheetah_db import records

records.define(conn, "ngram", "cnt:uint:4,prob:float:4,label:string:12")
records.set_row(conn, "ngram", "berlin", {"cnt": 42, "prob": 0.25, "label": "city"})

# A write patches only the fields it names; the others keep their bytes.
records.set_row(conn, "ngram", "berlin", {"cnt": 43})
records.get_row(conn, "ngram", "berlin")   # {'cnt': 43, 'prob': 0.25, 'label': 'city'}

for row in records.iter_rows(conn, "ngram", prefix="be"):
    print(row.text, row.fields)
```

Three properties of the family are worth internalising, because they decide how
you use it:

- **`alter` never rewrites a row.** Field offsets never move: an added field is
  appended, a dropped field's bytes stay as dead space. A row written before an
  `add` therefore reads `None` for the new field — *not* `0`, which nobody wrote
  — until the next `set_row` brings it up to the current width.
- **`compact` is the only call that touches rows.** It reclaims what a drop left
  behind, and it is explicit for that reason. It bumps the table's generation and
  briefly doubles its footprint while copying.
- **A field name is an argument.** ``RECORD set table=t key=k <field>=<value>``
  puts field names in the same namespace as the command's own modifiers, so
  `table`, `key`, `fields`, `limit`, `cursor` and friends are refused — by this
  binder at `define`/`set_row` time, before the wire.

Values follow the same escaping rule as everywhere else: text holding a space or
starting with `x` travels as `x<hex>`, and the binder does it for you. `bytes`
fields read back padded to their declared width, because a fixed-width field has
no length of its own.

## Databases of their own

```python
from cheetah_db import admin

# Settings that override the server's [database] section for this database
# alone, persisted next to its data so they survive a restart.
admin.create_database(conn, "bench", pair_bytes=2, payload_cache_mb=256)
admin.list_databases(conn)      # name, path, loaded, ad_hoc, settings
```

`create_database` refuses a name that already exists — that refusal is the whole
difference from `use_database`, which opens-or-creates and would silently adopt a
populated directory *and* ignore the settings passed, since trie geometry is
decided when the directory is made.

## Concurrency

A `CheetahClient` serializes send+receive under a lock, so two threads sharing
one are safe but take turns. For concurrent work give each thread its own
socket:

```python
from cheetah_db import CheetahClient, ThreadLocalClientPool

pool = ThreadLocalClientPool(lambda: CheetahClient("127.0.0.1", 4455, database="app"))
conn = pool.acquire()   # this thread's client, connected
...
pool.close_all()
```

## `CheetahDatabase`

The class exists because every application ends up writing the same handful of
things around the free functions: pool construction, a connect that refuses a
database written by an incompatible codec, a close that only closes what it
owns, a read-modify-write that cannot interleave with itself, collision-checked
id allocation, and payload accounting per namespace. It holds those and nothing
about any particular schema.

```python
from cheetah_db import CheetahDatabase


class ArticleStore(CheetahDatabase):
    def __init__(self, **options):
        super().__init__(layout={"key": "cfg:article_layout", "version": 1}, **options)
        self.articles = {}

    # Runs on this thread's connection after the layout check.
    def on_connect(self, conn):
        pass

    # Called when reset() drops the database under you.
    def clear_caches(self):
        self.articles.clear()

    def put(self, article):
        article_id = self.allocate_random_id(lambda candidate: f"a:{candidate}")
        # Cheetah has no transaction spanning several writes, so a completion
        # marker is the commit: write incomplete, write the parts, then flip it.
        self.put_json(f"a:{article_id}", {**article, "complete": False}, upsert=True)
        self.put_json_batched(
            [(f"as:{article_id}/{at}", section) for at, section in enumerate(article["sections"])]
        )
        self.put_json(f"a:{article_id}", {**article, "complete": True}, upsert=True)
        return article_id

    def hits(self, word):
        return self.recall([f"w:{word}"], hops=1, edge_type="mention")
```

Inherited: `connect`/`close`/`reset`, `conn`, `get_value`/`put_value`,
`get_json`/`put_json`/`put_json_batched`, `delete_pair`, `scan`/`scan_all`/
`scan_json`, `mutate_json`, `allocate_random_id`, `timestamp`, `pair_summary`/
`namespace_summary`, and the graph surface (`set_node`, `get_node`,
`set_edge_batch`, `degree`, `recall`).

Hooks for subclasses: `on_connect(conn)` and `clear_caches()`.

`layout` is optional but strongly advised. A trie key layout is not
self-describing: a codec that changed its segment widths reads an older database
as keys that simply match nothing, which looks like an empty result rather than
an error. The marker turns that into a loud failure on connect.

## Running a server for tests

```python
from cheetah_db.server import start_server

server = start_server(port=4467, data_dir="/tmp/cheetah-test")
# … talk to it on server.host:server.port …
server.stop()
```

It builds `cheetah-server` from this repository if the binary is missing, which
needs a Go toolchain. `graph_term_index` and `pair_index_bytes` are left unset
unless you pass them, so the server's own configuration decides; note that
`pair_index_bytes` is adopted when a database directory is **created** and
pinned from then on, so setting it against an existing database does nothing.

## Tests

```bash
cd binders/python && python3 -m unittest discover -s tests -t .
```

No dev dependency: the runner is the standard library's. The suite covers the
protocol codec, the key primitives, the KV/graph/record/job/database call shapes
and `CheetahDatabase` against an in-memory stand-in that speaks the same line
protocol. That stand-in does not prove the server answers that way — the Go
suite (`go test ./src`) and the gated integration test do:

```bash
cd binders/python && CHEETAH_INTEGRATION=1 python3 -m unittest discover -s tests -t .
```

which builds and boots a real server on a free port in a temporary data
directory — note that it builds only when the binary is **missing**, so a stale
`cheetah-server` at the repository root silently tests an old protocol. Rebuild
it (`go build -o cheetah-server ./src`) when a command there answers
`ERROR,unknown_command`.

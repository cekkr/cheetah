// `CheetahDatabase` — a connected, version-guarded handle on one database.
//
// The layers below this one are deliberately free functions over a connection:
// `kv`, `graph` and `protocol` know nothing about who is calling them. That is
// the right shape for a protocol binding and the wrong shape for an
// application, which ends up rewriting the same six things around them every
// time — pool construction, a connect that refuses a database written by an
// incompatible codec, a close that only closes what it owns, a read-modify-write
// that cannot interleave with itself, collision-checked id allocation, and
// payload accounting per namespace.
//
// So this class holds exactly those, and nothing about any particular schema.
// It is meant to be **subclassed**: a store extends it, adds the methods that
// know its own namespaces, and inherits the plumbing. Everything it does is
// still reachable as a free function for callers who would rather compose.
//
// The commit protocol worth copying, since Cheetah has no transaction spanning
// several writes: write the record incomplete, write its parts, then flip a
// completion marker, and have every reader ignore records without it. This
// class does not impose it — a marker means nothing without a reader that
// respects it — but `mutateJson` and `allocateRandomId` are the pieces such a
// protocol needs.

const crypto = require('crypto');
const { CheetahError, CheetahPool } = require('./client');
const { decodeItemPayload, numericField } = require('./protocol');
const graph = require('./graph');
const kv = require('./kv');
const records = require('./records');

const DEFAULT_SCAN_LIMIT = 500;
const DEFAULT_WRITE_BATCH_SIZE = 256;

/** Decode a payload hydrated by the `continuations` reducer during a scan. */
function hydrateJson(item) {
    const bytes = decodeItemPayload(item);
    if (!bytes) {
        throw new CheetahError(`cheetah scan did not hydrate ${item?.key || '(unknown key)'}`);
    }
    try {
        return JSON.parse(bytes.toString('utf8'));
    } catch (error) {
        throw new CheetahError(`cheetah payload at ${item.key} is not JSON: ${error.message}`);
    }
}

class CheetahDatabase {
    /**
     * @param {object} [options]
     * @param {object} [options.pool] an existing pool/client; when given, this
     *   instance does not own it and `close()` leaves it open.
     * @param {string} [options.database] database name (`DATABASE <name>`)
     * @param {object} [options.databaseOptions] inline overrides sent with it,
     *   e.g. `{pair_bytes: 2}`. Only honoured when the directory is created.
     * @param {boolean|object} [options.binary] enable the byte-wise TCP
     *   protocol, optionally with `{uint, int, float}` default widths.
     * @param {{key: string, version: number}} [options.layout] the layout
     *   version marker to write on first connect and to verify afterwards.
     * @param {function} [options.now] clock, for tests
     * @param {function} [options.randomInt] id source, for tests
     */
    constructor(options = {}) {
        const {
            pool = null,
            database = null,
            databaseOptions = null,
            layout = null,
            now = null,
            randomInt = null,
            writeBatchSize = DEFAULT_WRITE_BATCH_SIZE,
            scanLimit = DEFAULT_SCAN_LIMIT,
            poolSize,
            host,
            port,
            binary,
            connectTimeoutMs,
            commandTimeoutMs,
            maxInFlight,
        } = options;

        this.databaseName = database;
        this.pool = pool || new CheetahPool({
            size: poolSize,
            host,
            port,
            binary,
            database,
            databaseOptions,
            connectTimeoutMs,
            commandTimeoutMs,
            maxInFlight,
        });
        this.ownsPool = !pool;
        this.layout = layout;
        this.now = now || (() => new Date());
        this.randomInt = randomInt || crypto.randomInt;
        this.writeBatchSize = Math.max(1, Number(writeBatchSize) || DEFAULT_WRITE_BATCH_SIZE);
        this.scanLimit = Math.max(1, Number(scanLimit) || DEFAULT_SCAN_LIMIT);
        this.connected = false;
        // One promise chain per key, so two read-modify-writes of the same
        // record cannot interleave. See `mutateJson`.
        this.mutationChains = new Map();
    }

    // -- lifecycle ----------------------------------------------------------

    /**
     * Run `fn` against a leased connection when the pool can lease one.
     *
     * A multi-command sequence that must not be interleaved — every
     * read-modify-write — has to go through here rather than through the pool's
     * own `send`, which spreads commands over connections.
     */
    async withConnection(fn) {
        if (typeof this.pool.withConnection === 'function') {
            return this.pool.withConnection(fn);
        }
        return fn(this.pool);
    }

    async connect() {
        if (this.connected) return this;
        if (typeof this.pool.connect === 'function') await this.pool.connect();
        await this.withConnection(async (conn) => {
            await this.assertLayout(conn);
            await this.onConnect(conn);
        });
        this.connected = true;
        return this;
    }

    /**
     * Write the layout marker on an empty database, or refuse a database
     * written by an incompatible codec.
     *
     * Failing loudly here is the point. A trie key layout is not
     * self-describing: a codec that changed its segment widths reads an older
     * database as a set of keys that simply do not match anything, which looks
     * like an empty result rather than an error.
     */
    async assertLayout(conn) {
        if (!this.layout) return;
        const { key, version, label = 'key layout' } = this.layout;
        const stored = await kv.getValue(conn, key);
        if (stored === null) {
            await kv.putValue(conn, key, String(version), { upsert: true });
            return;
        }
        if (Number.parseInt(stored, 10) !== Number(version)) {
            throw new CheetahError(
                `cheetah ${label} ${stored} is incompatible with codec ${version}; ` +
                're-ingest into a fresh database'
            );
        }
    }

    /** Subclass hook: runs on a leased connection after the layout check. */
    async onConnect() {}

    /** Subclass hook: drop every in-memory cache of database content. */
    clearCaches() {}

    async close() {
        if (!this.ownsPool || typeof this.pool.close !== 'function') return;
        await this.pool.close();
        this.connected = false;
    }

    /**
     * Drop and recreate this store's database.
     *
     * Destructive and immediate: `RESET_DB` recreates the directory, so
     * everything in it is gone — which is what a run establishing a corpus from
     * scratch wants, and never what a run adding to one wants.
     *
     * Every connection is re-pointed afterwards, not just the one that issued
     * the reset. That follows from the server's own model: database selection
     * is per-connection, and `Engine.ResetDatabase` closes the database and
     * drops it from the engine registry while `server.go` re-selects the fresh
     * one only for the socket that ran the command — so the other sockets hold
     * a pointer to a closed `Database`. Re-pointing them is one round trip each
     * and removes the question.
     */
    async reset() {
        const name = this.databaseName;
        if (!name) throw new CheetahError('cannot reset a store with no database name');
        await this.withConnection(async (conn) => {
            const response = await conn.send(`RESET_DB ${name}`);
            if (!response.ok) {
                throw new CheetahError(`cheetah RESET_DB ${name} failed: ${response.error || response.raw}`, {
                    response,
                });
            }
        });
        if (typeof this.pool.broadcast === 'function') {
            for (const response of await this.pool.broadcast(`DATABASE ${name}`)) {
                if (!response.ok) {
                    throw new CheetahError(
                        `cheetah could not re-select ${name} after a reset: ${response.error || response.raw}`,
                        { response }
                    );
                }
            }
        }
        // Everything cached described the database that just ceased to exist.
        this.clearCaches();
        this.mutationChains.clear();
        this.connected = false;
        return this.connect();
    }

    // -- values -------------------------------------------------------------

    getValue(key) {
        return kv.getValue(this.pool, key);
    }

    putValue(key, payload, options) {
        return kv.putValue(this.pool, key, payload, options);
    }

    getJson(key) {
        return kv.getJson(this.pool, key);
    }

    putJson(key, value, options) {
        return kv.putJson(this.pool, key, value, options);
    }

    /** `entries` as `{key, payload}`, written in `writeBatchSize` pages. */
    async putJsonBatched(entries, options) {
        let written = 0;
        for (let at = 0; at < entries.length; at += this.writeBatchSize) {
            written += await kv.putJsonBatch(
                this.pool,
                entries.slice(at, at + this.writeBatchSize),
                options
            );
        }
        return written;
    }

    deletePair(key) {
        return kv.deletePair(this.pool, key);
    }

    scan(prefix, options = {}) {
        return kv.scanPrefix(this.pool, prefix, { limit: this.scanLimit, ...options });
    }

    scanAll(prefix, options = {}) {
        return kv.scanAll(this.pool, prefix, { limit: this.scanLimit, ...options });
    }

    /** `scan` with the `continuations` reducer, yielding `{item, value}`. */
    async *scanJson(prefix, options = {}) {
        for await (const item of this.scan(prefix, { ...options, reducer: 'continuations' })) {
            yield { item, value: hydrateJson(item) };
        }
    }

    /**
     * Read-modify-write one JSON record, serialized per key.
     *
     * Cheetah has no compare-and-swap, so two concurrent increments of the same
     * counter would both read the old value and one would be lost. The chain
     * makes that impossible within a process; across processes it is still a
     * race, and a record written by several writers needs a different design.
     */
    async mutateJson(key, fallback, mutate) {
        const previous = this.mutationChains.get(key) || Promise.resolve();
        const run = previous.then(() => this.withConnection(async (conn) => {
            const current = await kv.getJson(conn, key);
            const next = mutate(current || { ...fallback });
            await kv.putJson(conn, key, next, { upsert: true });
            return next;
        }));
        const tail = run.then(() => undefined, () => undefined);
        this.mutationChains.set(key, tail);
        void tail.then(() => {
            if (this.mutationChains.get(key) === tail) this.mutationChains.delete(key);
        });
        return run;
    }

    // -- ids ----------------------------------------------------------------

    /**
     * A random id no record uses yet.
     *
     * Random rather than a `cfg:` counter because allocation must not depend on
     * a single process: worker threads have isolated module state, so a
     * process-local counter hands the same id to two writers.
     */
    async allocateRandomId(keyFor, { max = 0x100000000, attempts = 64 } = {}) {
        for (let attempt = 0; attempt < attempts; attempt += 1) {
            const candidate = this.randomInt(1, max);
            if (await kv.getJson(this.pool, keyFor(candidate)) === null) return candidate;
        }
        throw new CheetahError(`unable to allocate a collision-free id below ${max}`);
    }

    /** ISO-8601 stamp from the injected clock. */
    timestamp() {
        const value = this.now();
        return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
    }

    // -- accounting ---------------------------------------------------------

    /**
     * `PAIR_SUMMARY` for one prefix: how many records it holds and how many
     * payload bytes they carry.
     *
     * The byte figure is a **payload-retention** signal, not disk usage: the
     * server reports it without hydrating values, and it excludes trie, table
     * and filesystem overhead. It is the right input to a retention budget and
     * the wrong answer to "how big is this directory".
     */
    async pairSummary(prefix, depth = 1) {
        const response = await this.pool.command('PAIR_SUMMARY', prefix, depth);
        if (!response.ok) {
            throw new CheetahError(
                `cheetah PAIR_SUMMARY ${prefix} failed: ${response.error || response.raw}`,
                { response }
            );
        }
        return {
            prefix,
            count: numericField(response.fields, 'count', 0),
            payloadBytes: numericField(response.fields, 'total_payload_bytes', 0),
            response,
        };
    }

    /** `pairSummary` over several namespaces, plus their totals. */
    async namespaceSummary(prefixes) {
        const unique = [...new Set(prefixes)];
        const entries = await Promise.all(unique.map((prefix) => this.pairSummary(prefix, 1)));
        return {
            totalRecords: entries.reduce((sum, entry) => sum + entry.count, 0),
            totalPayloadBytes: entries.reduce((sum, entry) => sum + entry.payloadBytes, 0),
            namespaces: Object.fromEntries(entries.map((entry) => [entry.prefix, {
                prefix: entry.prefix,
                count: entry.count,
                payloadBytes: entry.payloadBytes,
            }])),
        };
    }

    // -- graph --------------------------------------------------------------

    setNode(node) {
        return graph.setNode(this.pool, node);
    }

    getNode(id) {
        return graph.getNode(this.pool, id);
    }

    setEdgeBatch(items, options) {
        return graph.setEdgeBatch(this.pool, items, options);
    }

    degree(options) {
        return graph.degree(this.pool, options);
    }

    recall(options) {
        return graph.recallBatched(this.pool, options);
    }

    recallAsync(options) {
        return graph.recallAsync(this.pool, options);
    }

    fetchRecall(jobId) {
        return graph.fetchRecall(this.pool, jobId);
    }

    awaitRecall(jobId, options) {
        return graph.awaitRecall(this.pool, jobId, options);
    }

    // -- record tables ------------------------------------------------------
    //
    // Thin delegations, like the graph ones above: a subclass that keeps a
    // declared table usually wants `defineRecordTable` once in `onConnect` and
    // the row calls everywhere else.

    defineRecordTable(table, fields, options = { ifNotExists: true }) {
        return records.define(this.pool, table, fields, options);
    }

    setRecord(table, key, values) {
        return records.setRow(this.pool, table, key, values);
    }

    getRecord(table, key, options) {
        return records.getRow(this.pool, table, key, options);
    }

    scanRecords(table, options) {
        return records.scanRows(this.pool, table, options);
    }

    recordSchema(table, options) {
        return records.schema(this.pool, table, options);
    }
}

module.exports = {
    CheetahDatabase,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_WRITE_BATCH_SIZE,
    hydrateJson,
};

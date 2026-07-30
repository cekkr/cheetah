// String → uint32 token vocabulary.
//
// Trie keys want short, fixed-width segments, and n-gram contexts want them
// fixed-width by construction. A 40-character sha1 (or any other free-form
// identifier) is neither, so clients end up interning identifiers into small
// integers. This is that allocator, with both directions of the mapping
// persisted.
//
// **Concurrency.** Cheetah has no compare-and-swap, so the counter is guarded
// by a single-flight promise chain in Node. That makes allocation safe within
// one process. Two processes — including two Node worker threads, whose
// CommonJS module state is isolated — allocating into the same database
// concurrently would race and can hand the same id to two names. Either confine
// allocation to one process, or move it server-side.

const { getValue, putValue } = require('./kv');
const { CheetahError } = require('./client');
const { hex } = require('./keys');

/** 0 is reserved as "no token", so ids start at 1. */
const FIRST_TOKEN = 1;
const MAX_TOKEN = 0xffffffff;

const DEFAULTS = Object.freeze({
    counterKey: 'cfg:next_token',
    tokenKey: (name) => `t:${name}`,
    reverseTokenKey: (token) => `r:${hex(token, 8, 'token')}`,
    cacheLimit: 200000,
});

class TokenVocabulary {
    /**
     * @param {object} conn a CheetahClient or CheetahPool
     * @param {object} [options]
     * @param {string} [options.counterKey] where the next free id is persisted
     * @param {function} [options.tokenKey] name → key holding its token
     * @param {function} [options.reverseTokenKey] token → key holding its name
     * @param {number} [options.cacheLimit] in-memory name→token entries
     */
    constructor(conn, options = {}) {
        const config = { ...DEFAULTS, ...options };
        this.conn = conn;
        this.counterKey = config.counterKey;
        this.tokenKey = config.tokenKey;
        this.reverseTokenKey = config.reverseTokenKey;
        this.cacheLimit = config.cacheLimit;
        this.forward = new Map();
        this.reverse = new Map();
        // Serializes every read-modify-write of the counter.
        this.allocationChain = Promise.resolve();
        this.pendingLookups = new Map();
    }

    #remember(name, token) {
        if (this.forward.size >= this.cacheLimit) {
            // Cheapest bounded policy that cannot thrash: drop the oldest insert.
            const oldest = this.forward.keys().next().value;
            const staleToken = this.forward.get(oldest);
            this.forward.delete(oldest);
            this.reverse.delete(staleToken);
        }
        this.forward.set(name, token);
        this.reverse.set(token, name);
        return token;
    }

    async #readCounter() {
        const raw = await getValue(this.conn, this.counterKey);
        if (raw === null) return FIRST_TOKEN;
        const value = Number.parseInt(raw, 10);
        if (!Number.isFinite(value) || value < FIRST_TOKEN) {
            throw new CheetahError(`cheetah ${this.counterKey} holds a non-token value: ${raw}`);
        }
        return value;
    }

    /**
     * Reserve `count` consecutive ids. Serialized against every other caller on
     * this instance; the counter is advanced *before* any mapping is written,
     * so a crash mid-allocation loses ids rather than reusing them.
     */
    async allocate(count = 1) {
        const amount = Math.max(1, Math.trunc(count));
        const run = this.allocationChain.then(async () => {
            const next = await this.#readCounter();
            if (next + amount - 1 > MAX_TOKEN) {
                throw new CheetahError('cheetah token vocabulary exhausted (uint32)');
            }
            await putValue(this.conn, this.counterKey, String(next + amount), { upsert: true });
            return next;
        });
        // Keep the chain alive even when this allocation rejects.
        this.allocationChain = run.then(() => undefined, () => undefined);
        return run;
    }

    /** The token for a name, allocating and persisting on first sight. */
    async tokenFor(name) {
        const cached = this.forward.get(name);
        if (cached !== undefined) return cached;

        const inFlight = this.pendingLookups.get(name);
        if (inFlight) return inFlight;

        const lookup = (async () => {
            const stored = await getValue(this.conn, this.tokenKey(name));
            if (stored !== null) {
                const token = Number.parseInt(stored, 10);
                if (!Number.isFinite(token)) {
                    throw new CheetahError(`cheetah token for ${name} is not numeric: ${stored}`);
                }
                return this.#remember(name, token);
            }
            const token = await this.allocate(1);
            // Reverse first: a reverse entry without a forward one is a readable
            // orphan, while a forward without a reverse breaks every
            // key→name explanation.
            await putValue(this.conn, this.reverseTokenKey(token), name, { upsert: true });
            await putValue(this.conn, this.tokenKey(name), String(token), { upsert: true });
            return this.#remember(name, token);
        })().finally(() => {
            this.pendingLookups.delete(name);
        });

        this.pendingLookups.set(name, lookup);
        return lookup;
    }

    /** The name behind a token, or null when unknown. */
    async nameFor(token) {
        const cached = this.reverse.get(token);
        if (cached !== undefined) return cached;
        const stored = await getValue(this.conn, this.reverseTokenKey(token));
        if (stored === null) return null;
        this.#remember(stored, token);
        return stored;
    }

    /** Resolve many names, preserving input order. */
    async tokensFor(names) {
        const requested = Array.isArray(names) ? names : [];
        const unique = [...new Set(requested)];
        const fresh = unique.filter((name) =>
            !this.forward.has(name) && !this.pendingLookups.has(name)
        );

        if (fresh.length > 0) {
            // Resolve all existing mappings concurrently, reserve one token
            // block for the true misses, then pipeline both mapping directions.
            // Register each name in pendingLookups before the first read can
            // finish so tokenFor() collapses onto this batch.
            const batch = (async () => {
                const storedValues = await Promise.all(
                    fresh.map((name) => getValue(this.conn, this.tokenKey(name)))
                );
                const resolved = new Map();
                const missing = [];
                storedValues.forEach((stored, index) => {
                    const name = fresh[index];
                    if (stored === null) {
                        missing.push(name);
                        return;
                    }
                    const token = Number.parseInt(stored, 10);
                    if (!Number.isFinite(token)) {
                        throw new CheetahError(
                            `cheetah token for ${name} is not numeric: ${stored}`
                        );
                    }
                    resolved.set(name, token);
                });

                if (missing.length > 0) {
                    const first = await this.allocate(missing.length);
                    await Promise.all(missing.flatMap((name, index) => {
                        const token = first + index;
                        resolved.set(name, token);
                        return [
                            putValue(this.conn, this.reverseTokenKey(token), name, { upsert: true }),
                            putValue(this.conn, this.tokenKey(name), String(token), { upsert: true }),
                        ];
                    }));
                }
                return resolved;
            })();

            for (const name of fresh) {
                const lookup = batch
                    .then((resolved) => this.#remember(name, resolved.get(name)))
                    .finally(() => {
                        if (this.pendingLookups.get(name) === lookup) {
                            this.pendingLookups.delete(name);
                        }
                    });
                this.pendingLookups.set(name, lookup);
            }
        }

        return Promise.all(requested.map((name) => this.tokenFor(name)));
    }
}

module.exports = {
    FIRST_TOKEN,
    MAX_TOKEN,
    TokenVocabulary,
    VOCABULARY_DEFAULTS: DEFAULTS,
};

// Value + name layer helpers — the two-step Cheetah write, in one place.
//
// Cheetah separates the bytes from the name: `INSERT` stores a payload and
// returns an absolute key, `PAIR_SET` binds a trie prefix to that key. There is
// no combined single-record command, so a write is two round trips and a read
// is two (`PAIR_GET` then `READ`). Everything that persists a record goes
// through here so the pairing is never half-done in one caller and whole in
// another.
//
// Byte transparency: the socket speaks latin1, which maps bytes 1:1 to code
// units, so UTF-8 payloads must be transcoded on the way in and out. A JSON
// document with an accented string in it is the common case.
//
// Every function takes a `conn` — a `CheetahClient`, a `CheetahPool`, or
// anything else exposing `send`/`command`/`commandOrThrow`.

const {
    buildCommand,
    encodeArgument,
    parseCursor,
    parseItems,
    rawArgument,
} = require('./protocol');
const { CheetahError } = require('./client');

/** UTF-8 text → the latin1 spelling that puts those exact bytes on the wire. */
function toWire(text) {
    return Buffer.from(String(text), 'utf8').toString('latin1');
}

/** The inverse: latin1 wire text → the UTF-8 string it encodes. */
function fromWire(text) {
    return Buffer.from(String(text), 'latin1').toString('utf8');
}

function assertSingleLine(payload) {
    if (payload.includes('\n') || payload.includes('\r')) {
        throw new CheetahError('cheetah payloads must not contain a newline');
    }
    if (payload.length === 0) {
        throw new CheetahError('cheetah payloads must not be empty (INSERT answers missing_value)');
    }
    return payload;
}

/** Resolve a name to its absolute value key, or null when unbound. */
async function getAbsoluteKey(conn, key) {
    const response = await conn.command('PAIR_GET', key);
    if (response.ok) {
        const absKey = Number.parseInt(response.fields.key, 10);
        return Number.isFinite(absKey) ? absKey : null;
    }
    if (response.error && response.error.startsWith('not_found')) return null;
    throw new CheetahError(`cheetah PAIR_GET ${key} failed: ${response.error || response.raw}`, {
        response,
    });
}

/** Hydrate the bytes behind an absolute key. Returns null when deleted. */
async function readAbsoluteKey(conn, absKey) {
    const response = await conn.send(buildCommand('READ', absKey));
    if (response.ok) return fromWire(response.fields.value ?? '');
    if (response.error && response.error.startsWith('key_not_found')) return null;
    throw new CheetahError(`cheetah READ ${absKey} failed: ${response.error || response.raw}`, {
        response,
    });
}

/**
 * Bind `key` to `payload`.
 *
 * `upsert: false` (the default, and what a bulk ingest wants) is the
 * two-round-trip blind write: INSERT then PAIR_SET. Rebinding a name this way
 * leaves the old value readable by its own absolute key — harmless for
 * write-once rows, wasteful for records that are rewritten, which is what
 * `upsert: true` is for: it EDITs the existing value in place and keeps the
 * absolute key stable.
 */
async function putValue(conn, key, payload, { upsert = false } = {}) {
    const wire = assertSingleLine(toWire(payload));
    if (upsert) {
        const existing = await getAbsoluteKey(conn, key);
        if (existing !== null) {
            const edited = await conn.send(`EDIT ${existing} ${wire}`);
            if (!edited.ok) {
                throw new CheetahError(`cheetah EDIT ${existing} failed: ${edited.error || edited.raw}`, {
                    response: edited,
                });
            }
            return existing;
        }
    }
    const inserted = await conn.send(`INSERT ${wire}`);
    if (!inserted.ok) {
        throw new CheetahError(`cheetah INSERT failed: ${inserted.error || inserted.raw}`, {
            response: inserted,
        });
    }
    const absKey = Number.parseInt(inserted.fields.key, 10);
    if (!Number.isFinite(absKey)) {
        throw new CheetahError(`cheetah INSERT returned no key: ${inserted.raw}`, { response: inserted });
    }
    await conn.commandOrThrow('PAIR_SET', key, absKey);
    return absKey;
}

/**
 * Bind many keys in **one** round trip.
 *
 * `putValue` is two requests per record — INSERT then PAIR_SET — which is the
 * right separation but the wrong cost for a bulk ingest: ~7,800 records became
 * ~15,600 requests, with the server process idling at about a quarter of a core
 * while the client waited on latency. `PAIR_PUT_BATCH` does both server-side for
 * a whole page of records.
 *
 * It is **not** a transaction. Items are independent and applied in order, so a
 * failure part-way leaves the earlier ones written; the server reports
 * `applied`/`failed` and this throws on any failure rather than returning a
 * count nobody checks. A half-written batch is an index with holes in it.
 *
 * `hexEncodeItems` exists because the items travel as JSON inside a
 * whitespace-split `key=value` token: a payload containing a newline, or bytes
 * that are not valid UTF-8, has to go as `x<hex>`. Plain keys and JSON payloads
 * are neither, so the default is off and the wire stays readable.
 */
async function putValuesBatch(conn, entries, { hidden = false, hexEncodeItems = false } = {}) {
    if (!Array.isArray(entries) || entries.length === 0) return 0;
    const items = entries.map(({ key, payload }) => {
        const value = assertSingleLine(toWire(payload));
        return hexEncodeItems
            ? {
                k: `x${Buffer.from(String(key), 'latin1').toString('hex')}`,
                v: `x${Buffer.from(value, 'latin1').toString('hex')}`,
            }
            : { k: String(key), v: value };
    });
    const encoded = Buffer.from(JSON.stringify(items), 'latin1').toString('base64');
    const response = await conn.send(
        `PAIR_PUT_BATCH items=${encoded}${hidden ? ' hidden=1' : ''}`
    );
    if (!response.ok) {
        throw new CheetahError(
            `cheetah PAIR_PUT_BATCH of ${items.length} failed: ${response.error || response.raw}`,
            { response }
        );
    }
    const applied = Number.parseInt(response.fields.applied, 10);
    const failed = Number.parseInt(response.fields.failed, 10);
    if (!(applied === items.length) || failed > 0) {
        throw new CheetahError(
            `cheetah PAIR_PUT_BATCH applied ${applied}/${items.length} (failed=${failed}): ` +
            `${response.fields.first_error || 'no reason reported'}`,
            { response }
        );
    }
    return applied;
}

/** `putValuesBatch` for JSON payloads. */
async function putJsonBatch(conn, entries, options) {
    return putValuesBatch(
        conn,
        entries.map(({ key, payload }) => ({ key, payload: JSON.stringify(payload) })),
        options
    );
}

/** Read the payload bound to `key`, or null when the name is unbound. */
async function getValue(conn, key) {
    const absKey = await getAbsoluteKey(conn, key);
    if (absKey === null) return null;
    return readAbsoluteKey(conn, absKey);
}

async function putJson(conn, key, value, options) {
    return putValue(conn, key, JSON.stringify(value), options);
}

async function getJson(conn, key) {
    const raw = await getValue(conn, key);
    if (raw === null) return null;
    try {
        return JSON.parse(raw);
    } catch (error) {
        throw new CheetahError(`cheetah value at ${key} is not JSON: ${error.message}`);
    }
}

/**
 * Unbind one name. Returns the number of pairs removed (0 when absent).
 *
 * The key travels through `encodeArgument` because `DEL` speaks the micro
 * dialect, which splits on whitespace: a key holding a space, or starting with
 * `x`, survives only in its `x<hex>` spelling.
 */
async function deletePair(conn, key) {
    const response = await conn.send(`DEL pairs key=${encodeArgument(key)}`);
    if (response.ok) {
        const deleted = Number.parseInt(response.fields.deleted, 10);
        return Number.isFinite(deleted) ? deleted : 1;
    }
    if (response.error && /not_found/.test(response.error)) return 0;
    throw new CheetahError(`cheetah DEL pairs ${key} failed: ${response.error || response.raw}`, {
        response,
    });
}

/**
 * One page of a `PAIR_SCAN` (or `PAIR_REDUCE`, with `reducer`).
 *
 * The cursor is handed back **verbatim** through `rawArgument`. Encoding it
 * would hex-encode the `x<hex>` token Cheetah returned, and the server would
 * resume from a prefix that does not exist — a sweep that quietly stops after
 * the first page instead of failing.
 */
async function scanPage(conn, prefix, { limit = 500, cursor = null, reducer = null, includeHidden = false } = {}) {
    const options = includeHidden ? 'include_hidden=1' : undefined;
    const cursorArg = cursor ? rawArgument(cursor) : undefined;
    const response = reducer
        ? await conn.command('PAIR_REDUCE', reducer, prefix, limit, cursorArg, options)
        : await conn.command('PAIR_SCAN', prefix, limit, cursorArg, options);
    if (!response.ok) {
        throw new CheetahError(`cheetah scan of ${prefix} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return {
        items: parseItems(response.fields.items),
        cursor: parseCursor(response.fields),
        response,
    };
}

/**
 * Every entry under `prefix`, one page at a time. `maxItems` bounds a sweep
 * that would otherwise be unbounded — a hot namespace holds thousands of rows.
 */
async function* scanPrefix(conn, prefix, { limit = 500, maxItems = Infinity, reducer = null, includeHidden = false } = {}) {
    let cursor = null;
    let yielded = 0;
    do {
        const page = await scanPage(conn, prefix, { limit, cursor, reducer, includeHidden });
        for (const item of page.items) {
            if (yielded >= maxItems) return;
            yielded += 1;
            yield item;
        }
        cursor = page.cursor;
    } while (cursor);
}

/** `scanPrefix` collected into an array. */
async function scanAll(conn, prefix, options) {
    const items = [];
    for await (const item of scanPrefix(conn, prefix, options)) items.push(item);
    return items;
}

module.exports = {
    deletePair,
    fromWire,
    getAbsoluteKey,
    getJson,
    getValue,
    putJson,
    putJsonBatch,
    putValue,
    putValuesBatch,
    readAbsoluteKey,
    scanAll,
    scanPage,
    scanPrefix,
    toWire,
};

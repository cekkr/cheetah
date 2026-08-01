// Prediction tables — the `PREDICT_*` family.
//
// A prediction table maps a prefix (`key=`) to candidate values with
// probabilities and per-context weights, and can be *trained*: `PREDICT_TRAIN`
// moves the stored weights toward a target, which is what makes this a learned
// table rather than a cache.
//
// The family is thin on purpose here. Cheetah owns the numerics; the binder owns
// the encodings that are easy to get wrong — the `key=value` dialect's
// whitespace rule, and the base64-JSON envelopes that `weights=`, `windows=`,
// `key_windows=` and `items=` travel in.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, decodePayload, numericField } = require('./protocol');

/** JSON → the base64 spelling a `key=value` token can carry. */
function encodeJsonArgument(value) {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
}

function jsonField(value) {
    if (value === undefined || value === null) return null;
    return typeof value === 'string' ? value : encodeJsonArgument(value);
}

function commaList(value) {
    if (value === undefined || value === null) return null;
    const parts = (Array.isArray(value) ? value : String(value).split(','))
        .map((entry) => String(entry).trim())
        .filter(Boolean);
    return parts.length > 0 ? parts.join(',') : null;
}

async function sendOrThrow(conn, line, what) {
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah ${what} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return response;
}

/** Declare a candidate value for a prefix with its probability. The write path. */
function buildSet({ key, value, prob, weights = null, table = null }) {
    return buildKeyValueCommand('PREDICT_SET', {
        key,
        value,
        prob,
        weights: jsonField(weights),
        table,
    });
}

function buildQuery({ key, keys = null, ctx = null, windows = null, keyWindows = null, merge = null, table = null }) {
    return buildKeyValueCommand('PREDICT_QUERY', {
        key,
        keys: commaList(keys),
        ctx: jsonField(ctx),
        windows: jsonField(windows),
        key_windows: jsonField(keyWindows),
        merge,
        table,
    });
}

function buildTrain({ key, target, ctx = null, lr = null, negatives = null, table = null }) {
    return buildKeyValueCommand('PREDICT_TRAIN', {
        key,
        target,
        ctx: jsonField(ctx),
        lr,
        negatives: commaList(negatives),
        table,
    });
}

function buildContextAdjust({ key, ctx, mode = null, strength = null, table = null }) {
    return buildKeyValueCommand('PREDICT_CTX', { key, ctx: jsonField(ctx), mode, strength, table });
}

function buildInherit({ key, target, sources, merge = null, table = null }) {
    return buildKeyValueCommand('PREDICT_INHERIT', {
        key,
        target,
        sources: commaList(sources),
        merge,
        table,
    });
}

function buildInheritBatch(items, { key = null, merge = null, table = null } = {}) {
    if (!Array.isArray(items) || items.length === 0) {
        throw new CheetahError('cheetah PREDICT_INHERIT_BATCH requires items');
    }
    return buildKeyValueCommand('PREDICT_INHERIT_BATCH', {
        items: encodeJsonArgument(items),
        key,
        merge,
        table,
    });
}

async function setValue(conn, options) {
    return sendOrThrow(conn, buildSet(options), 'PREDICT_SET');
}

/**
 * Evaluate one or many prefixes and merge their probability windows. The
 * numeric shape of the payload is the server's contract, not this binder's.
 */
async function query(conn, options) {
    const response = await sendOrThrow(conn, buildQuery(options), 'PREDICT_QUERY');
    return {
        count: numericField(response.fields, 'count', 0),
        backend: response.fields.backend || null,
        payload: decodePayload(response.fields),
        response,
    };
}

/** Move the stored weights toward `target`. Persistent learning. */
async function train(conn, options) {
    return sendOrThrow(conn, buildTrain(options), 'PREDICT_TRAIN');
}

/** A nudge to this query, not a lesson: `PREDICT_CTX` trains nothing. */
async function contextAdjust(conn, options) {
    return sendOrThrow(conn, buildContextAdjust(options), 'PREDICT_CTX');
}

/**
 * Seed a new value by merging existing ones under the same prefix. Every source
 * must already exist under `key`, or the command answers
 * `inherit_sources_missing` — a statement about the table, not a transport
 * failure.
 */
async function inherit(conn, options) {
    return sendOrThrow(conn, buildInherit(options), 'PREDICT_INHERIT');
}

/**
 * The same merge for many targets in one call. Submit it through `jobs.js` when
 * the batch is large: it is one of the two commands the server accepts as a
 * detached job.
 */
async function inheritBatch(conn, items, options) {
    return sendOrThrow(conn, buildInheritBatch(items, options), 'PREDICT_INHERIT_BATCH');
}

/**
 * Read or switch which merger a table uses. The `gpu` path is
 * `webgpu-simulated` — CPU fan-out, not a real WebGPU binding — so treat a
 * switch as a scheduling choice, not an accelerator.
 */
async function backend(conn, { mode = null, table = null } = {}) {
    const response = await sendOrThrow(
        conn,
        buildKeyValueCommand('PREDICT_BACKEND', { mode, table }),
        'PREDICT_BACKEND'
    );
    return {
        backend: response.fields.backend || response.fields.mode || null,
        table: response.fields.table || null,
        response,
    };
}

/** Compare the two mergers on this host, so the choice above is measured. */
async function bench(conn, { samples, window, table = null }) {
    const response = await sendOrThrow(
        conn,
        buildKeyValueCommand('PREDICT_BENCH', { samples, window, table }),
        'PREDICT_BENCH'
    );
    return { fields: { ...response.fields }, payload: decodePayload(response.fields), response };
}

module.exports = {
    backend,
    bench,
    buildContextAdjust,
    buildInherit,
    buildInheritBatch,
    buildQuery,
    buildSet,
    buildTrain,
    contextAdjust,
    encodeJsonArgument,
    inherit,
    inheritBatch,
    query,
    setValue,
    train,
};

// Key-building primitives.
//
// Cheetah assigns no meaning to a key: it walks the raw bytes through the pair
// trie. That freedom is also the trap, because in a trie **the key bytes are
// the index** — a layout change means rebuilding the database. Every client
// therefore ends up needing the same handful of primitives, and getting one of
// them subtly wrong is expensive, so they live here rather than in each client.
//
// Three rules they enforce:
//
//   1. Fixed-width, zero-padded lowercase hex for every numeric segment.
//      PAIR_SCAN is byte-ordered, so `String(n)` sorts `10` before `9`.
//   2. `/` separates segments and never appears inside one. Hex guarantees it.
//   3. No key may start with a byte the server reserves — see
//      `protocol.assertUnreservedKey`.
//
// This module owns no namespace of its own. Deciding what a key *means* is the
// application's job; this only makes the bytes well-formed.

const crypto = require('crypto');
const { assertUnreservedKey } = require('./protocol');

/** The conventional segment separator. Nothing forces it; consistency helps. */
const SEGMENT = '/';

/**
 * **Bucketing happens in integers, not floats.**
 *
 * A continuous value that reaches a key is first quantized to `KEY_QUANTUM`,
 * then bucketed by integer division.
 *
 * This is not tidiness. With float division, `(v - tol) / width` lands on
 * `224.99999999999997` where exact arithmetic gives `225`, so a tolerance sweep
 * silently widens from two buckets to three for roughly half of all probes —
 * when the tolerance is an exact multiple of the rounding grid, boundary
 * alignment is the common case, not the rare one. Integer division removes the
 * whole class of error.
 */
const KEY_QUANTUM = 1e-6;
const QUANTA_PER_UNIT = 1e6;

const LOWER_HEX = /^[0-9a-f]+$/;
const SHA1_HEX = /^[0-9a-f]{40}$/;

/** A non-negative integer as fixed-width lowercase hex. Throws on overflow. */
function hex(value, width, what = 'value') {
    if (!Number.isInteger(value) || value < 0) {
        throw new RangeError(`${what} must be a non-negative integer, got ${value}`);
    }
    const text = value.toString(16);
    if (text.length > width) {
        throw new RangeError(`${what} ${value} does not fit in ${width} hex digits`);
    }
    return text.padStart(width, '0');
}

/** Inverse of `hex`. Rejects anything that is not lowercase hex. */
function unhex(text, what = 'value') {
    if (typeof text !== 'string' || !LOWER_HEX.test(text)) {
        throw new TypeError(`${what} is not lowercase hex: ${text}`);
    }
    return Number.parseInt(text, 16);
}

/** SHA-1 hex of a string — the usual way to key free-form text like a path. */
function sha1(text) {
    return crypto.createHash('sha1').update(String(text)).digest('hex');
}

function assertSha1(value, what = 'value') {
    if (typeof value !== 'string' || !SHA1_HEX.test(value)) {
        throw new TypeError(`${what} must be a 40-char lowercase sha1 hex string, got ${value}`);
    }
    return value;
}

/** Join pre-formatted segments with `SEGMENT`. */
function joinSegments(...segments) {
    return segments.join(SEGMENT);
}

/** A value in its integer quantum domain. The only entry point to bucketing. */
function quantize(value) {
    if (!Number.isFinite(value)) throw new TypeError(`cannot quantize non-finite value ${value}`);
    return Math.round(value * QUANTA_PER_UNIT);
}

/**
 * `floor(quantize(v) / widthUnits)`. Signed; the caller applies any bias needed
 * to keep the hex spelling byte-ordered. `widthUnits` is a bucket width **in
 * quanta**, never in value units.
 */
function bucketize(value, widthUnits) {
    if (!Number.isInteger(widthUnits) || widthUnits <= 0) {
        throw new RangeError(`bucket width must be a positive integer of quanta, got ${widthUnits}`);
    }
    return Math.floor(quantize(value) / widthUnits);
}

/**
 * The distinct buckets that can hold a row within `toleranceUnits` of `value`.
 * Ordered ascending; length 1 or 2 while `widthUnits >= 2 * toleranceUnits`,
 * which is the property a caller's frozen widths must keep so that sweeping
 * `bucket(v - tol) … bucket(v + tol)` cannot miss a matching row.
 */
function bucketSweep(value, widthUnits, toleranceUnits) {
    const centre = quantize(value);
    const low = Math.floor((centre - toleranceUnits) / widthUnits);
    const high = Math.floor((centre + toleranceUnits) / widthUnits);
    const buckets = [];
    for (let bucket = low; bucket <= high; bucket += 1) buckets.push(bucket);
    return buckets;
}

/**
 * Validate a key against the reserved namespaces and the bare-argument rules.
 * Cheap enough to call on every write in tests.
 */
function assertValidKey(key) {
    assertUnreservedKey(key);
    if (/\s/.test(key)) throw new Error(`cheetah key must not contain whitespace: ${key}`);
    if (String(key).startsWith('x')) {
        throw new Error(`cheetah key must not start with 'x' (it would be read as hex): ${key}`);
    }
    return key;
}

module.exports = {
    KEY_QUANTUM,
    QUANTA_PER_UNIT,
    SEGMENT,
    assertSha1,
    assertValidKey,
    bucketSweep,
    bucketize,
    hex,
    joinSegments,
    quantize,
    sha1,
    unhex,
};

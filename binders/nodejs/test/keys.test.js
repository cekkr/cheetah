// Key-primitive tests.
//
// The bucketing cases are the reason this module exists: they are the ones that
// fail silently rather than loudly when the arithmetic drifts into floats.

const test = require('node:test');
const assert = require('node:assert/strict');

const {
    KEY_QUANTUM,
    assertSha1,
    assertValidKey,
    bucketSweep,
    bucketize,
    hex,
    joinSegments,
    quantize,
    sha1,
    unhex,
} = require('../lib/keys');

test('hex is fixed-width, zero-padded and byte-ordered', () => {
    assert.equal(hex(10, 8, 'token'), '0000000a');
    assert.equal(hex(0, 4, 'seq'), '0000');
    // The property the padding exists for: a scan is byte-ordered, so 9 must
    // sort before 10 in the spelling as well as in the number.
    assert.ok(hex(9, 4, 'n') < hex(10, 4, 'n'));
    assert.ok('9' > '10');
});

test('hex refuses values it cannot spell', () => {
    assert.throws(() => hex(0x10000, 4, 'token'), /does not fit in 4 hex digits/);
    assert.throws(() => hex(-1, 4, 'token'), /non-negative integer/);
    assert.throws(() => hex(1.5, 4, 'token'), /non-negative integer/);
});

test('unhex inverts hex and rejects anything else', () => {
    assert.equal(unhex('0000000a', 'token'), 10);
    assert.throws(() => unhex('0000000A', 'token'), /not lowercase hex/);
    assert.throws(() => unhex('zz', 'token'), /not lowercase hex/);
});

test('joinSegments and sha1 build a readable free-text key', () => {
    assert.equal(joinSegments('ab', 'cd', 'ef'), 'ab/cd/ef');
    assert.equal(sha1('hello').length, 40);
    assert.equal(assertSha1(sha1('hello')), sha1('hello'));
    assert.throws(() => assertSha1('nope'), /sha1 hex string/);
});

test('quantize maps a value onto the integer grid', () => {
    assert.equal(quantize(0.45), 450000);
    assert.equal(quantize(-0.0015), -1500);
    assert.equal(quantize(KEY_QUANTUM), 1);
    assert.throws(() => quantize(Number.NaN), /non-finite/);
});

test('bucketing stays exact where float division does not', () => {
    // 0.45 with a tolerance of 1e-4 and a width of 2e-4 (200 quanta): exact
    // arithmetic puts (450000 - 100) / 200 on 2249.5 → 2249, and 450000/200 on
    // 2250, so the sweep is two buckets. In floats the same expression lands on
    // 2249.9999999999998 for the upper end and the sweep silently widens.
    assert.deepEqual(bucketSweep(0.45, 200, 100), [2249, 2250]);
    assert.equal(bucketize(0.45, 200), 2250);
    // A value whose whole tolerance interval sits inside one bucket needs only
    // that bucket. At tolerance = width/2 the interval is exactly one bucket
    // wide, so it meets two unless it is perfectly aligned — which is why a
    // width must be *at least* twice the tolerance, not exactly twice it.
    assert.deepEqual(bucketSweep(0.4501, 200, 50), [2250]);
    assert.deepEqual(bucketSweep(0.4501, 200, 100), [2250, 2251]);
});

test('a sweep contains every bucket within tolerance of the value', () => {
    const width = 200;
    const tolerance = 100;
    for (let step = 0; step < 500; step += 1) {
        const value = 0.02 + step * 0.00086;
        const sweep = bucketSweep(value, width, tolerance);
        assert.ok(sweep.length <= 2, `sweep of ${value} widened to ${sweep.length}`);
        for (const offset of [-tolerance, 0, tolerance]) {
            const neighbour = Math.floor((quantize(value) + offset) / width);
            assert.ok(sweep.includes(neighbour), `sweep of ${value} misses ${neighbour}`);
        }
    }
});

test('bucketize refuses a width expressed in value units', () => {
    // 2e-4 is a width in *units*; the API wants it in quanta (200).
    assert.throws(() => bucketize(0.45, 2e-4), /positive integer of quanta/);
});

test('assertValidKey rejects keys the wire cannot carry', () => {
    assert.equal(assertValidKey('f:0000000a/0064'), 'f:0000000a/0064');
    assert.throws(() => assertValidKey('has space'), /whitespace/);
    assert.throws(() => assertValidKey('xkey'), /must not start with 'x'/);
    assert.throws(() => assertValidKey('graph/nodes'), /reserved namespace/);
});

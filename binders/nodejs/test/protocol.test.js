// Response-parser and argument-encoder tests.
//
// Every expected line here is a format taken from README.md or emitted by
// src/database.go, not invented. When the server changes a response shape,
// these are the tests that should fail first.

const test = require('node:test');
const assert = require('node:assert/strict');

const {
    assertUnreservedKey,
    buildCommand,
    buildKeyValueCommand,
    decodeHexKey,
    decodeItemPayload,
    decodePayload,
    encodeArgument,
    isBareSafeArgument,
    numericField,
    parseBranches,
    parseCursor,
    parseItems,
    parseResponse,
    rawArgument,
} = require('../lib/protocol');

test('parseResponse reads a bare success flag', () => {
    const response = parseResponse('SUCCESS,pair_set');
    assert.equal(response.ok, true);
    assert.deepEqual(response.flags, ['pair_set']);
    assert.deepEqual({ ...response.fields }, {});
});

test('parseResponse reads key=value fields', () => {
    const response = parseResponse('SUCCESS,key=42');
    assert.equal(response.ok, true);
    assert.equal(response.fields.key, '42');
    assert.equal(numericField(response.fields, 'key'), 42);
});

test('parseResponse gives value= the rest of the line, commas included', () => {
    const response = parseResponse('SUCCESS,size=27,value={"a":1,"b":"x,y"}');
    assert.equal(response.fields.size, '27');
    assert.equal(response.fields.value, '{"a":1,"b":"x,y"}');
});

test('parseResponse keeps an ERROR reason whole', () => {
    const response = parseResponse('ERROR,value_size_mismatch (expected 16, got 17)');
    assert.equal(response.ok, false);
    assert.equal(response.status, 'ERROR');
    assert.equal(response.error, 'value_size_mismatch (expected 16, got 17)');
});

test('parseResponse recognises a not_found error', () => {
    const response = parseResponse('ERROR,not_found');
    assert.equal(response.ok, false);
    assert.equal(response.error, 'not_found');
});

test('parseResponse marks PENDING job lines', () => {
    const response = parseResponse('PENDING,job=reduce_1,progress=42.00');
    assert.equal(response.ok, false);
    assert.equal(response.pending, true);
    assert.equal(response.fields.progress, '42.00');
});

test('parseResponse tolerates a trailing newline', () => {
    assert.equal(parseResponse('SUCCESS,key=7\n').fields.key, '7');
});

test('parseItems splits a PAIR_SCAN page and decodes the hex names', () => {
    // From README.md: ctx:BERLIN → 1, ctx:LISBON → 5.
    const line = 'SUCCESS,count=2,items=6374783a4245524c494e:1;6374783a4c4953424f4e:5';
    const response = parseResponse(line);
    const items = parseItems(response.fields.items);
    assert.equal(items.length, 2);
    assert.equal(items[0].key, 'ctx:BERLIN');
    assert.equal(items[0].absKey, 1);
    assert.equal(items[1].key, 'ctx:LISBON');
    assert.equal(items[1].absKey, 5);
    assert.equal(items[0].payloadBase64, null);
});

test('parseItems reads the third PAIR_REDUCE component as a payload', () => {
    const line = 'SUCCESS,reducer=counts,count=1,items=6374783a4245524c494e:1:Y3R4OkJFUkxJTg==';
    const response = parseResponse(line);
    const [item] = parseItems(response.fields.items);
    assert.equal(item.key, 'ctx:BERLIN');
    assert.equal(decodeItemPayload(item).toString('utf8'), 'ctx:BERLIN');
});

test('parseItems returns an empty page for a missing items field', () => {
    assert.deepEqual(parseItems(parseResponse('SUCCESS,count=0').fields.items), []);
});

test('parseCursor returns the token to hand back, and null when exhausted', () => {
    const paged = parseResponse('SUCCESS,count=3,next_cursor=x6374783a4d554e494348,items=6374783a4245524c494e:1');
    assert.equal(parseCursor(paged.fields), 'x6374783a4d554e494348');
    assert.equal(decodeHexKey(parseCursor(paged.fields)).toString('latin1'), 'ctx:MUNICH');

    assert.equal(parseCursor(parseResponse('SUCCESS,count=2').fields), null);
    assert.equal(parseCursor(parseResponse('SUCCESS,count=2,next_cursor=*').fields), null);
});

test('decodePayload base64-decodes a record payload into JSON', () => {
    const payload = { associations: [{ id: 'm00000001', score: 0.396 }] };
    const encoded = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64');
    const response = parseResponse(`SUCCESS,command=GRAPH_RECALL,count=1,payload=${encoded}`);
    assert.deepEqual(decodePayload(response.fields), payload);
    assert.equal(decodePayload(parseResponse('SUCCESS,count=0').fields), null);
});

test('parseBranches reads the PAIR_SUMMARY fan-out histogram', () => {
    const line = 'SUCCESS,command=PAIR_SUMMARY,count=5,total_payload_bytes=91,branches=50:2;42:1;4c:1';
    const response = parseResponse(line);
    assert.equal(numericField(response.fields, 'total_payload_bytes'), 91);
    assert.deepEqual(parseBranches(response.fields.branches), [
        { byteHex: '50', byte: 0x50, count: 2 },
        { byteHex: '42', byte: 0x42, count: 1 },
        { byteHex: '4c', byte: 0x4c, count: 1 },
    ]);
});

test('encodeArgument leaves an ordinary key bare', () => {
    assert.equal(encodeArgument('f:0000000a/0064/1388/1388/'), 'f:0000000a/0064/1388/1388/');
    assert.equal(isBareSafeArgument(Buffer.from('d:abc')), true);
});

test('encodeArgument escapes anything the server would misread', () => {
    // A leading `x` is decoded as hex by src/helpers.go → parseValue.
    assert.equal(encodeArgument('x:hello'), 'x783a68656c6c6f');
    // Spaces would be split into separate positional arguments.
    assert.equal(encodeArgument('a b'), 'x612062');
    // `*` means "the whole trie" to PAIR_SCAN.
    assert.equal(encodeArgument('*'), 'x2a');
    // Non-printable bytes cannot travel raw.
    assert.equal(encodeArgument(Buffer.from([0x01, 0x02])), 'x0102');
});

test('encodeArgument round-trips through the hex spelling', () => {
    for (const sample of ['x:hello', 'a b', '*', 'plain', 'é']) {
        const encoded = encodeArgument(Buffer.from(sample, 'utf8'));
        const decoded = encoded.startsWith('x')
            ? decodeHexKey(encoded)
            : Buffer.from(encoded, 'latin1');
        assert.equal(decoded.toString('utf8'), sample);
    }
});

test('buildCommand assembles a positional command line', () => {
    assert.equal(buildCommand('PAIR_SET', 'ctx:BERLIN', 1), 'PAIR_SET ctx:BERLIN 1');
    assert.equal(buildCommand('PAIR_SCAN', 'f:0000000a/', 500), 'PAIR_SCAN f:0000000a/ 500');
    assert.equal(buildCommand('PAIR_GET', 'x:legacy'), 'PAIR_GET x783a6c6567616379');
});

test('buildCommand cannot smuggle a newline into the line protocol', () => {
    // A newline would terminate the command early and desynchronize the FIFO,
    // so the encoder escapes it rather than passing it through.
    assert.equal(buildCommand('PAIR_SET', 'a\nb', 1), 'PAIR_SET x610a62 1');
});

test('a next_cursor handed back through rawArgument is not re-encoded', () => {
    // The cursor comes back as `x<hex>`, which encodeArgument would treat as a
    // key that happens to start with `x` and hex-encode again. The server would
    // then resume from a prefix that does not exist and the sweep would stop
    // after one page instead of failing.
    const cursor = parseCursor(parseResponse('SUCCESS,count=1,next_cursor=x6374783a4d554e494348').fields);
    assert.equal(
        buildCommand('PAIR_SCAN', 'f:0000000a/', 500, rawArgument(cursor)),
        'PAIR_SCAN f:0000000a/ 500 x6374783a4d554e494348'
    );
    assert.notEqual(
        buildCommand('PAIR_SCAN', 'f:', 5, cursor),
        buildCommand('PAIR_SCAN', 'f:', 5, rawArgument(cursor))
    );
});

test('buildKeyValueCommand assembles the GRAPH_* dialect and drops empties', () => {
    assert.equal(
        buildKeyValueCommand('GRAPH_RECALL', { seeds: 'n0000000a,n0000000b', hops: 2, limit: '' }),
        'GRAPH_RECALL seeds=n0000000a,n0000000b hops=2'
    );
});

test('assertUnreservedKey rejects the server reserved prefixes', () => {
    assert.equal(assertUnreservedKey('f:0000000a/'), 'f:0000000a/');
    assert.throws(() => assertUnreservedKey('\x01gn:node'), /reserved control-byte/);
    assert.throws(() => assertUnreservedKey('\x05gt:term'), /reserved control-byte/);
    assert.throws(() => assertUnreservedKey('graph/edges'), /reserved namespace/);
    assert.throws(() => assertUnreservedKey('idx/whatever'), /reserved namespace/);
    assert.throws(() => assertUnreservedKey(''), /must not be empty/);
});

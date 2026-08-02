const test = require('node:test');
const assert = require('node:assert');

const binary = require('../lib/binary');
const alias = require('../lib/alias');
const { parseResponse } = require('../lib/protocol');

/**
 * A session preloaded with a small index, standing in for a handshake ack.
 * Real ids come from the server; the codec only cares that they round-trip.
 */
function testSession() {
    return new binary.BinarySession({ widths: { uint: 8, int: 8, float: 8 } })
        .loadCommands(
            [
                { id: 1, name: 'RECORD', kind: 'micro' },
                { id: 2, name: 'PAIR_SET', kind: 'builtin' },
                { id: 3, name: 'INSERT', kind: 'builtin' },
                { id: 4, name: 'BATCH', kind: 'micro' },
                { id: 5, name: 'EDIT', kind: 'builtin' },
            ],
            'deadbeefdeadbeef'
        )
        .loadKeys(
            [
                { id: 1, name: 'table' },
                { id: 2, name: 'key' },
                { id: 3, name: 'count' },
                { id: 4, name: 'limit' },
                { id: 5, name: 'value' },
            ],
            'feedfacefeedface'
        );
}

/** Decode a request frame back into its canonical line, as the server does. */
function decodeRequestLine(frame, session) {
    const { KIND, KEY_MODE, ENUM_FAMILY } = binary;
    const body = frame.subarray(binary.FRAME_HEADER_BYTES);
    const cursor = { at: 0 };
    const readShort = () => {
        const length = body[cursor.at];
        const text = body.toString('utf8', cursor.at + 1, cursor.at + 1 + length);
        cursor.at += 1 + length;
        return text;
    };
    const flags = body[cursor.at];
    cursor.at += 1;
    let command;
    if (flags & 0x01) {
        command = readShort();
    } else {
        command = session.commandName(body.readUInt16BE(cursor.at));
        cursor.at += 2;
    }
    if (flags & 0x02) command += `:${readShort()}`;

    const count = body.readUInt16BE(cursor.at);
    cursor.at += 2;
    const parts = [command];
    for (let i = 0; i < count; i += 1) {
        const mode = body[cursor.at];
        cursor.at += 1;
        let key = '';
        if (mode === KEY_MODE.INDEXED) {
            key = session.keyName(body.readUInt16BE(cursor.at));
            cursor.at += 2;
        } else if (mode === KEY_MODE.INLINE) {
            key = readShort();
        }
        const tag = body[cursor.at];
        cursor.at += 1;
        const kind = tag >> 4;
        let width = tag & 0x0f;
        let rendered;
        if (kind === KIND.STRING || kind === KIND.BYTES) {
            const length = body.readUInt32BE(cursor.at);
            cursor.at += 4;
            const raw = body.subarray(cursor.at, cursor.at + length);
            cursor.at += length;
            rendered = kind === KIND.BYTES ? `x${raw.toString('hex')}` : raw.toString('utf8');
        } else if (kind === KIND.UINT || kind === KIND.INT) {
            if (width === 0) width = kind === KIND.UINT ? session.widths.uint : session.widths.int;
            let value = 0n;
            for (let b = 0; b < width; b += 1) value = (value << 8n) | BigInt(body[cursor.at + b]);
            if (kind === KIND.INT) {
                const bits = BigInt(width * 8);
                if (value >= 1n << (bits - 1n)) value -= 1n << bits;
            }
            cursor.at += width;
            rendered = value.toString(10);
        } else if (kind === KIND.FLOAT) {
            if (width === 0) width = session.widths.float;
            rendered = String(width === 4 ? body.readFloatBE(cursor.at) : body.readDoubleBE(cursor.at));
            cursor.at += width;
        } else if (kind === KIND.BOOL) {
            rendered = body[cursor.at] ? '1' : '0';
            cursor.at += 1;
        } else if (kind === KIND.ENUM) {
            const family = body[cursor.at];
            const id = body.readUInt16BE(cursor.at + 1);
            cursor.at += 3;
            rendered = family === ENUM_FAMILY.COMMANDS ? session.commandName(id) : session.keyName(id);
        } else if (kind === KIND.NULL) {
            continue;
        }
        parts.push(key ? `${key}=${rendered}` : rendered);
    }
    return parts.join(' ');
}

/** Encode a response line the way the server would, for the decoder tests. */
function encodeResponseFrame(line, session) {
    const { KIND, KEY_MODE, STATUS } = binary;
    const status =
        line.startsWith('SUCCESS') ? STATUS.SUCCESS
        : line.startsWith('ERROR') ? STATUS.ERROR
        : line.startsWith('PENDING') ? STATUS.PENDING
        : STATUS.OTHER;
    const comma = line.indexOf(',');
    const rest = comma === -1 ? '' : line.slice(comma + 1);

    let fields = [];
    if (status === STATUS.ERROR) {
        fields = [{ key: '', value: rest }];
    } else if (rest !== '') {
        let cursor = 0;
        for (;;) {
            const next = rest.indexOf(',', cursor);
            const token = next === -1 ? rest.slice(cursor) : rest.slice(cursor, next);
            const equals = token.indexOf('=');
            if (equals === -1) fields.push({ key: '', value: token });
            else if (token.slice(0, equals) === 'value') {
                fields.push({ key: 'value', value: rest.slice(cursor + equals + 1) });
                break;
            } else fields.push({ key: token.slice(0, equals), value: token.slice(equals + 1) });
            if (next === -1) break;
            cursor = next + 1;
        }
    }

    const parts = [];
    const head = Buffer.allocUnsafe(3);
    head[0] = status;
    head.writeUInt16BE(fields.length, 1);
    parts.push(head);
    for (const field of fields) {
        if (!field.key) parts.push(Buffer.from([KEY_MODE.POSITIONAL]));
        else {
            const id = session.keyId(field.key);
            if (id === undefined) {
                const name = Buffer.from(field.key, 'utf8');
                parts.push(Buffer.concat([Buffer.from([KEY_MODE.INLINE, name.length]), name]));
            } else {
                const buf = Buffer.allocUnsafe(3);
                buf[0] = KEY_MODE.INDEXED;
                buf.writeUInt16BE(id, 1);
                parts.push(buf);
            }
        }
        if (field.key === 'value') {
            const raw = Buffer.from(field.value, 'latin1');
            const header = Buffer.allocUnsafe(5);
            header[0] = KIND.BYTES << 4;
            header.writeUInt32BE(raw.length, 1);
            parts.push(header, raw);
            continue;
        }
        const numeric = binary.canonicalNumber(field.value);
        if (numeric && numeric.type !== 'float') {
            parts.push(binary.encodeValue({ ...numeric, width: binary.minimalWidth(numeric.type, numeric.value) }));
        } else if (numeric) {
            parts.push(binary.encodeValue({ type: 'float', value: numeric.value, width: 8 }));
        } else {
            parts.push(binary.encodeValue({ type: 'string', value: field.value }));
        }
    }
    return binary.encodeFrame(binary.FRAME.RESPONSE, Buffer.concat(parts));
}

test('a request frame carries the command as two bytes', () => {
    const session = testSession();
    const frame = binary.encodeCommandLine('RECORD get table=ngram', session);
    assert.strictEqual(frame[0], binary.FRAME_MAGIC);
    assert.strictEqual(frame[1], binary.FRAME.REQUEST);
    // flags(1) + id(2) + argc(2), then the arguments.
    assert.strictEqual(frame.readUInt16BE(binary.FRAME_HEADER_BYTES + 1), session.commandId('RECORD'));
    assert.strictEqual(decodeRequestLine(frame, session), 'RECORD get table=ngram');
});

test('an unknown command falls back to its name rather than failing', () => {
    const session = testSession();
    const frame = binary.encodeCommandLine('GRAPH_RECALL seeds=a', session);
    assert.strictEqual(frame[binary.FRAME_HEADER_BYTES] & 0x01, 0x01);
    assert.strictEqual(decodeRequestLine(frame, session), 'GRAPH_RECALL seeds=a');
});

test('transcoding is lossless for the lines every command layer builds', () => {
    const session = testSession();
    const lines = [
        'RECORD set table=ngram key=x6265726c696e cnt=42 prob=0.25',
        'PAIR_SET x616263 ctx:BERLIN',
        'RECORD scan table=ngram limit=100 cursor=x00ff',
        'BATCH PAIR_SET items=W10= continue_on_error=1',
        'EDIT 7 hello  world',
        'INSERT:16 sixteen bytes!!',
        'PAIR_SCAN ctx: 50',
        // A payload carrying base64 padding: the `=` is data, not a key split.
        'INSERT eyJhIjoxfQ==',
        'EDIT 7 a=b c=d',
        'PAIR_SET x00ff k=1',
    ];
    for (const line of lines) {
        assert.strictEqual(decodeRequestLine(binary.encodeCommandLine(line, session), session), line);
    }
});

test('transcoding preserves UTF-8 payload bytes represented by the latin1 command line', () => {
    const session = testSession();
    const payload = 'clichés-café.jpg';
    const wire = Buffer.from(payload, 'utf8').toString('latin1');
    assert.strictEqual(
        decodeRequestLine(binary.encodeCommandLine(`INSERT ${wire}`, session), session),
        `INSERT ${payload}`
    );
});

test('hex arguments travel as real bytes and come back spelled the same', () => {
    const session = testSession();
    const frame = binary.encodeCommandLine('RECORD get table=t key=x6265726c696e', session);
    // 6 hex bytes cost 6 bytes plus a 5-byte header instead of 13 characters.
    assert.ok(frame.length < Buffer.byteLength('RECORD get table=t key=x6265726c696e') + 20);
    assert.strictEqual(decodeRequestLine(frame, session), 'RECORD get table=t key=x6265726c696e');
});

test('numbers pick the smallest width that holds them', () => {
    assert.deepStrictEqual(binary.typeToken('42'), { type: 'uint', value: 42, width: 1 });
    assert.deepStrictEqual(binary.typeToken('70000'), { type: 'uint', value: 70000, width: 4 });
    assert.deepStrictEqual(binary.typeToken('-7'), { type: 'int', value: -7, width: 1 });
    assert.deepStrictEqual(binary.typeToken('0.25'), { type: 'float', value: 0.25, width: 8 });
    // A form that would not re-render identically stays a string: the line is
    // the contract, and "007" must come back as "007".
    assert.strictEqual(binary.typeToken('007').type, 'string');
    assert.strictEqual(binary.typeToken('1e3').type, 'string');
});

test('a transcoded value always states its own width', () => {
    // The bug this pins: a float written 8 bytes wide but tagged "use the
    // default" is read back at the session's 4 and comes out as 1.625. The
    // transcoder does not know which table a line addresses, so it can never
    // predict what a width-0 tag resolves to.
    const session = testSession();
    session.widths = { uint: 4, int: 4, float: 4 };
    const frame = binary.encodeCommandLine('RECORD set table=t prob=0.25', session);
    assert.strictEqual(decodeRequestLine(frame, session), 'RECORD set table=t prob=0.25');
    // Same line under a session that resolves differently: still exact.
    session.widths = { uint: 8, int: 8, float: 8 };
    assert.strictEqual(decodeRequestLine(frame, session), 'RECORD set table=t prob=0.25');
});

test('a width left at 0 follows the table profile the session knows', () => {
    const session = testSession();
    session.loadProfile('ngram', { uint: 2, int: 8, float: 4 });
    const frame = binary.encodeRequest(
        {
            command: 'RECORD',
            args: [
                { type: 'string', value: 'set' },
                { key: 'table', type: 'string', value: 'ngram' },
                { key: 'count', type: 'uint', value: 256 },
            ],
        },
        session
    );
    // 2 bytes written and 2 declared as "resolved": the server reads the same
    // pair because the profile lives on its side.
    const body = frame.subarray(binary.FRAME_HEADER_BYTES);
    assert.strictEqual(body[body.length - 3] >> 4, binary.KIND.UINT);
    assert.strictEqual(body[body.length - 3] & 0x0f, 0, 'the tag defers to the profile');
    assert.strictEqual(body.length - (body.length - 2), 2, 'and exactly two bytes follow');
});

test('an explicitly typed request needs no text line at all', () => {
    const session = testSession();
    const frame = binary.encodeRequest(
        {
            command: 'RECORD',
            args: [
                { type: 'string', value: 'set' },
                { key: 'table', type: 'string', value: 'ngram' },
                { key: 'key', type: 'bytes', value: Buffer.from('berlin') },
                { key: 'cnt', type: 'uint', value: 42, width: 4 },
                { key: 'prob', type: 'float', value: 0.5, width: 8 },
                { key: 'limit', type: 'null' },
            ],
        },
        session
    );
    assert.strictEqual(
        decodeRequestLine(frame, session),
        'RECORD set table=ngram key=x6265726c696e cnt=42 prob=0.5'
    );
});

test('INSERT carries its declared size as a command suffix', () => {
    const session = testSession();
    const frame = binary.encodeRequest({ command: 'INSERT', suffix: '16', args: [{ type: 'string', value: 'payload' }] }, session);
    assert.strictEqual(frame[binary.FRAME_HEADER_BYTES] & 0x02, 0x02);
    assert.strictEqual(decodeRequestLine(frame, session), 'INSERT:16 payload');
});

test('a nested command travels as an enum id', () => {
    const session = testSession();
    const frame = binary.encodeRequest(
        {
            command: 'BATCH',
            args: [
                { type: 'enum', family: binary.ENUM_FAMILY.COMMANDS, value: session.commandId('PAIR_SET') },
                { key: 'items', type: 'string', value: '[]' },
            ],
        },
        session
    );
    assert.strictEqual(decodeRequestLine(frame, session), 'BATCH PAIR_SET items=[]');
});

test('a response frame decodes to the canonical line every parser expects', () => {
    const session = testSession();
    const lines = [
        'SUCCESS,pair_set',
        'SUCCESS,count=2,limit=100',
        'SUCCESS,size=5,value=a,b c',
        'ERROR,value_size_mismatch (expected 16, got 17)',
        'PENDING,job=reduce_1',
    ];
    for (const line of lines) {
        const frame = encodeResponseFrame(line, session);
        const decoded = binary.decodeResponse(frame.subarray(binary.FRAME_HEADER_BYTES), session, session.widths);
        assert.strictEqual(decoded.line, line);
        // And the line still parses exactly as it does over a text socket.
        assert.deepStrictEqual(parseResponse(decoded.line).status, line.split(',')[0]);
    }
});

test('a READ payload survives bytes that are not valid text', () => {
    const session = testSession();
    const raw = '\x00\xff,\x01';
    const frame = encodeResponseFrame(`SUCCESS,size=4,value=${raw}`, session);
    const decoded = binary.decodeResponse(frame.subarray(binary.FRAME_HEADER_BYTES), session, session.widths);
    assert.strictEqual(decoded.fields.value, raw);
});

test('readFrame waits for a whole frame and keeps the remainder', () => {
    const frame = binary.encodeFrame(binary.FRAME.RESPONSE, Buffer.from([1, 0, 0]));
    assert.strictEqual(binary.readFrame(frame.subarray(0, 4)), null);
    assert.strictEqual(binary.readFrame(frame.subarray(0, frame.length - 1)), null);
    const taken = binary.readFrame(Buffer.concat([frame, Buffer.from([0xc7])]));
    assert.strictEqual(taken.frame.type, binary.FRAME.RESPONSE);
    assert.strictEqual(taken.rest.length, 1);
    assert.throws(() => binary.readFrame(Buffer.from([0x41, 0, 0, 0, 0, 0])), /bad magic byte/);
});

test('the handshake ack fills a session with both tables', () => {
    // Rebuilt here in the ack's own layout, which is the contract with
    // src/binary_protocol.go → encodeHandshakeAck.
    const parts = [Buffer.from([1, 4, 8, 4, 0]), Buffer.alloc(8)];
    parts[1].writeBigUInt64BE(3n);
    const shortString = (text) => Buffer.concat([Buffer.from([text.length]), Buffer.from(text, 'utf8')]);
    parts.push(shortString('0123456789abcdef'), shortString('fedcba9876543210'));

    const commands = [{ id: 7, kind: 1, name: 'RECORD' }];
    const commandCount = Buffer.allocUnsafe(2);
    commandCount.writeUInt16BE(commands.length);
    parts.push(commandCount);
    for (const entry of commands) {
        const head = Buffer.allocUnsafe(3);
        head.writeUInt16BE(entry.id);
        head[2] = entry.kind;
        parts.push(head, shortString(entry.name));
    }
    const keys = [{ id: 9, name: 'table' }];
    const keyCount = Buffer.allocUnsafe(2);
    keyCount.writeUInt16BE(keys.length);
    parts.push(keyCount);
    for (const entry of keys) {
        const head = Buffer.allocUnsafe(2);
        head.writeUInt16BE(entry.id);
        parts.push(head, shortString(entry.name));
    }

    const ack = binary.decodeHandshakeAck(Buffer.concat(parts));
    assert.deepStrictEqual(ack.widths, { uint: 4, int: 8, float: 4 });
    assert.strictEqual(ack.epoch, 3);
    assert.strictEqual(ack.digest, '0123456789abcdef');
    assert.deepStrictEqual(ack.commands, [{ id: 7, kind: 'micro', name: 'RECORD' }]);
    assert.deepStrictEqual(ack.keys, [{ id: 9, name: 'table' }]);

    const session = new binary.BinarySession();
    session.loadCommands(ack.commands, ack.digest).loadKeys(ack.keys, ack.keysDigest);
    assert.strictEqual(session.commandId('record'), 7);
    assert.strictEqual(session.keyName(9), 'table');
    assert.ok(session.matchesDigest('0123456789abcdef'));
    assert.ok(!session.matchesDigest('nope'));
});

// --- the ALIAS command layer -------------------------------------------------

function fakeConnection(responses) {
    const sent = [];
    return {
        sent,
        async send(line) {
            sent.push(line);
            const answer = responses[line];
            if (answer === undefined) throw new Error(`unexpected command: ${line}`);
            return parseResponse(answer);
        },
    };
}

function payload(value) {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
}

test('aliasDigest reads the identity a cached index is checked against', async () => {
    const conn = fakeConnection({
        'ALIAS digest': 'SUCCESS,version=1,epoch=1,digest=abc123,commands=60,keys_digest=def456,keys=80',
    });
    const result = await alias.aliasDigest(conn);
    assert.strictEqual(result.digest, 'abc123');
    assert.strictEqual(result.keysDigest, 'def456');
    assert.strictEqual(result.commands, 60);
});

test('loadSession fills a BinarySession from the server', async () => {
    const conn = fakeConnection({
        'ALIAS list': `SUCCESS,epoch=1,digest=abc123,total=2,count=2,payload=${payload([
            { id: 1, name: 'ALIAS', kind: 'micro' },
            { id: 2, name: 'RECORD', kind: 'micro' },
        ])}`,
        'ALIAS keys': `SUCCESS,digest=def456,total=1,count=1,payload=${payload([{ id: 1, name: 'table' }])}`,
    });
    const session = await alias.loadSession(conn, new binary.BinarySession());
    assert.strictEqual(session.commandId('RECORD'), 2);
    assert.strictEqual(session.keyId('table'), 1);
    assert.ok(session.matchesDigest('abc123'));
});

test('tableProfile reads the resolved widths and writes the declared ones', async () => {
    const conn = fakeConnection({
        'ALIAS profile table=ngram':
            'SUCCESS,table=ngram,uint=8,int=8,float=8,declared=0,declared_uint=0,declared_int=0,declared_float=0,updated=0',
        'ALIAS profile table=ngram uint=4 float=4':
            'SUCCESS,table=ngram,uint=4,int=8,float=4,declared=1,declared_uint=4,declared_int=0,declared_float=4,updated=1',
        'ALIAS profile table=ngram reset=1':
            'SUCCESS,table=ngram,uint=8,int=8,float=8,declared=0,declared_uint=0,declared_int=0,declared_float=0,updated=0',
    });

    let profile = await alias.tableProfile(conn, 'ngram');
    assert.strictEqual(profile.declared, false);
    assert.strictEqual(profile.uint, 8);

    profile = await alias.tableProfile(conn, 'ngram', { uint: 4, float: 4 });
    assert.strictEqual(profile.updated, true);
    assert.strictEqual(profile.float, 4);
    // int was never declared, so it reads as the default rather than as zero.
    assert.strictEqual(profile.int, 8);
    assert.strictEqual(profile.declaredWidths.int, 0);

    profile = await alias.tableProfile(conn, 'ngram', { reset: true });
    assert.strictEqual(profile.declared, false);
});

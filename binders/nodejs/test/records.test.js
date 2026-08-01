// Record-table command shapes and the schema contract.
//
// The builders are pure, so most of this asserts the exact line that goes on the
// wire — the part a client gets silently wrong (a field value with a space in
// it, a cursor re-encoded into hex-of-hex, a field name that collides with a
// modifier). The round-trip helpers run against a stand-in that answers through
// the real `parseResponse`, so the response grammar is exercised too.

const test = require('node:test');
const assert = require('node:assert/strict');

const records = require('../lib/records');
const { parseResponse } = require('../lib/protocol');

function fakeConn(handler) {
    const lines = [];
    return {
        lines,
        async send(line) {
            lines.push(line);
            return parseResponse(handler(line, lines.length - 1));
        },
    };
}

const payloadOf = (value) => Buffer.from(JSON.stringify(value), 'utf8').toString('base64');

test('fieldSpec accepts the shapes a caller naturally has', () => {
    assert.equal(records.fieldSpec('cnt:uint:4'), 'cnt:uint:4');
    assert.equal(records.fieldSpec(['cnt', 'uint', 4]), 'cnt:uint:4');
    assert.equal(records.fieldSpec({ name: 'cnt', type: 'uint', width: 4 }), 'cnt:uint:4');
    assert.equal(records.fieldSpec({ name: 'cnt', type: 'uint', bytes: 4 }), 'cnt:uint:4');
});

test('fieldSpec fills in a default width only where the server has one', () => {
    assert.equal(records.fieldSpec('w:float'), 'w:float:8');
    assert.equal(records.fieldSpec('flag:bool'), 'flag:bool:1');
    // bytes/string have no sensible default: the width decides the cost of a row.
    assert.throws(() => records.fieldSpec('label:string'), /explicit byte width/);
});

test('fieldSpec refuses a name that would collide with a RECORD modifier', () => {
    for (const name of ['table', 'key', 'fields', 'limit', 'cursor']) {
        assert.throws(() => records.fieldSpec(`${name}:uint:4`), /collides with a RECORD modifier/);
    }
});

test('buildDefine renders one comma-separated fields token', () => {
    const line = records.buildDefine('ngram', ['cnt:uint:4', ['prob', 'float', 4], 'label:string:12']);
    assert.equal(line, 'RECORD define table=ngram fields=cnt:uint:4,prob:float:4,label:string:12');
    // The whole declaration must survive the whitespace split RECORD applies:
    // `RECORD`, `define`, `table=`, `fields=` and nothing more.
    assert.equal(line.split(' ').length, 4);
});

test('buildSet encodes a value with a space as x<hex>', () => {
    const line = records.buildSet('ngram', 'berlin', { cnt: 42, label: 'old town' });
    assert.match(line, /^RECORD set table=ngram key=berlin cnt=42 label=x/);
    assert.equal(line.split(' ').length, 6);
});

test('buildSet spells booleans as 1/0 and refuses null', () => {
    assert.match(records.buildSet('t', 'k', { seen: true }), /seen=1$/);
    assert.match(records.buildSet('t', 'k', { seen: false }), /seen=0$/);
    assert.throws(() => records.buildSet('t', 'k', { seen: null }), /cannot be set to null/);
    assert.throws(() => records.buildSet('t', 'k', {}), /at least one field/);
});

test('buildScan hands the cursor back verbatim', () => {
    const line = records.buildScan('ngram', { limit: 2, cursor: 'x6265726c696e' });
    assert.match(line, /cursor=x6265726c696e/);
    // Not x<hex of the hex>: that would resume from a prefix that does not exist.
    assert.doesNotMatch(line, /cursor=x78/);
});

test('buildAlter needs something to do and renders drops as a list', () => {
    assert.throws(() => records.buildAlter('t', {}), /needs add or drop/);
    assert.equal(
        records.buildAlter('t', { add: 'novelty:float:4', drop: ['label', 'note'], compact: true }),
        'RECORD alter table=t add=novelty:float:4 drop=label,note compact=1'
    );
});

test('buildSchema asks for the row count only when told to', () => {
    assert.equal(records.buildSchema('t'), 'RECORD schema table=t');
    assert.equal(records.buildSchema('t', { rows: true }), 'RECORD schema table=t rows=1');
});

test('an invalid table name is refused before the wire', () => {
    assert.throws(() => records.buildSchema('has/slash'), /table name is invalid/);
    assert.throws(() => records.buildSchema(''), /table name is invalid/);
});

test('define reports the shape the server answered with', async () => {
    const conn = fakeConn(() => 'SUCCESS,table=ngram,fields=3,width=20,dead_bytes=0,generation=1,created=1');
    const schema = await records.define(conn, 'ngram', 'cnt:uint:4,prob:float:4,label:string:12');
    assert.equal(schema.table, 'ngram');
    assert.equal(schema.width, 20);
    assert.equal(schema.generation, 1);
});

test('setRow reports whether the row was new and where it lives', async () => {
    const conn = fakeConn(() => 'SUCCESS,table=ngram,key=x6265726c696e,created=1,written=2,abs_key=7');
    const write = await records.setRow(conn, 'ngram', 'berlin', { cnt: 1, prob: 0.5 });
    assert.deepEqual(
        { created: write.created, written: write.written, absKey: write.absKey },
        { created: true, written: 2, absKey: 7 }
    );
});

test('getRow decodes the payload and keeps a null distinct from a zero', async () => {
    const conn = fakeConn(
        () => `SUCCESS,table=ngram,key=x61,abs_key=1,fields=3,payload=${payloadOf({ cnt: 42, novelty: null, prob: 0.25 })}`
    );
    const row = await records.getRow(conn, 'ngram', 'a');
    assert.equal(row.cnt, 42);
    // The row predates `novelty`: null is the answer, not 0.
    assert.equal(row.novelty, null);
});

test('getRow answers null for a missing row rather than throwing', async () => {
    const conn = fakeConn(() => 'ERROR,not_found');
    assert.equal(await records.getRow(conn, 'ngram', 'nope'), null);
});

test('schema answers null for a table that is not there', async () => {
    const conn = fakeConn(() => 'ERROR,record_table_not_found:ghost');
    assert.equal(await records.schema(conn, 'ghost'), null);
});

test('scanPage decodes rows and their hex keys', async () => {
    const rows = [
        { key: 'x6265726c696e', abs_key: 3, fields: { cnt: 42 } },
        { key: 'x6c6973626f6e', abs_key: 4, fields: { cnt: 7 } },
    ];
    const conn = fakeConn(() => `SUCCESS,table=ngram,count=2,payload=${payloadOf(rows)}`);
    const page = await records.scanPage(conn, 'ngram');
    assert.deepEqual(page.rows.map((row) => row.key), ['berlin', 'lisbon']);
    assert.equal(page.rows[0].fields.cnt, 42);
    assert.equal(page.cursor, null);
});

test('scanRows follows the cursor to the end of the sweep', async () => {
    const pages = [
        `SUCCESS,table=t,count=2,next_cursor=x62,payload=${payloadOf([
            { key: 'x61', abs_key: 1, fields: {} },
            { key: 'x62', abs_key: 2, fields: {} },
        ])}`,
        `SUCCESS,table=t,count=1,payload=${payloadOf([{ key: 'x63', abs_key: 3, fields: {} }])}`,
    ];
    const conn = fakeConn((line, index) => pages[index]);
    const seen = [];
    for await (const row of records.scanRows(conn, 't', { limit: 2 })) seen.push(row.key);
    assert.deepEqual(seen, ['a', 'b', 'c']);
    assert.match(conn.lines[1], /cursor=x62/);
});

test('deleteRow and dropTable are idempotent about a missing target', async () => {
    const missing = fakeConn(() => 'ERROR,not_found');
    assert.equal(await records.deleteRow(missing, 't', 'k'), false);
    const noTable = fakeConn(() => 'ERROR,record_table_not_found:t');
    assert.equal(await records.dropTable(noTable, 't'), 0);
    const dropped = fakeConn(() => 'SUCCESS,deleted=6,table=t,dropped=1');
    assert.equal(await records.dropTable(dropped, 't'), 6);
});

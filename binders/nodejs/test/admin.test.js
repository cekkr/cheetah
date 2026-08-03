// Server, database, job and prediction command shapes.
//
// These families are thin — the value they add is the encodings, and the
// encodings are exactly what a hand-written client gets wrong: a `NA` gauge read
// as zero, a job command line not base64'd, a settings token the server does not
// know, `FILE_CHECKPOINT`'s bare uppercase flags.

const test = require('node:test');
const assert = require('node:assert/strict');

const admin = require('../lib/admin');
const jobs = require('../lib/jobs');
const predict = require('../lib/predict');
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

// --- databases -------------------------------------------------------------

test('createDatabase sends the settings and reports the effective ones', async () => {
    const conn = fakeConn(
        () => 'SUCCESS,database_created=bench,pair_index_bytes=2,payload_cache_bytes=16777216'
    );
    const created = await admin.createDatabase(conn, 'bench', { pair_bytes: 2, payload_cache_mb: 16 });
    assert.equal(conn.lines[0], 'DB_CREATE bench pair_bytes=2 payload_cache_mb=16');
    assert.equal(created.name, 'bench');
    assert.equal(created.settings.pair_index_bytes, '2');
});

test('a setting the server does not know is caught before the wire', () => {
    assert.throws(() => admin.buildCreateDatabase('bench', { cache_size_mb: 16 }), /not understood/);
});

test('boolean settings travel as 1/0, never as "false"', () => {
    assert.equal(
        admin.buildCreateDatabase('bench', { adaptive_pair_index: false }),
        'DB_CREATE bench adaptive_pair_index=0'
    );
});

test('listDatabases decodes the payload and the ad-hoc flag', async () => {
    const rows = [
        { name: 'bench', path: 'cheetah_data/bench', loaded: true, ad_hoc_settings: true, settings: { pair_index_bytes: 2 } },
        { name: 'default', path: 'cheetah_data/default', loaded: false, ad_hoc_settings: false, settings: {} },
    ];
    const conn = fakeConn(() => `SUCCESS,count=2,default=default,payload=${payloadOf(rows)}`);
    const listed = await admin.listDatabases(conn);
    assert.deepEqual(listed.map((entry) => entry.name), ['bench', 'default']);
    assert.equal(listed[0].adHoc, true);
    assert.equal(listed[1].adHoc, false);
    assert.equal(listed[0].settings.pair_index_bytes, 2);
});

test('an existing database is an error, not a silent adoption', async () => {
    const conn = fakeConn(() => 'ERROR,database_exists:bench');
    await assert.rejects(() => admin.createDatabase(conn, 'bench'), /database_exists/);
});

test('configureDatabase separates hot changes from trie resets', async () => {
    const conn = fakeConn(
        () => 'SUCCESS,database_configured=bench,loaded=1,' +
            'applied=payload_cache_entries;graph_cache_sample,on_open=-,reopen=-,' +
            'reset=pair_index_bytes,pair_index_bytes=2,payload_cache_entries=7,graph_cache_sample=0.5'
    );
    const changed = await admin.configureDatabase(conn, 'bench', {
        payload_cache_entries: 7,
        graph_cache_sample: 0.5,
        pair_bytes: 2,
    });
    assert.equal(
        conn.lines[0],
        'DB_CONFIG bench payload_cache_entries=7 graph_cache_sample=0.5 pair_bytes=2'
    );
    assert.deepEqual(changed.applied, ['payload_cache_entries', 'graph_cache_sample']);
    assert.deepEqual(changed.reset, ['pair_index_bytes']);
    assert.equal(changed.settings.graph_cache_sample, '0.5');
});

test('resetDatabase refuses to carry settings without a name', async () => {
    const conn = fakeConn(() => 'SUCCESS,database_reset_to_notes');
    await assert.rejects(() => admin.resetDatabase(conn, null, { pair_bytes: 2 }), /explicit database name/);
    await admin.resetDatabase(conn, 'notes', { pair_bytes: 2 });
    assert.equal(conn.lines[0], 'RESET_DB notes pair_bytes=2');
});

// --- server gauges ---------------------------------------------------------

test('systemStats keeps NA distinct from zero and computes the hit ratio', async () => {
    const conn = fakeConn(
        () => 'SUCCESS,command=SYSTEM_STATS,logical_cores=8,process_cpu_pct=1.50,system_cpu_pct=NA,' +
            'payload_cache_hits=9,payload_cache_misses=1'
    );
    const stats = await admin.systemStats(conn);
    assert.equal(stats.logicalCores, 8);
    assert.equal(stats.processCpuPct, 1.5);
    // `NA` is how a platform-unavailable metric reports: not zero, not an error.
    assert.equal(stats.systemCpuPct, null);
    assert.equal(stats.cacheHitRatio, 0.9);
});

test('logFlush decodes the payload list', async () => {
    const conn = fakeConn(() => `SUCCESS,count=2,payload=${payloadOf(['one', 'two'])}`);
    assert.deepEqual(await admin.logFlush(conn), ['one', 'two']);
});

test('fileCheckpoint spells its bare uppercase flags', async () => {
    const conn = fakeConn(() => 'SUCCESS,file_checkpoint_flushed=4');
    assert.equal(await admin.fileCheckpoint(conn, { idle: '0s', closeHandles: true }), 4);
    assert.equal(conn.lines[0], 'FILE_CHECKPOINT IDLE=0s CLOSE_HANDLES');
});

test('forkAssign splits the pipe-separated owner list', async () => {
    const conn = fakeConn(() => 'SUCCESS,fork_id=f3,nodes=nodeA|nodeB');
    const assignment = await admin.forkAssign(conn, 'ctx:');
    assert.equal(assignment.forkId, 'f3');
    assert.deepEqual(assignment.nodes, ['nodeA', 'nodeB']);
});

// --- jobs ------------------------------------------------------------------

test('JOB submit sends the command line base64-encoded', () => {
    const line = jobs.buildSubmit('PAIR_REDUCE counts ctx: 256');
    assert.equal(
        line,
        `JOB submit command=${Buffer.from('PAIR_REDUCE counts ctx: 256', 'utf8').toString('base64')}`
    );
    // The micro dialect splits on whitespace; a command line is full of it.
    assert.equal(line.split(' ').length, 3);
});

test('a failed job reports state=failed on a SUCCESS line', async () => {
    const conn = fakeConn(() => 'SUCCESS,job=reduce_1,state=failed,progress=12.00,error=boom');
    const snapshot = await jobs.status(conn, 'reduce_1');
    assert.equal(snapshot.state, 'failed');
    assert.equal(snapshot.failed, true);
    assert.equal(snapshot.finished, true);
    assert.equal(snapshot.error, 'boom');
});

test('fetch answers null while the job is still running', async () => {
    const conn = fakeConn(() => 'PENDING,job=reduce_1,state=running,progress=40.00');
    assert.equal(await jobs.fetch(conn, 'reduce_1'), null);
});

test('awaitJob polls, then fetches once', async () => {
    const script = [
        'SUCCESS,job=reduce_1,state=running,progress=50.00,completed=1,total=2',
        'SUCCESS,job=reduce_1,state=completed,progress=100.00,completed=2,total=2',
        'SUCCESS,job=reduce_1,reducer=counts,count=2',
    ];
    const conn = fakeConn((line, index) => script[index]);
    const seen = [];
    const result = await jobs.awaitJob(conn, 'reduce_1', {
        pollIntervalMs: 1,
        onProgress: (snapshot) => seen.push(snapshot.state),
    });
    assert.deepEqual(seen, ['running', 'completed']);
    assert.equal(result.fields.count, '2');
    assert.match(conn.lines[2], /^JOB fetch id=reduce_1$/);
});

test('awaitJob turns a failed job into an error rather than a result', async () => {
    const conn = fakeConn(() => 'SUCCESS,job=reduce_1,state=failed,error=boom');
    await assert.rejects(() => jobs.awaitJob(conn, 'reduce_1', { pollIntervalMs: 1 }), /failed: boom/);
});

// --- prediction ------------------------------------------------------------

test('PREDICT_SET carries its weights as base64 JSON', () => {
    const line = predict.buildSet({ key: 'ctx:a', value: 'cat', prob: 0.4, weights: { w: [0.1, 0.2] } });
    assert.match(line, /^PREDICT_SET key=ctx:a value=cat prob=0.4 weights=/);
    // One token per field: the family splits on whitespace.
    assert.equal(line.split(' ').length, 5);
    const encoded = line.split('weights=')[1];
    assert.deepEqual(JSON.parse(Buffer.from(encoded, 'base64').toString('utf8')), { w: [0.1, 0.2] });
});

test('PREDICT_QUERY renders its list arguments comma-separated', () => {
    const line = predict.buildQuery({ key: 'ctx:a', keys: ['ctx:a', 'ctx:b'], merge: 'mean' });
    assert.equal(line, 'PREDICT_QUERY key=ctx:a keys=ctx:a,ctx:b merge=mean');
});

test('PREDICT_INHERIT_BATCH refuses an empty batch', () => {
    assert.throws(() => predict.buildInheritBatch([]), /requires items/);
});

test('predict.query decodes the payload', async () => {
    const conn = fakeConn(() => `SUCCESS,count=2,backend=cpu,payload=${payloadOf([['cat', 0.6]])}`);
    const result = await predict.query(conn, { key: 'ctx:a' });
    assert.equal(result.count, 2);
    assert.equal(result.backend, 'cpu');
    assert.deepEqual(result.payload, [['cat', 0.6]]);
});

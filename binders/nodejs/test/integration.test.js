// Live round-trip against a spawned cheetah-server.
//
// Skipped unless CHEETAH_INTEGRATION=1, because it needs a Go toolchain and
// builds the server binary. Run it with:
//
//   npm run test:integration
//
// This is the test that proves the client, the codec and `CheetahDatabase`
// agree with the real server rather than with the handbook.

const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');

const ENABLED = process.env.CHEETAH_INTEGRATION === '1';

const { CheetahClient, CheetahDatabase, CheetahPool, TokenVocabulary, startServer } = require('..');
const kv = require('../lib/kv');
const graph = require('../lib/graph');
const { hex } = require('../lib/keys');

/** An ephemeral port the OS just told us is free. */
function freePort() {
    return new Promise((resolve, reject) => {
        const server = net.createServer();
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const { port } = server.address();
            server.close(() => resolve(port));
        });
    });
}

class NoteStore extends CheetahDatabase {
    constructor(options) {
        super({ ...options, layout: { key: 'cfg:note_layout', version: 1 } });
        this.notes = new Map();
    }

    clearCaches() {
        this.notes.clear();
    }

    noteKey(id) {
        return `note:${hex(id, 8, 'noteId')}`;
    }

    async put(text) {
        const id = await this.allocateRandomId((candidate) => this.noteKey(candidate));
        await this.putJson(this.noteKey(id), { text, at: this.timestamp() }, { upsert: true });
        this.notes.set(id, text);
        return id;
    }
}

test('cheetah binder round-trip', { skip: ENABLED ? false : 'set CHEETAH_INTEGRATION=1 to run' }, async (t) => {
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cheetah-binder-test-'));
    const port = await freePort();
    const server = await startServer({ port, dataDir, graphTermIndex: false, pairIndexBytes: 2 });
    const database = 'binder_test';
    const pool = new CheetahPool({
        port,
        size: 2,
        database,
        // Trie geometry is adopted only when the directory is created, so the
        // override has to be recorded on the name before anything is written.
        databaseOptions: { pair_bytes: 2 },
    });

    t.after(async () => {
        await pool.close();
        await server.stop();
        fs.rmSync(dataDir, { recursive: true, force: true });
    });
    await pool.connect();

    await t.test('a value round-trips through INSERT + PAIR_SET', async () => {
        await kv.putValue(pool, 'k:plain', 'hello');
        assert.equal(await kv.getValue(pool, 'k:plain'), 'hello');
        assert.equal(await kv.getValue(pool, 'k:absent'), null);
    });

    await t.test('a UTF-8 payload survives the latin1 wire', async () => {
        await kv.putJson(pool, 'k:accents', { name: 'Café Ünicode — ok' }, { upsert: true });
        assert.deepEqual(await kv.getJson(pool, 'k:accents'), { name: 'Café Ünicode — ok' });
    });

    await t.test('an upsert edits in place and keeps the absolute key', async () => {
        const first = await kv.putValue(pool, 'k:edit', 'one', { upsert: true });
        const second = await kv.putValue(pool, 'k:edit', 'two-longer', { upsert: true });
        assert.equal(second, first);
        assert.equal(await kv.getValue(pool, 'k:edit'), 'two-longer');
    });

    await t.test('a batched write binds every key in one request', async () => {
        const entries = Array.from({ length: 64 }, (unused, index) => ({
            key: `b:${hex(index, 4, 'index')}`,
            payload: { index },
        }));
        assert.equal(await kv.putJsonBatch(pool, entries), 64);
        assert.deepEqual(await kv.getJson(pool, 'b:0007'), { index: 7 });
    });

    await t.test('a scan pages through a prefix and stops', async () => {
        // 64 rows against a page of 10: the cursor is what makes this whole.
        const items = await kv.scanAll(pool, 'b:', { limit: 10 });
        assert.equal(items.length, 64);
        const keys = new Set(items.map((item) => item.key));
        assert.equal(keys.size, 64);
        assert.ok(keys.has('b:003f'));
    });

    await t.test('the continuations reducer hydrates payloads during the scan', async () => {
        const items = await kv.scanAll(pool, 'b:', { limit: 10, reducer: 'continuations' });
        assert.equal(items.length, 64);
        for (const item of items) {
            const payload = JSON.parse(Buffer.from(item.payloadBase64, 'base64').toString('utf8'));
            assert.equal(typeof payload.index, 'number');
        }
    });

    await t.test('deletePair unbinds a name and is idempotent afterwards', async () => {
        await kv.putValue(pool, 'k:doomed', 'bye');
        assert.equal(await kv.deletePair(pool, 'k:doomed'), 1);
        assert.equal(await kv.getValue(pool, 'k:doomed'), null);
        assert.equal(await kv.deletePair(pool, 'k:doomed'), 0);
    });

    await t.test('the vocabulary allocates each name exactly one token', async () => {
        const vocabulary = new TokenVocabulary(pool);
        const names = Array.from({ length: 40 }, (unused, index) => `descriptor-${index}`);
        const tokens = await vocabulary.tokensFor(names);
        assert.equal(new Set(tokens).size, 40);
        // Re-resolving is stable, and the reverse direction agrees.
        assert.deepEqual(await vocabulary.tokensFor(names), tokens);
        assert.equal(await new TokenVocabulary(pool).nameFor(tokens[9]), names[9]);
    });

    await t.test('recall finds what several seeds have in common', async () => {
        await graph.setNode(pool, { id: 'mdoc1', labels: ['doc'], props: { title: 'a doc' } });
        await graph.setNode(pool, { id: 'mdoc2', labels: ['doc'] });
        const applied = await graph.setEdgeBatch(pool, [
            { from: 'wapple', to: 'mdoc1', weight: 1 },
            { from: 'wpear', to: 'mdoc1', weight: 1 },
            { from: 'wpear', to: 'mdoc2', weight: 1 },
        ], { type: 'mention', directed: 1 });
        assert.equal(applied.failed, 0);
        assert.equal(applied.applied, 3);

        assert.equal((await graph.degree(pool, { id: 'wpear', type: 'mention' })).degree, 2);
        assert.equal((await graph.degree(pool, { id: 'wnever', type: 'mention' })).degree, 0);
        assert.equal((await graph.getNode(pool, 'mdoc1')).props.title, 'a doc');
        assert.equal(await graph.getNode(pool, 'mabsent'), null);

        const hits = await graph.recallBatched(pool, {
            seeds: ['wapple', 'wpear'],
            hops: 1,
            decay: 1,
            type: 'mention',
        });
        const ranked = hits.filter((hit) => hit.id.startsWith('mdoc'));
        assert.equal(ranked[0].id, 'mdoc1', 'the document both seeds reach ranks first');
        assert.equal(ranked[0].sourceCount, 2);
    });

    await t.test('a subclass inherits connect, ids and accounting', async () => {
        const notes = new NoteStore({ pool });
        await notes.connect();
        assert.equal(await notes.getValue('cfg:note_layout'), '1');

        const id = await notes.put('remember this');
        assert.equal((await notes.getJson(notes.noteKey(id))).text, 'remember this');

        const summary = await notes.namespaceSummary(['note:', 'b:']);
        assert.equal(summary.namespaces['note:'].count, 1);
        assert.equal(summary.namespaces['b:'].count, 64);
        assert.ok(summary.totalPayloadBytes > 0);

        // Closing a store that borrowed the pool must leave the pool usable.
        await notes.close();
        assert.equal(await kv.getValue(pool, 'k:plain'), 'hello');
    });

    await t.test('a mismatched layout version is refused on connect', async () => {
        const newer = new CheetahDatabase({
            pool,
            layout: { key: 'cfg:note_layout', version: 2, label: 'note layout' },
        });
        await assert.rejects(() => newer.connect(), /note layout 1 is incompatible with codec 2/);
    });

    await t.test('mutateJson survives concurrent increments of one key', async () => {
        const counter = new CheetahDatabase({ pool });
        await counter.connect();
        await Promise.all(Array.from({ length: 20 }, () =>
            counter.mutateJson('c:hits', { count: 0 }, (record) => ({ count: record.count + 1 }))
        ));
        assert.deepEqual(await counter.getJson('c:hits'), { count: 20 });
    });

    await t.test('reset drops the database on every connection', async () => {
        const store = new NoteStore({ pool, database });
        await store.connect();
        await store.put('gone after the reset');

        await store.reset();
        assert.equal(store.notes.size, 0, 'reset clears caches describing the old database');
        assert.equal(await kv.getValue(pool, 'k:plain'), null);
        // Every connection must still be usable, not just the one that reset.
        for (let round = 0; round < 4; round += 1) {
            await kv.putValue(pool, `after:${round}`, 'ok');
            assert.equal(await kv.getValue(pool, `after:${round}`), 'ok');
        }
    });

    await t.test('a single client pipelines without losing a response', async () => {
        // The protocol has no request ids: responses match commands by arrival
        // order. Issuing more commands than `maxInFlight` at once is the case
        // where a queue that reordered writes would show up as answers matched
        // to the wrong command.
        const single = new CheetahClient({ port, database, maxInFlight: 4 });
        await single.connect();
        const written = await Promise.all(
            Array.from({ length: 40 }, (unused, index) =>
                kv.putValue(single, `p:${hex(index, 4, 'index')}`, `value-${index}`))
        );
        assert.equal(new Set(written).size, 40, 'every INSERT got its own absolute key');
        const read = await Promise.all(
            Array.from({ length: 40 }, (unused, index) => kv.getValue(single, `p:${hex(index, 4, 'index')}`))
        );
        read.forEach((value, index) => assert.equal(value, `value-${index}`));
        await single.close();
    });
});

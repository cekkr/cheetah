// CheetahDatabase tests, against an in-memory stand-in for the server.
//
// The fake speaks the same line protocol the real server does — it is fed
// through the same `parseResponse` — so these tests exercise the actual command
// spellings, not a mock of the binder's own functions. What it does not prove
// is that the server answers this way; that is what the integration test and
// the Go suite are for.

const test = require('node:test');
const assert = require('node:assert/strict');

const { CheetahDatabase } = require('../lib/database');
const { buildCommand, parseResponse } = require('../lib/protocol');

/** An in-memory server: absolute keys hold bytes, pairs bind names to them. */
class FakeCheetah {
    constructor() {
        this.values = new Map();
        this.pairs = new Map();
        this.nextKey = 1;
        this.lines = [];
    }

    async send(line) {
        this.lines.push(line);
        return parseResponse(this.#handle(line));
    }

    async command(name, ...args) {
        return this.send(buildCommand(name, ...args));
    }

    async commandOrThrow(name, ...args) {
        const response = await this.command(name, ...args);
        if (!response.ok) throw new Error(`${name} failed: ${response.raw}`);
        return response;
    }

    #handle(line) {
        const space = line.indexOf(' ');
        const command = space === -1 ? line : line.slice(0, space);
        const rest = space === -1 ? '' : line.slice(space + 1);
        switch (command) {
            case 'INSERT': {
                const key = this.nextKey++;
                this.values.set(key, rest);
                return `SUCCESS,key=${key}`;
            }
            case 'EDIT': {
                const editSpace = rest.indexOf(' ');
                const key = Number.parseInt(rest.slice(0, editSpace), 10);
                if (!this.values.has(key)) return 'ERROR,key_not_found';
                this.values.set(key, rest.slice(editSpace + 1));
                return 'SUCCESS,edited';
            }
            case 'READ': {
                const key = Number.parseInt(rest, 10);
                if (!this.values.has(key)) return 'ERROR,key_not_found';
                const value = this.values.get(key);
                return `SUCCESS,size=${value.length},value=${value}`;
            }
            case 'PAIR_SET': {
                const [name, key] = rest.split(' ');
                this.pairs.set(name, Number.parseInt(key, 10));
                return 'SUCCESS,pair_set';
            }
            case 'PAIR_GET': {
                if (!this.pairs.has(rest)) return 'ERROR,not_found';
                return `SUCCESS,key=${this.pairs.get(rest)}`;
            }
            case 'PAIR_SUMMARY': {
                const [prefix] = rest.split(' ');
                let count = 0;
                let bytes = 0;
                for (const [name, key] of this.pairs) {
                    if (!name.startsWith(prefix)) continue;
                    count += 1;
                    bytes += (this.values.get(key) || '').length;
                }
                return `SUCCESS,command=PAIR_SUMMARY,count=${count},total_payload_bytes=${bytes}`;
            }
            case 'DEL': {
                const match = /^pairs key=(.*)$/.exec(rest);
                if (!match) return 'ERROR,unsupported';
                if (!this.pairs.delete(match[1])) return 'ERROR,not_found';
                return 'SUCCESS,deleted=1';
            }
            default:
                return `ERROR,unknown_command:${command}`;
        }
    }
}

function store(options = {}) {
    const pool = new FakeCheetah();
    const database = new CheetahDatabase({
        pool,
        layout: { key: 'cfg:layout', version: 3 },
        ...options,
    });
    return { pool, database };
}

test('connect stamps the layout version on an empty database', async () => {
    const { pool, database } = store();
    await database.connect();
    assert.equal(await database.getValue('cfg:layout'), '3');
    assert.equal(database.connected, true);

    // Re-connecting is a no-op, not a second round of writes.
    const before = pool.lines.length;
    await database.connect();
    assert.equal(pool.lines.length, before);
});

test('connect refuses a database written by an incompatible codec', async () => {
    const { pool } = store();
    const written = new CheetahDatabase({ pool, layout: { key: 'cfg:layout', version: 3 } });
    await written.connect();

    const newer = new CheetahDatabase({
        pool,
        layout: { key: 'cfg:layout', version: 4, label: 'sign layout' },
    });
    await assert.rejects(() => newer.connect(), /sign layout 3 is incompatible with codec 4/);
    assert.equal(newer.connected, false);
});

test('a store with no layout marker connects against anything', async () => {
    const { database } = store({ layout: null });
    await database.connect();
    assert.equal(await database.getValue('cfg:layout'), null);
});

test('json values round-trip, and upsert keeps the absolute key stable', async () => {
    const { database } = store();
    await database.connect();

    const first = await database.putJson('thing:1', { name: 'a' }, { upsert: true });
    assert.deepEqual(await database.getJson('thing:1'), { name: 'a' });

    const second = await database.putJson('thing:1', { name: 'b' }, { upsert: true });
    assert.equal(second, first, 'an upsert edits in place rather than rebinding');
    assert.deepEqual(await database.getJson('thing:1'), { name: 'b' });

    // A blind write rebinds the name to fresh bytes.
    const third = await database.putJson('thing:1', { name: 'c' });
    assert.notEqual(third, first);
    assert.deepEqual(await database.getJson('thing:1'), { name: 'c' });

    assert.equal(await database.getJson('thing:missing'), null);
});

test('mutateJson serializes concurrent read-modify-writes of one key', async () => {
    const { database } = store();
    await database.connect();

    await Promise.all(Array.from({ length: 25 }, () =>
        database.mutateJson('use:1', { count: 0 }, (record) => ({ count: record.count + 1 }))
    ));
    assert.deepEqual(await database.getJson('use:1'), { count: 25 });
    // The chain must not leak an entry per key it has finished with.
    assert.equal(database.mutationChains.size, 0);
});

test('a failed mutation does not wedge the chain for that key', async () => {
    const { database } = store();
    await database.connect();

    await assert.rejects(() => database.mutateJson('use:2', { count: 0 }, () => {
        throw new Error('mutation blew up');
    }), /mutation blew up/);
    const after = await database.mutateJson('use:2', { count: 0 }, (record) => ({ count: record.count + 1 }));
    assert.deepEqual(after, { count: 1 });
});

test('allocateRandomId skips ids already taken', async () => {
    const draws = [7, 7, 9];
    const { database } = store({ randomInt: () => draws.shift() });
    await database.connect();
    await database.putJson('i:7', { taken: true });

    assert.equal(await database.allocateRandomId((id) => `i:${id}`), 9);
});

test('allocateRandomId gives up rather than looping forever', async () => {
    const { database } = store({ randomInt: () => 7 });
    await database.connect();
    await database.putJson('i:7', { taken: true });

    await assert.rejects(
        () => database.allocateRandomId((id) => `i:${id}`, { attempts: 3 }),
        /collision-free id/
    );
});

test('namespaceSummary totals payload bytes per prefix', async () => {
    const { database } = store();
    await database.connect();
    await database.putJson('a:1', { v: 1 });
    await database.putJson('a:2', { v: 2 });
    await database.putJson('b:1', { v: 3 });

    const summary = await database.namespaceSummary(['a:', 'b:', 'a:']);
    assert.equal(summary.namespaces['a:'].count, 2);
    assert.equal(summary.namespaces['b:'].count, 1);
    // The duplicate prefix must not be counted twice into the total.
    assert.equal(summary.totalRecords, 3);
    assert.equal(
        summary.totalPayloadBytes,
        summary.namespaces['a:'].payloadBytes + summary.namespaces['b:'].payloadBytes
    );
});

test('deletePair reports 0 for a name nobody bound', async () => {
    const { database } = store();
    await database.connect();
    await database.putJson('a:1', { v: 1 });

    assert.equal(await database.deletePair('a:1'), 1);
    assert.equal(await database.deletePair('a:1'), 0);
});

test('close leaves a borrowed pool open', async () => {
    const pool = new FakeCheetah();
    let closed = false;
    pool.close = async () => { closed = true; };

    const borrowed = new CheetahDatabase({ pool });
    await borrowed.connect();
    await borrowed.close();
    assert.equal(closed, false, 'a pool passed in is the caller lifetime, not ours');
    assert.equal(borrowed.ownsPool, false);
});

test('a subclass inherits the plumbing and adds its own namespace', async () => {
    class ThingStore extends CheetahDatabase {
        constructor(options) {
            super({ ...options, layout: { key: 'cfg:things', version: 1 } });
            this.cache = new Map();
        }

        clearCaches() {
            this.cache.clear();
        }

        async onConnect(conn) {
            this.seenConnection = conn;
        }

        async putThing(thing) {
            const id = await this.allocateRandomId((candidate) => `thing:${candidate}`);
            const record = { ...thing, created_at: this.timestamp(), complete: false };
            await this.putJson(`thing:${id}`, record, { upsert: true });
            this.cache.set(id, record);
            return id;
        }
    }

    const pool = new FakeCheetah();
    const things = new ThingStore({ pool, now: () => new Date('2026-07-31T00:00:00Z') });
    await things.connect();

    assert.equal(things.seenConnection, pool, 'onConnect runs on a leased connection');
    const id = await things.putThing({ name: 'widget' });
    assert.deepEqual(await things.getJson(`thing:${id}`), {
        name: 'widget',
        created_at: '2026-07-31T00:00:00.000Z',
        complete: false,
    });
    assert.equal(things.cache.size, 1);
});

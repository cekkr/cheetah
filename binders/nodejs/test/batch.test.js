// BATCH command-shape and auto-batching tests.
//
// Two halves, and they test different things. The builders are pure, so the
// first half asserts the exact line that goes on the wire — that is the part a
// client gets silently wrong. The second half drives `CommandBatcher` through a
// scripted stand-in and asserts the properties that make transparent
// coalescing safe to leave on: one response per caller, ordering preserved,
// and nothing batched that was not issued as bulk work.

const test = require('node:test');
const assert = require('node:assert/strict');

const {
    AUTO_BATCH_DEFAULTS,
    CommandBatcher,
    buildBatch,
    decodeResultLines,
    parseBatchResponse,
    runBatch,
    splitCommandLine,
} = require('../lib/batch');
const { parseResponse } = require('../lib/protocol');

/** Records every line and answers from a caller-supplied script. */
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

const fieldOf = (line, name) => {
    const match = line.match(new RegExp(`(?:^| )${name}=([^ ]*)`));
    return match ? match[1] : null;
};
const itemsOf = (line) => JSON.parse(Buffer.from(fieldOf(line, 'items'), 'base64').toString('utf8'));
const payloadOf = (value) => Buffer.from(JSON.stringify(value), 'utf8').toString('base64');

// --- builders --------------------------------------------------------------

test('buildBatch encodes items and carries its continue-on-error default explicitly', () => {
    const line = buildBatch('PAIR_SET', ['ctx:a 1', 'ctx:b 2']);
    assert.equal(line.startsWith('BATCH PAIR_SET items='), true);
    assert.deepEqual(itemsOf(line), ['ctx:a 1', 'ctx:b 2']);
    // The server defaults to stop-on-error, unlike this binder.
    assert.equal(fieldOf(line, 'continue_on_error'), '1');
    assert.equal(line.includes('results='), false);
});

test('buildBatch carries the non-default modifiers and the shared ones', () => {
    const line = buildBatch('GRAPH_EDGE_SET', [{ from: 'a', to: 'b' }], {
        continueOnError: false,
        results: false,
        async: true,
        shared: { type: 'knows', weight: 2 },
    });
    assert.equal(fieldOf(line, 'continue_on_error'), '0');
    assert.equal(fieldOf(line, 'results'), '0');
    assert.equal(fieldOf(line, 'async'), '1');
    assert.equal(fieldOf(line, 'type'), 'knows');
    assert.equal(fieldOf(line, 'weight'), '2');
});

test('buildBatch refuses the targets the server refuses, before the round trip', () => {
    for (const target of ['BATCH', 'JOB', 'DATABASE', 'RESET_DB', 'EXIT']) {
        assert.throws(() => buildBatch(target, ['x']), /cannot target/);
    }
});

test('buildBatch refuses an empty list and a shared value with whitespace', () => {
    assert.throws(() => buildBatch('PAIR_SET', []), /at least one item/);
    assert.throws(
        () => buildBatch('GRAPH_EDGE_SET', [{ from: 'a' }], { shared: { note: 'two words' } }),
        /must not contain whitespace/
    );
});

test('splitCommandLine uppercases the verb and keeps the arguments verbatim', () => {
    assert.deepEqual(splitCommandLine('pair_set ctx:a 42'), ['PAIR_SET', 'ctx:a 42']);
    assert.deepEqual(splitCommandLine('SYSTEM_STATS'), ['SYSTEM_STATS', '']);
});

// --- responses -------------------------------------------------------------

test('decodeResultLines reads plain lines and the base64 fallback alike', () => {
    const plain = { payload: payloadOf(['SUCCESS,pair_set', null]) };
    assert.deepEqual(decodeResultLines(plain), ['SUCCESS,pair_set', null]);

    // The server switches the whole array to base64 when a line is not valid
    // UTF-8 — a READ of binary bytes — and says so in results_encoding.
    const binary = {
        payload: payloadOf([Buffer.from('SUCCESS,size=1,value=\xff', 'latin1').toString('base64')]),
        results_encoding: 'base64',
    };
    assert.deepEqual(decodeResultLines(binary), ['SUCCESS,size=1,value=\xff']);
});

test('parseBatchResponse splits the aggregate from the per-item responses', () => {
    const response = parseResponse(
        'SUCCESS,command=BATCH,target=PAIR_SET,requested=3,applied=2,failed=1,' +
            'first_error=item_1:invalid_absolute_key_format,payload=' +
            payloadOf(['SUCCESS,pair_set', 'ERROR,invalid_absolute_key_format', null])
    );
    const parsed = parseBatchResponse(response);
    assert.equal(parsed.target, 'PAIR_SET');
    assert.deepEqual(
        [parsed.requested, parsed.applied, parsed.failed],
        [3, 2, 1]
    );
    assert.equal(parsed.firstError, 'item_1:invalid_absolute_key_format');
    assert.equal(parsed.results[0].ok, true);
    assert.equal(parsed.results[1].error, 'invalid_absolute_key_format');
    assert.equal(parsed.results[2], null);
});

test('runBatch throws when the request itself is refused', async () => {
    const conn = fakeConn(() => 'ERROR,batch_requires_nonempty_items');
    await assert.rejects(runBatch(conn, 'PAIR_SET', ['ctx:a 1']), /batch_requires_nonempty_items/);
});

// --- auto-batching ---------------------------------------------------------

/** A batcher over a recording sink, with the hot-window threshold in hand. */
function makeBatcher(options = {}) {
    const sent = [];
    const batcher = new CommandBatcher(async (line) => {
        sent.push(line);
        const [command] = splitCommandLine(line);
        if (command !== 'BATCH' || fieldOf(line, 'items') === null) {
            return parseResponse('SUCCESS,pair_set');
        }
        const items = itemsOf(line);
        return parseResponse(
            `SUCCESS,command=BATCH,target=PAIR_SET,requested=${items.length},` +
                `applied=${items.length},failed=0,payload=` +
                payloadOf(items.map((_, index) => `SUCCESS,item=${index}`))
        );
    }, options);
    return { batcher, sent };
}

test('a cold command is never batched', async () => {
    const { batcher, sent } = makeBatcher();
    const responses = await Promise.all([
        batcher.submit('PAIR_SET ctx:a 1'),
        batcher.submit('PAIR_SET ctx:b 2'),
    ]);
    assert.deepEqual(sent, ['PAIR_SET ctx:a 1', 'PAIR_SET ctx:b 2']);
    assert.equal(responses.every((response) => response.ok), true);
    assert.equal(batcher.stats.batches, 0);
});

test('a burst of a hot command becomes one BATCH, one response per caller', async () => {
    const { batcher, sent } = makeBatcher({ threshold: 2 });
    const responses = await Promise.all(
        Array.from({ length: 6 }, (_, index) => batcher.submit(`PAIR_SET ctx:${index} ${index}`))
    );
    const batches = sent.filter((line) => line.startsWith('BATCH '));
    assert.equal(batches.length, 1);
    // The first call is what makes the command hot; it goes out on its own.
    assert.deepEqual(itemsOf(batches[0]), ['ctx:1 1', 'ctx:2 2', 'ctx:3 3', 'ctx:4 4', 'ctx:5 5']);
    // Every caller got its own line back, in its own order.
    assert.deepEqual(
        responses.slice(1).map((response) => response.fields.item),
        ['0', '1', '2', '3', '4']
    );
    assert.equal(batcher.stats.batched, 5);
});

test('a hot command awaited one at a time still never batches', async () => {
    const { batcher, sent } = makeBatcher({ threshold: 1 });
    for (let index = 0; index < 5; index += 1) {
        const response = await batcher.submit(`PAIR_SET ctx:${index} ${index}`);
        assert.equal(response.ok, true);
    }
    // minSize: a queue of one is sent as itself, so a sequential caller pays
    // nothing for the machinery.
    assert.equal(sent.some((line) => line.startsWith('BATCH ')), false);
    assert.equal(batcher.stats.batches, 0);
});

test('a command that cannot join the queue flushes it first, keeping order', async () => {
    const { batcher, sent } = makeBatcher({ threshold: 1 });
    const queued = [batcher.submit('PAIR_SET ctx:a 1'), batcher.submit('PAIR_SET ctx:b 2')];
    const other = batcher.submit('SYSTEM_STATS');
    await Promise.all([...queued, other]);
    assert.equal(sent.length, 2);
    assert.equal(sent[0].startsWith('BATCH PAIR_SET'), true);
    assert.equal(sent[1], 'SYSTEM_STATS');
});

test('switching commands mid-burst closes the open batch', async () => {
    const { batcher, sent } = makeBatcher({ threshold: 1 });
    const first = [batcher.submit('PAIR_SET ctx:a 1'), batcher.submit('PAIR_SET ctx:b 2')];
    const second = [batcher.submit('PAIR_GET ctx:a'), batcher.submit('PAIR_GET ctx:b')];
    await Promise.all([...first, ...second]);
    assert.equal(sent.length, 2);
    assert.equal(sent[0].startsWith('BATCH PAIR_SET'), true);
    assert.equal(sent[1].startsWith('BATCH PAIR_GET'), true);
});

test('excluded commands never batch, however hot they get', async () => {
    const { batcher, sent } = makeBatcher({ threshold: 1 });
    await Promise.all(AUTO_BATCH_DEFAULTS.exclude.map((name) => batcher.submit(name)));
    assert.equal(sent.some((line) => line.startsWith('BATCH ')), false);
});

test('disabled means the line goes out exactly as it came in', async () => {
    const { batcher, sent } = makeBatcher({ enabled: false, threshold: 1 });
    await Promise.all([batcher.submit('PAIR_SET ctx:a 1'), batcher.submit('PAIR_SET ctx:b 2')]);
    assert.deepEqual(sent, ['PAIR_SET ctx:a 1', 'PAIR_SET ctx:b 2']);
});

test('a batch the server refuses rejects every caller in it', async () => {
    const sent = [];
    const batcher = new CommandBatcher(async (line) => {
        sent.push(line);
        return parseResponse('ERROR,batch_too_many_items');
    }, { threshold: 1 });
    const pending = [batcher.submit('PAIR_SET ctx:a 1'), batcher.submit('PAIR_SET ctx:b 2')];
    await assert.rejects(Promise.all(pending), /batch_too_many_items/);
});

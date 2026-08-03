// Graph command-shape tests.
//
// The builders are pure, so most of this asserts the exact line that goes on
// the wire — which is the part a client gets silently wrong (a value with a
// space in it, a flag spelled `true`, a `references` clear that reads as an
// empty list). The round-trip helpers are driven by a scripted stand-in that
// answers through the real `parseResponse`, so the tests exercise the actual
// response grammar rather than a mock of the binder's own return values.

const test = require('node:test');
const assert = require('node:assert/strict');

const graph = require('../lib/graph');
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

const payloadOf = (value) => Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
const fieldOf = (line, name) => {
    const match = line.match(new RegExp(`(?:^| )${name}=([^ ]*)`));
    return match ? match[1] : null;
};
const decodeField = (line, name) =>
    JSON.parse(Buffer.from(fieldOf(line, name), 'base64').toString('utf8'));

test('buildNodeSet encodes labels as a list and props as base64 JSON', () => {
    const line = graph.buildNodeSet({
        id: 'module:parser',
        labels: ['module', 'code'],
        props: { title: 'the parser module', depth: 2 },
    });
    assert.match(line, /^GRAPH_NODE_SET id=module:parser labels=module,code props=/);
    // The props value must survive the whitespace split that GRAPH_* applies.
    assert.equal(line.split(' ').length, 4);
    assert.deepEqual(decodeField(line, 'props'), { title: 'the parser module', depth: 2 });
});

test('buildNodeSet distinguishes omitted references from cleared ones', () => {
    assert.equal(fieldOf(graph.buildNodeSet({ id: 'n1' }), 'references'), null);
    assert.equal(fieldOf(graph.buildNodeSet({ id: 'n1', references: null }), 'references'), '-');
    assert.deepEqual(decodeField(graph.buildNodeSet({ id: 'n1', references: [] }), 'references'), []);
});

test('buildNodeSet normalizes references and bounds them at the server cap', () => {
    const line = graph.buildNodeSet({
        id: 'n1',
        references: [
            { id: ' r1 ', text: '  The parser rejects non-finite values.  ', source: ' design ' },
            { text: 'A second sentence.', ordinal: 7 },
            { text: '   ' },
            null,
        ],
    });
    assert.deepEqual(decodeField(line, 'references'), [
        { text: 'The parser rejects non-finite values.', id: 'r1', source: 'design', ordinal: 0 },
        { text: 'A second sentence.', ordinal: 7 },
    ]);

    const many = Array.from({ length: graph.MAX_NODE_REFERENCES + 10 }, (_, index) => ({
        text: `sentence ${index}`,
    }));
    assert.equal(
        decodeField(graph.buildNodeSet({ id: 'n1', references: many }), 'references').length,
        graph.MAX_NODE_REFERENCES
    );
});

test('buildNodeSet requires an id', () => {
    assert.throws(() => graph.buildNodeSet({ id: '  ' }), /requires an id/);
});

test('flag arguments are spelled 1/0 and absent when undeclared', () => {
    assert.equal(graph.buildNodeDel({ id: 'n1', cascade: true }), 'GRAPH_NODE_DEL id=n1 cascade=1');
    assert.equal(graph.buildNodeDel({ id: 'n1' }), 'GRAPH_NODE_DEL id=n1 cascade=0');
    assert.equal(
        graph.buildEdgeSet({ from: 'a', to: 'b', type: 'follows' }),
        'GRAPH_EDGE_SET from=a to=b type=follows'
    );
    assert.equal(
        graph.buildEdgeSet({ from: 'a', to: 'b', type: 'follows', directed: false, autocreate: false }),
        'GRAPH_EDGE_SET from=a to=b type=follows directed=0 autocreate=0'
    );
});

test('buildRecall carries the reference and expansion options additively', () => {
    assert.equal(graph.buildRecall({ seeds: ['a', 'b'] }), 'GRAPH_RECALL seeds=a,b');

    const line = graph.buildRecall({
        seeds: 'mission,focus',
        hops: 2,
        precision: 0.05,
        limit: 32,
        direction: 'both',
        expand: 'none',
        includeSeeds: true,
        references: true,
        referenceLimit: 48,
    });
    assert.equal(
        line,
        'GRAPH_RECALL seeds=mission,focus hops=2 precision=0.05 direction=both limit=32 ' +
            'expand=none include_seeds=1 references=1 reference_limit=48'
    );
});

test('buildRecall spells "no synonym expansion" as the dash the server expects', () => {
    assert.equal(fieldOf(graph.buildRecall({ seeds: ['a'] }), 'synonym_types'), null);
    assert.equal(fieldOf(graph.buildRecall({ seeds: ['a'], synonymTypes: [] }), 'synonym_types'), '-');
    assert.equal(
        fieldOf(graph.buildRecall({ seeds: ['a'], synonymTypes: ['alias', 'aka'] }), 'synonym_types'),
        'alias,aka'
    );
});

test('buildRecall rejects an empty seed set rather than sending seeds=', () => {
    assert.throws(() => graph.buildRecall({ seeds: [] }), /at least one seed/);
    assert.throws(() => graph.buildRecall({ seeds: ['', '  '] }), /at least one seed/);
});

test('termIndex exposes weighted vocabulary statistics additively', async () => {
    const conn = fakeConn(() =>
        'SUCCESS,command=GRAPH_TERM_INDEX,action=stats,enabled=1,entries=12,' +
        'weighted=1,nodes=6,tokens=8,trigrams=31'
    );
    const result = await graph.termIndex(conn);
    assert.equal(conn.lines[0], 'GRAPH_TERM_INDEX action=stats');
    assert.equal(result.weighted, true);
    assert.equal(result.tokens, 8);
    assert.equal(result.trigrams, 31);
});

test('buildEdgeSetBatch base64-encodes the items and any shared props default', () => {
    const line = graph.buildEdgeSetBatch(
        [{ from: 'a', to: 'b', type: 'mentions' }],
        { continueOnError: true, type: 'mentions', props: { src: 'episode 1' } }
    );
    assert.equal(line.split(' ').length, 5);
    assert.deepEqual(decodeField(line, 'items'), [{ from: 'a', to: 'b', type: 'mentions' }]);
    assert.deepEqual(decodeField(line, 'props'), { src: 'episode 1' });
    assert.equal(fieldOf(line, 'continue_on_error'), '1');
});

test('setEdgeBatch chunks on request and merges the counters', async () => {
    const conn = fakeConn(() => 'SUCCESS,requested=2,applied=2,created=1,updated=1,failed=0');
    const items = Array.from({ length: 6 }, (_, index) => ({ from: 'a', to: `b${index}`, type: 't' }));
    const totals = await graph.setEdgeBatch(conn, items, { chunkSize: 2 });
    assert.equal(conn.lines.length, 3);
    assert.deepEqual(totals, { requested: 6, applied: 6, created: 3, updated: 3, failed: 0 });
    assert.equal(decodeField(conn.lines[0], 'items').length, 2);
});

test('setEdgeBatch sends one batch when no chunk size is given', async () => {
    const conn = fakeConn(() => 'SUCCESS,requested=6,applied=6,created=6,updated=0,failed=0');
    const items = Array.from({ length: 6 }, (_, index) => ({ from: 'a', to: `b${index}`, type: 't' }));
    await graph.setEdgeBatch(conn, items);
    assert.equal(conn.lines.length, 1);
});

test('a missing node is an answer, not a fault', async () => {
    const conn = fakeConn(() => 'ERROR,node_not_found');
    assert.equal(await graph.getNode(conn, 'absent'), null);
    assert.deepEqual(await graph.deleteNode(conn, 'absent'), { deleted: false, id: 'absent' });
    assert.deepEqual(await graph.degree(conn, { id: 'absent' }), { degree: 0, weighted: 0 });
    assert.deepEqual(await graph.neighbors(conn, { id: 'absent' }), {
        edges: [],
        count: 0,
        nextCursor: null,
    });
    assert.deepEqual(await graph.neighborTypes(conn, { id: 'absent' }), {
        types: [],
        count: 0,
        nextCursor: null,
    });
});

test('a real failure still throws', async () => {
    const conn = fakeConn(() => 'ERROR,invalid_direction');
    await assert.rejects(() => graph.getNode(conn, 'n1'), /invalid_direction/);
    await assert.rejects(() => graph.neighbors(conn, { id: 'n1' }), /invalid_direction/);
});

test('neighborsAll pages until the cursor is exhausted', async () => {
    const pages = [
        `SUCCESS,count=2,next_cursor=x0102,payload=${payloadOf([{ to: 'b1' }, { to: 'b2' }])}`,
        `SUCCESS,count=1,next_cursor=*,payload=${payloadOf([{ to: 'b3' }])}`,
    ];
    const conn = fakeConn((_line, index) => pages[index]);
    const seen = [];
    for await (const edge of graph.neighborsAll(conn, { id: 'a', limit: 2 })) seen.push(edge.to);
    assert.deepEqual(seen, ['b1', 'b2', 'b3']);
    // The cursor token must travel back verbatim, not re-encoded.
    assert.match(conn.lines[1], / cursor=x0102(?: |$)/);
});

test('neighborsAll stops at maxEdges without asking for another page', async () => {
    const conn = fakeConn(
        () => `SUCCESS,count=2,next_cursor=x01,payload=${payloadOf([{ to: 'b1' }, { to: 'b2' }])}`
    );
    const seen = [];
    for await (const edge of graph.neighborsAll(conn, { id: 'a', maxEdges: 1 })) seen.push(edge.to);
    assert.deepEqual(seen, ['b1']);
    assert.equal(conn.lines.length, 1);
});

test('recall returns the hydrated references and the reference count', async () => {
    const conn = fakeConn(
        () =>
            'SUCCESS,command=GRAPH_RECALL,references=1,count=1,truncated=0,decay=0.55,' +
            'cache_decay=1.1,decay_relations=2,decay_profile=abcd,payload=' +
            payloadOf({
                seeds: [{ term: 'parser', matches: [{ id: 'module:parser' }] }],
                associations: [
                    {
                        id: 'module:parser',
                        score: 0.8,
                        source_count: 1,
                        references: [{ id: 'r1', text: 'The parser rejects non-finite values.' }],
                    },
                ],
            })
    );
    const result = await graph.recall(conn, { seeds: ['parser'], references: true, referenceLimit: 8 });
    assert.equal(result.references, 1);
    assert.equal(result.decay, 0.55);
    assert.equal(result.cacheDecay, 1.1);
    assert.equal(result.decayRelations, 2);
    assert.equal(result.decayProfile, 'abcd');
    assert.equal(result.associations[0].references[0].text, 'The parser rejects non-finite values.');
    assert.match(conn.lines[0], / references=1 reference_limit=8$/);
});

test('detached recall returns a job id and retrieves its decoded result by id', async () => {
    const payload = payloadOf({
        seeds: [{ term: 'parser', matches: [{ id: 'module:parser' }] }],
        associations: [{ id: 'module:parser', score: 0.8, source_count: 1 }],
    });
    const conn = fakeConn((line) => {
        if (line.startsWith('JOB submit')) {
            const submitted = Buffer.from(fieldOf(line, 'command'), 'base64').toString('utf8');
            assert.equal(submitted, 'GRAPH_RECALL seeds=parser hops=2');
            // No budget means "use the detached maximum" on the server.
            assert.equal(fieldOf(submitted, 'budget'), null);
            return 'SUCCESS,job=graph_recall_7,kind=graph_recall,state=queued';
        }
        assert.equal(line, 'JOB fetch id=graph_recall_7');
        return `SUCCESS,job=graph_recall_7,command=GRAPH_RECALL,references=0,truncated=0,payload=${payload}`;
    });

    const jobId = await graph.recallAsync(conn, { seeds: ['parser'], hops: 2 });
    assert.equal(jobId, 'graph_recall_7');
    const result = await graph.fetchRecall(conn, jobId);
    assert.equal(result.jobId, jobId);
    assert.equal(result.associations[0].id, 'module:parser');
});

test('recall refuses more seeds than the server accepts', async () => {
    const conn = fakeConn(() => 'SUCCESS');
    const seeds = Array.from({ length: graph.MAX_RECALL_SEEDS + 1 }, (_, index) => `s${index}`);
    await assert.rejects(() => graph.recall(conn, { seeds }), RangeError);
});

test('recallBatched merges scores by noisy-OR and keeps the evidence', async () => {
    const seeds = Array.from({ length: graph.MAX_RECALL_SEEDS + 1 }, (_, index) => `s${index}`);
    const conn = fakeConn(
        (_line, index) =>
            'SUCCESS,count=1,payload=' +
            payloadOf({
                associations: [
                    {
                        id: 'shared',
                        score: 0.5,
                        source_count: 1,
                        sources: [{ seed: `s${index}`, activation: 0.5 }],
                        references: [{ id: `r${index}`, text: `evidence ${index}` }],
                    },
                ],
            })
    );
    const merged = await graph.recallBatched(conn, { seeds });
    assert.equal(conn.lines.length, 2);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].score, 0.75);
    assert.equal(merged[0].sourceCount, 2);
    assert.deepEqual(merged[0].references.map((reference) => reference.id), ['r0', 'r1']);
});

test('buildQuery passes the clause verbatim and refuses an empty one', () => {
    assert.equal(
        graph.buildQuery("MATCH (id='alice')-[:follows]->(*) RETURN edges LIMIT 8"),
        "GRAPH_QUERY MATCH (id='alice')-[:follows]->(*) RETURN edges LIMIT 8"
    );
    assert.throws(() => graph.buildQuery('   '), /requires a MATCH clause/);
});

test('query reports the return mode, the match count and the payload', async () => {
    const conn = fakeConn(() => `SUCCESS,return=nodes,matches=2,next_cursor=*,payload=${payloadOf(['a', 'b'])}`);
    const result = await graph.query(conn, "MATCH (id='a')-[:*]->(*) RETURN nodes");
    assert.deepEqual(result, { mode: 'nodes', matches: 2, nextCursor: null, payload: ['a', 'b'] });
});

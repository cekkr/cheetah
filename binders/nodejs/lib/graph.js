// Thin, typed access to Cheetah's graph and associative-recall commands.
//
// The authority for every format here is this repository's handbook (AGENTS.md)
// and the `ExecuteCommand` switch — this module only spells the commands, it
// does not restate their semantics.
//
// Three encoding rules the command family imposes, and the reason each one
// matters here:
//
//   1. `GRAPH_*` speaks `key=value` tokens split on **whitespace**
//      (src/database.go → parseKeyValueArgs uses strings.Fields), so no value
//      may contain a space. Anything free-form — props, batch items — travels
//      base64. A filename with a space in it is the common case.
//   2. The split is on the **first** `=` (strings.Cut), so base64 padding is
//      safe inside a value.
//   3. `GRAPH_RECALL` accepts at most 32 seeds per call
//      (graphRecallMaxSeeds). Callers with more must batch and merge, which is
//      what `recallBatched` below does.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, decodePayload, numericField } = require('./protocol');

/** Hard server-side cap on `seeds=`; batching above it is the caller's job. */
const MAX_RECALL_SEEDS = 32;

/** JSON → the base64 spelling a `key=value` token can carry. */
function encodeJsonArgument(value) {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
}

async function commandOrThrow(conn, line, what) {
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah ${what} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return response;
}

/** Upsert one node. Omitted fields keep whatever is stored. */
async function setNode(conn, { id, labels = null, props = null }) {
    return commandOrThrow(conn, buildKeyValueCommand('GRAPH_NODE_SET', {
        id,
        labels: Array.isArray(labels) ? labels.join(',') : labels,
        props: props ? encodeJsonArgument(props) : null,
    }), `GRAPH_NODE_SET ${id}`);
}

/** Read one node record back, or null when it has never been written. */
async function getNode(conn, id) {
    const response = await conn.send(buildKeyValueCommand('GRAPH_NODE_GET', { id }));
    if (!response.ok) {
        if (response.error && /not_found/.test(response.error)) return null;
        throw new CheetahError(`cheetah GRAPH_NODE_GET ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return decodePayload(response.fields);
}

/**
 * Upsert many edges in one round trip. `defaults` fills the fields every item
 * shares, so the payload only carries what differs.
 *
 * Returns the server's own accounting, `applied` included: a batch that reports
 * fewer applied than requested has silently dropped edges, and a caller that
 * ignores the difference builds an index with holes in it.
 */
async function setEdgeBatch(conn, items, { continueOnError = true, ...defaults } = {}) {
    if (!Array.isArray(items) || items.length === 0) {
        return { requested: 0, applied: 0, created: 0, updated: 0, failed: 0 };
    }
    const response = await commandOrThrow(conn, buildKeyValueCommand('GRAPH_EDGE_SET_BATCH', {
        items: encodeJsonArgument(items),
        continue_on_error: continueOnError ? 1 : 0,
        ...defaults,
    }), `GRAPH_EDGE_SET_BATCH of ${items.length}`);
    return {
        requested: numericField(response.fields, 'requested', items.length),
        applied: numericField(response.fields, 'applied', 0),
        created: numericField(response.fields, 'created', 0),
        updated: numericField(response.fields, 'updated', 0),
        failed: numericField(response.fields, 'failed', 0),
    };
}

/**
 * How many edges a node carries. The cheapest question in the family — no edge
 * record is hydrated — which is what makes it usable as a per-seed stop-word
 * test at query time.
 */
async function degree(conn, { id, direction = 'out', type = null, weighted = false }) {
    const response = await conn.send(buildKeyValueCommand('GRAPH_DEGREE', {
        id,
        direction,
        type,
        weighted: weighted ? 1 : 0,
    }));
    // A node nobody has written yet is a legitimate answer of zero, not a fault.
    if (!response.ok) {
        if (response.error && /not_found/.test(response.error)) return { degree: 0, weighted: 0 };
        throw new CheetahError(`cheetah GRAPH_DEGREE ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return {
        degree: numericField(response.fields, 'degree', 0),
        weighted: numericField(response.fields, 'weighted_degree', 0),
    };
}

/**
 * Spread activation from every seed at once and return what they co-activate.
 *
 * `associations[].source_count` is how many of the seeds reached that node and
 * `score` is the noisy-OR of their activations — which is exactly the question
 * "given these observations, what do they have in common?".
 */
async function recall(conn, {
    seeds,
    hops = 1,
    decay = 1,
    precision = 0.05,
    direction = 'out',
    type = null,
    limit = 64,
    branchLimit = 1024,
    budget = 65536,
    minSources = 1,
}) {
    if (!Array.isArray(seeds) || seeds.length === 0) return { seeds: [], associations: [] };
    if (seeds.length > MAX_RECALL_SEEDS) {
        throw new RangeError(
            `GRAPH_RECALL accepts at most ${MAX_RECALL_SEEDS} seeds, got ${seeds.length}; use recallBatched`
        );
    }
    const response = await commandOrThrow(conn, buildKeyValueCommand('GRAPH_RECALL', {
        seeds: seeds.join(','),
        hops,
        decay,
        precision,
        direction,
        type,
        limit,
        branch_limit: branchLimit,
        budget,
        min_sources: minSources,
    }), `GRAPH_RECALL over ${seeds.length} seeds`);
    const payload = decodePayload(response.fields) || {};
    return {
        seeds: payload.seeds || [],
        associations: payload.associations || [],
        truncated: numericField(response.fields, 'truncated', 0) > 0,
    };
}

/**
 * `recall` over any number of seeds.
 *
 * Scores are combined with a noisy-OR across batches, which is the same rule the
 * server uses to combine seeds *inside* one batch — so splitting 40 seeds into
 * two calls ranks the same way as one impossible call of 40 would. `sourceCount`
 * sums instead, because the batches are disjoint sets of seeds.
 *
 * `sources` — **which** seeds reached each hit, with how much activation — is
 * kept rather than collapsed. It is the only part of the answer a caller can
 * reweight: the server has no way to know that some seeds are far more telling
 * than others, and that judgement is what separates two hits that both
 * saturated the noisy-OR.
 */
async function recallBatched(conn, { seeds, ...options }) {
    const unique = [...new Set(seeds)];
    const merged = new Map();
    for (let at = 0; at < unique.length; at += MAX_RECALL_SEEDS) {
        const batch = unique.slice(at, at + MAX_RECALL_SEEDS);
        const { associations } = await recall(conn, { seeds: batch, ...options });
        for (const association of associations) {
            const current = merged.get(association.id) ||
                { id: association.id, score: 0, sourceCount: 0, sources: new Map() };
            current.score = 1 - (1 - current.score) * (1 - Number(association.score || 0));
            current.sourceCount += Number(association.source_count || 0);
            for (const source of association.sources || []) {
                const activation = Number(source.activation || 0);
                current.sources.set(source.seed, Math.max(current.sources.get(source.seed) || 0, activation));
            }
            merged.set(association.id, current);
        }
    }
    return [...merged.values()].sort((left, right) =>
        right.score - left.score || right.sourceCount - left.sourceCount
    );
}

module.exports = {
    MAX_RECALL_SEEDS,
    degree,
    encodeJsonArgument,
    getNode,
    recall,
    recallBatched,
    setEdgeBatch,
    setNode,
};

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
//      may contain a space. Anything free-form — props, references, batch
//      items — travels base64. A filename with a space in it is the common case.
//   2. The split is on the **first** `=` (strings.Cut), so base64 padding is
//      safe inside a value.
//   3. `GRAPH_RECALL` accepts at most 32 seeds per call
//      (graphRecallMaxSeeds). Callers with more must batch and merge, which is
//      what `recallBatched` below does.
//
// The module is split in two halves. The `build*` functions are pure: they
// return the command line and nothing else, so a caller that assembles its own
// batch (several commands written to one connection in one go) gets the same
// encoding as the round-trip helpers instead of hand-rolling base64 and `x<hex>`
// a second time. The `async` functions are those builders plus one `send`.

const { CheetahError } = require('./client');
const jobs = require('./jobs');
const { buildKeyValueCommand, decodePayload, numericField, parseCursor } = require('./protocol');

/** Hard server-side cap on `seeds=`; batching above it is the caller's job. */
const MAX_RECALL_SEEDS = 32;

/**
 * Bound on `GraphNodeRecord.References` (graph.go): at most 64 sentences per
 * node. Trimming here keeps a re-mirror idempotent instead of letting the
 * server silently decide which ones survive.
 */
const MAX_NODE_REFERENCES = 64;

/** JSON → the base64 spelling a `key=value` token can carry. */
function encodeJsonArgument(value) {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
}

/** `1`/`0`, the only spelling the flag arguments accept. A `null` stays absent. */
function flag(value) {
    if (value === undefined || value === null) return null;
    return value ? 1 : 0;
}

/** `a,b,c` from an array, a string, or null. Empty collapses to absent. */
function commaList(value) {
    if (value === undefined || value === null) return null;
    const parts = (Array.isArray(value) ? value : String(value).split(','))
        .map((entry) => String(entry).trim())
        .filter(Boolean);
    return parts.length > 0 ? parts.join(',') : null;
}

/**
 * Normalize `references=` entries to the `{id,text,source?,ordinal?}` shape the
 * server stores. A missing id is SHA-256-derived server-side, but supplying one
 * keeps repeated mirrors of the same sentence idempotent rather than
 * accumulating near-duplicates.
 */
function normalizeReferences(references) {
    return (Array.isArray(references) ? references : [])
        .filter((reference) => reference && typeof reference.text === 'string' && reference.text.trim() !== '')
        .slice(0, MAX_NODE_REFERENCES)
        .map((reference, ordinal) => {
            const entry = { text: reference.text.trim() };
            if (reference.id !== undefined && reference.id !== null && String(reference.id).trim() !== '') {
                entry.id = String(reference.id).trim();
            }
            if (reference.source !== undefined && reference.source !== null && String(reference.source).trim() !== '') {
                entry.source = String(reference.source).trim();
            }
            entry.ordinal = Number.isFinite(reference.ordinal) ? Math.max(0, Math.floor(reference.ordinal)) : ordinal;
            return entry;
        });
}

// ---------------------------------------------------------------------------
// Command builders (pure)
// ---------------------------------------------------------------------------

/**
 * `GRAPH_NODE_SET`. Omitted fields keep whatever is stored, which is what makes
 * a metadata-only update cheap; `references: null` is the documented `-` that
 * clears them, and is therefore spelled differently from "leave them alone".
 */
function buildNodeSet({ id, labels = null, props = null, references = undefined }) {
    if (id === undefined || id === null || String(id).trim() === '') {
        throw new CheetahError('cheetah GRAPH_NODE_SET requires an id');
    }
    const fields = {
        id: String(id).trim(),
        labels: commaList(labels),
        props: props === undefined || props === null ? null : encodeJsonArgument(props),
    };
    if (references === null) fields.references = '-';
    else if (references !== undefined) fields.references = encodeJsonArgument(normalizeReferences(references));
    return buildKeyValueCommand('GRAPH_NODE_SET', fields);
}

function buildNodeGet(id) {
    return buildKeyValueCommand('GRAPH_NODE_GET', { id });
}

function buildNodeDel({ id, cascade = false }) {
    return buildKeyValueCommand('GRAPH_NODE_DEL', { id, cascade: flag(cascade) });
}

function buildEdgeSet({
    from,
    to,
    type = null,
    weight = null,
    directed = null,
    props = null,
    confidence = null,
    modality = null,
    ambiguity = null,
    autocreate = null,
}) {
    if (!from || !to) throw new CheetahError('cheetah GRAPH_EDGE_SET requires from and to');
    return buildKeyValueCommand('GRAPH_EDGE_SET', {
        from,
        to,
        type,
        weight,
        directed: flag(directed),
        confidence,
        modality,
        ambiguity,
        autocreate: flag(autocreate),
        props: props === undefined || props === null ? null : encodeJsonArgument(props),
    });
}

function buildEdgeSetBatch(items, { continueOnError = true, ...defaults } = {}) {
    const fields = { items: encodeJsonArgument(items), continue_on_error: flag(continueOnError) };
    for (const [key, value] of Object.entries(defaults)) {
        if (value === undefined || value === null) continue;
        fields[key] = key === 'props' ? encodeJsonArgument(value) : value;
    }
    return buildKeyValueCommand('GRAPH_EDGE_SET_BATCH', fields);
}

function buildEdgeGet({ from, to, type = null, directed = null }) {
    return buildKeyValueCommand('GRAPH_EDGE_GET', { from, to, type, directed: flag(directed) });
}

function buildEdgeDel({ from, to, type = null, directed = null }) {
    return buildKeyValueCommand('GRAPH_EDGE_DEL', { from, to, type, directed: flag(directed) });
}

function buildNeighbors({ id, direction = null, type = null, limit = null, cursor = null }) {
    return buildKeyValueCommand('GRAPH_NEIGHBORS', { id, direction, type, limit, cursor });
}

function buildNeighborTypes({ id, direction = null, limit = null, cursor = null, weighted = null }) {
    return buildKeyValueCommand('GRAPH_NEIGHBOR_TYPES', {
        id,
        direction,
        limit,
        cursor,
        weighted: flag(weighted),
    });
}

function buildDegree({ id, direction = 'out', type = null, weighted = false }) {
    return buildKeyValueCommand('GRAPH_DEGREE', { id, direction, type, weighted: flag(weighted) });
}

/**
 * `GRAPH_RECALL`. `references=1` hydrates the complete stored sentences (and
 * the episodic payloads named by an edge's `props.src`) in the same round trip,
 * which is the difference between an answer a model can read and a list of ids
 * it has to go fetch one at a time.
 */
function buildRecall({
    seeds,
    hops = null,
    decay = null,
    precision = null,
    direction = null,
    type = null,
    limit = null,
    branchLimit = null,
    budget = null,
    minSources = null,
    expand = null,
    synonymTypes = undefined,
    includeSeeds = null,
    seedLimit = null,
    references = null,
    referenceLimit = null,
}) {
    const encodedSeeds = commaList(seeds);
    if (!encodedSeeds) throw new CheetahError('cheetah GRAPH_RECALL requires at least one seed');
    return buildKeyValueCommand('GRAPH_RECALL', {
        seeds: encodedSeeds,
        hops,
        decay,
        precision,
        direction,
        type: commaList(type),
        limit,
        branch_limit: branchLimit,
        budget,
        min_sources: minSources,
        expand,
        // `-` disables synonym expansion entirely; omitted keeps the default set.
        synonym_types: synonymTypes === undefined ? null : (commaList(synonymTypes) || '-'),
        include_seeds: flag(includeSeeds),
        seed_limit: seedLimit,
        references: flag(references),
        reference_limit: referenceLimit,
    });
}

function buildSimilar({ id, by = null, limit = null }) {
    return buildKeyValueCommand('GRAPH_SIMILAR', { id, by, limit });
}

function buildTermIndex({ action = 'stats', limit = null, cursor = null } = {}) {
    return buildKeyValueCommand('GRAPH_TERM_INDEX', { action, limit, cursor });
}

/**
 * `GRAPH_QUERY` is the one command in the family that speaks a clause language
 * rather than `key=value`, so the clause travels verbatim. The left node must
 * be id-anchored: a wildcard-left pattern is rejected rather than degraded into
 * a scan.
 */
function buildQuery(clause) {
    const text = String(clause || '').trim();
    if (!text) throw new CheetahError('cheetah GRAPH_QUERY requires a MATCH clause');
    return `GRAPH_QUERY ${text}`;
}

// ---------------------------------------------------------------------------
// Round-trip helpers
// ---------------------------------------------------------------------------

async function commandOrThrow(conn, line, what) {
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah ${what} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return response;
}

/** True when the server said "there is no such node/edge", not "I failed". */
function isNotFound(response) {
    return Boolean(response && response.error && /not_found/.test(response.error));
}

/** Upsert one node. Omitted fields keep whatever is stored. */
async function setNode(conn, node) {
    return commandOrThrow(conn, buildNodeSet(node), `GRAPH_NODE_SET ${node.id}`);
}

/** Read one node record back, or null when it has never been written. */
async function getNode(conn, id) {
    const response = await conn.send(buildNodeGet(id));
    if (!response.ok) {
        if (isNotFound(response)) return null;
        throw new CheetahError(`cheetah GRAPH_NODE_GET ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return decodePayload(response.fields);
}

/**
 * Delete a node, optionally with its incident edges. A node that was already
 * gone answers `{deleted: false}`: the caller asked for a state, and it holds.
 */
async function deleteNode(conn, id, { cascade = false } = {}) {
    const response = await conn.send(buildNodeDel({ id, cascade }));
    if (!response.ok) {
        if (isNotFound(response)) return { deleted: false, id };
        throw new CheetahError(`cheetah GRAPH_NODE_DEL ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return { deleted: true, id: response.fields.id || id };
}

/** Upsert one edge. Missing endpoints are stubbed out unless `autocreate: false`. */
async function setEdge(conn, edge) {
    const response = await commandOrThrow(conn, buildEdgeSet(edge), `GRAPH_EDGE_SET ${edge.from}->${edge.to}`);
    return { id: response.fields.id || null, response };
}

async function getEdge(conn, selector) {
    const response = await conn.send(buildEdgeGet(selector));
    if (!response.ok) {
        if (isNotFound(response)) return null;
        throw new CheetahError(`cheetah GRAPH_EDGE_GET failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return decodePayload(response.fields);
}

async function deleteEdge(conn, selector) {
    const response = await conn.send(buildEdgeDel(selector));
    if (!response.ok) {
        if (isNotFound(response)) return { deleted: false };
        throw new CheetahError(`cheetah GRAPH_EDGE_DEL failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return { deleted: true, id: response.fields.id || null };
}

/**
 * Upsert many edges in one round trip. `defaults` fills the fields every item
 * shares, so the payload only carries what differs.
 *
 * Returns the server's own accounting, `applied` included: a batch that reports
 * fewer applied than requested has silently dropped edges, and a caller that
 * ignores the difference builds an index with holes in it.
 *
 * `chunkSize` splits an oversized list into several batches and merges the
 * counters. It is off by default because the split changes the failure
 * granularity — the command is not a transaction, so two batches can leave a
 * different partial state than one would.
 */
async function setEdgeBatch(conn, items, { continueOnError = true, chunkSize = null, ...defaults } = {}) {
    if (!Array.isArray(items) || items.length === 0) {
        return { requested: 0, applied: 0, created: 0, updated: 0, failed: 0 };
    }
    const size = Number.isFinite(chunkSize) && chunkSize > 0 ? Math.floor(chunkSize) : items.length;
    const totals = { requested: 0, applied: 0, created: 0, updated: 0, failed: 0 };
    for (let at = 0; at < items.length; at += size) {
        const batch = items.slice(at, at + size);
        const response = await commandOrThrow(
            conn,
            buildEdgeSetBatch(batch, { continueOnError, ...defaults }),
            `GRAPH_EDGE_SET_BATCH of ${batch.length}`
        );
        totals.requested += numericField(response.fields, 'requested', batch.length);
        totals.applied += numericField(response.fields, 'applied', 0);
        totals.created += numericField(response.fields, 'created', 0);
        totals.updated += numericField(response.fields, 'updated', 0);
        totals.failed += numericField(response.fields, 'failed', 0);
    }
    return totals;
}

/**
 * One page of a node's edges. `cursor` comes back verbatim in `nextCursor`, or
 * `null` once the scan is exhausted. `direction=both` merges both sides and
 * does not accept a cursor.
 */
async function neighbors(conn, { id, direction = null, type = null, limit = null, cursor = null }) {
    const response = await conn.send(buildNeighbors({ id, direction, type, limit, cursor }));
    if (!response.ok) {
        // A node nobody has written yet has no edges; that is an answer.
        if (isNotFound(response)) return { edges: [], count: 0, nextCursor: null };
        throw new CheetahError(`cheetah GRAPH_NEIGHBORS ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return {
        edges: decodePayload(response.fields) || [],
        count: numericField(response.fields, 'count', 0),
        nextCursor: parseCursor(response.fields),
    };
}

/** Every edge of a node, paged. `maxEdges` bounds an otherwise unbounded sweep. */
async function* neighborsAll(conn, { id, maxEdges = Infinity, ...options }) {
    let cursor = options.cursor || null;
    let yielded = 0;
    do {
        const page = await neighbors(conn, { id, ...options, cursor });
        for (const edge of page.edges) {
            if (yielded >= maxEdges) return;
            yielded += 1;
            yield edge;
        }
        cursor = page.nextCursor;
    } while (cursor);
}

/**
 * The relation histogram of a node — `[{type,count,weighted}]` — without
 * hydrating a single edge record. The fast path for feature extraction, and the
 * cheap "do I already know anything about this?" probe before a write.
 */
async function neighborTypes(conn, { id, direction = null, limit = null, cursor = null, weighted = null }) {
    const response = await conn.send(buildNeighborTypes({ id, direction, limit, cursor, weighted }));
    if (!response.ok) {
        if (isNotFound(response)) return { types: [], count: 0, nextCursor: null };
        throw new CheetahError(`cheetah GRAPH_NEIGHBOR_TYPES ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    const payload = decodePayload(response.fields);
    return {
        types: Array.isArray(payload) ? payload : [],
        count: numericField(response.fields, 'count', 0),
        nextCursor: parseCursor(response.fields),
    };
}

/**
 * How many edges a node carries. The cheapest question in the family — no edge
 * record is hydrated — which is what makes it usable as a per-seed stop-word
 * test at query time.
 */
async function degree(conn, { id, direction = 'out', type = null, weighted = false }) {
    const response = await conn.send(buildDegree({ id, direction, type, weighted }));
    // A node nobody has written yet is a legitimate answer of zero, not a fault.
    if (!response.ok) {
        if (isNotFound(response)) return { degree: 0, weighted: 0 };
        throw new CheetahError(`cheetah GRAPH_DEGREE ${id} failed: ${response.error || response.raw}`, {
            response,
        });
    }
    return {
        degree: numericField(response.fields, 'degree', 0),
        weighted: numericField(response.fields, 'weighted_degree', 0),
    };
}

/** Run one `MATCH` clause. `payload` shape follows the `RETURN` mode. */
async function query(conn, clause) {
    const response = await commandOrThrow(conn, buildQuery(clause), 'GRAPH_QUERY');
    return {
        mode: response.fields.return || null,
        matches: numericField(response.fields, 'matches', 0),
        nextCursor: parseCursor(response.fields),
        payload: decodePayload(response.fields),
    };
}

/**
 * Spread activation from every seed at once and return what they co-activate.
 *
 * `associations[].source_count` is how many of the seeds reached that node and
 * `score` is the noisy-OR of their activations — which is exactly the question
 * "given these observations, what do they have in common?".
 *
 * The defaults are deliberately unchanged from this binder's first release;
 * `expand`, `synonymTypes`, `includeSeeds`, `seedLimit`, `references` and
 * `referenceLimit` are additive and stay absent unless asked for, so the
 * server's own defaults apply.
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
    expand = null,
    synonymTypes = undefined,
    includeSeeds = null,
    seedLimit = null,
    references = null,
    referenceLimit = null,
}) {
    if (!Array.isArray(seeds) || seeds.length === 0) return { seeds: [], associations: [] };
    if (seeds.length > MAX_RECALL_SEEDS) {
        throw new RangeError(
            `GRAPH_RECALL accepts at most ${MAX_RECALL_SEEDS} seeds, got ${seeds.length}; use recallBatched`
        );
    }
    const response = await commandOrThrow(conn, buildRecall({
        seeds,
        hops,
        decay,
        precision,
        direction,
        type,
        limit,
        branchLimit,
        budget,
        minSources,
        expand,
        synonymTypes,
        includeSeeds,
        seedLimit,
        references,
        referenceLimit,
    }), `GRAPH_RECALL over ${seeds.length} seeds`);
    return recallResult(response);
}

/** Decode the GRAPH_RECALL response returned directly or through JOB fetch. */
function recallResult(response, jobId = null) {
    const payload = decodePayload(response.fields) || {};
    const result = {
        seeds: payload.seeds || [],
        associations: payload.associations || [],
        references: numericField(response.fields, 'references', 0),
        truncated: numericField(response.fields, 'truncated', 0) > 0,
        decay: numericField(response.fields, 'decay', 0),
        cacheDecay: numericField(response.fields, 'cache_decay', 1),
        decayRelations: numericField(response.fields, 'decay_relations', 0),
        decayProfile: response.fields.decay_profile || null,
    };
    const resolvedJobId = response.fields.job || jobId;
    if (resolvedJobId) result.jobId = resolvedJobId;
    return result;
}

/**
 * Detach one recall and return its `graph_recall_<n>` id.
 *
 * `budget` intentionally stays absent unless the caller supplies it: the
 * server gives detached recall its maximum bounded sweep by default, unlike
 * the smaller interactive default used by synchronous GRAPH_RECALL.
 */
async function recallAsync(conn, options = {}) {
    if (Array.isArray(options.seeds) && options.seeds.length > MAX_RECALL_SEEDS) {
        throw new RangeError(
            `GRAPH_RECALL accepts at most ${MAX_RECALL_SEEDS} seeds, got ${options.seeds.length}`
        );
    }
    return jobs.submit(conn, buildRecall(options));
}

/** Retrieve and decode a detached recall by job id, or null while it runs. */
async function fetchRecall(conn, jobId) {
    const response = await jobs.fetch(conn, jobId);
    return response === null ? null : recallResult(response, jobId);
}

/** Poll a detached recall by id and decode its terminal result. */
async function awaitRecall(conn, jobId, options = {}) {
    const response = await jobs.awaitJob(conn, jobId, options);
    return recallResult(response, jobId);
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
 * saturated the noisy-OR. Hydrated `references` are kept for the same reason:
 * they are the evidence a caller shows, and re-fetching them costs a round trip.
 */
async function recallBatched(conn, { seeds, ...options }) {
    const unique = [...new Set(seeds)];
    const merged = new Map();
    for (let at = 0; at < unique.length; at += MAX_RECALL_SEEDS) {
        const batch = unique.slice(at, at + MAX_RECALL_SEEDS);
        const { associations } = await recall(conn, { seeds: batch, ...options });
        for (const association of associations) {
            const current = merged.get(association.id) ||
                { id: association.id, score: 0, sourceCount: 0, sources: new Map(), references: [] };
            current.score = 1 - (1 - current.score) * (1 - Number(association.score || 0));
            current.sourceCount += Number(association.source_count || 0);
            for (const source of association.sources || []) {
                const activation = Number(source.activation || 0);
                current.sources.set(source.seed, Math.max(current.sources.get(source.seed) || 0, activation));
            }
            for (const reference of association.references || []) {
                if (!current.references.some((kept) => kept.id === reference.id && kept.text === reference.text)) {
                    current.references.push(reference);
                }
            }
            merged.set(association.id, current);
        }
    }
    return [...merged.values()].sort((left, right) =>
        right.score - left.score || right.sourceCount - left.sourceCount
    );
}

/** Nodes that resemble `id` by shared context, by shared words, or by both. */
async function similar(conn, { id, by = null, limit = null }) {
    const response = await commandOrThrow(conn, buildSimilar({ id, by, limit }), `GRAPH_SIMILAR ${id}`);
    return {
        matches: decodePayload(response.fields) || [],
        count: numericField(response.fields, 'count', 0),
        truncated: numericField(response.fields, 'truncated', 0) > 0,
    };
}

/**
 * Inspect, rebuild or drop the derived lexical term index. It is derived, never
 * authoritative: recall degrades to exact-id seeds when it is missing, so a
 * rebuild is a repair, not a migration.
 */
async function termIndex(conn, options = {}) {
    const response = await commandOrThrow(conn, buildTermIndex(options), 'GRAPH_TERM_INDEX');
    return {
        fields: response.fields,
        nextCursor: parseCursor(response.fields),
        weighted: numericField(response.fields, 'weighted', 0) > 0,
        tokens: numericField(response.fields, 'tokens', 0),
        trigrams: numericField(response.fields, 'trigrams', 0),
    };
}

// ---------------------------------------------------------------------------
// Ambiguity — several readings that exclude each other
// ---------------------------------------------------------------------------

/**
 * `GRAPH_AMBIGUITY_SET` — write a whole set of mutually exclusive readings at
 * once and normalize their shares to sum to 1.
 *
 * The engine has no `OR`: a disjunction is *stored* as a group rather than
 * expressed as a query. `options` is either a list of ids (equal shares) or a
 * map of id → share.
 */
function buildAmbiguitySet({ from, group, options, type = null, normalize = true }) {
    const rendered = Array.isArray(options)
        ? options.map((option) => String(option).trim()).filter(Boolean).join(',')
        : Object.entries(options || {})
            .map(([id, share]) => `${String(id).trim()}=${share}`)
            .join(',');
    if (!rendered) throw new CheetahError('cheetah GRAPH_AMBIGUITY_SET requires options');
    return buildKeyValueCommand('GRAPH_AMBIGUITY_SET', {
        from,
        group,
        options: rendered,
        type,
        normalize: flag(normalize),
    });
}

function buildAmbiguityGet({ from, group, direction = null, limit = null }) {
    return buildKeyValueCommand('GRAPH_AMBIGUITY_GET', { from, group, direction, limit });
}

/**
 * `GRAPH_AMBIGUITY_RESOLVE` — collapse the set: the winner becomes `certain`,
 * the others `ruled_out` (or are deleted with `drop`), and the group dissolves.
 */
function buildAmbiguityResolve({ from, group, winner, drop = false }) {
    return buildKeyValueCommand('GRAPH_AMBIGUITY_RESOLVE', { from, group, winner, drop: flag(drop) });
}

async function ambiguitySet(conn, options) {
    return commandOrThrow(conn, buildAmbiguitySet(options), 'GRAPH_AMBIGUITY_SET');
}

/** One alternative group read back, strongest reading first. */
async function ambiguityGet(conn, options) {
    const line = buildAmbiguityGet(options);
    const response = await conn.send(line);
    if (!response.ok) {
        if (isNotFound(response)) {
            return { count: 0, alternatives: [], top: null, confidenceSum: 0 };
        }
        throw new CheetahError(`cheetah GRAPH_AMBIGUITY_GET failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return {
        count: numericField(response.fields, 'count', 0),
        confidenceSum: numericField(response.fields, 'confidence_sum', 0),
        top: response.fields.top || null,
        topModality: response.fields.top_modality || null,
        alternatives: decodePayload(response.fields) || [],
        response,
    };
}

async function ambiguityResolve(conn, options) {
    return commandOrThrow(conn, buildAmbiguityResolve(options), 'GRAPH_AMBIGUITY_RESOLVE');
}

module.exports = {
    MAX_NODE_REFERENCES,
    MAX_RECALL_SEEDS,
    ambiguityGet,
    ambiguityResolve,
    ambiguitySet,
    awaitRecall,
    buildAmbiguityGet,
    buildAmbiguityResolve,
    buildAmbiguitySet,
    buildDegree,
    buildEdgeDel,
    buildEdgeGet,
    buildEdgeSet,
    buildEdgeSetBatch,
    buildNeighborTypes,
    buildNeighbors,
    buildNodeDel,
    buildNodeGet,
    buildNodeSet,
    buildQuery,
    buildRecall,
    buildSimilar,
    buildTermIndex,
    degree,
    deleteEdge,
    deleteNode,
    encodeJsonArgument,
    fetchRecall,
    getEdge,
    getNode,
    neighborTypes,
    neighbors,
    neighborsAll,
    normalizeReferences,
    query,
    recall,
    recallAsync,
    recallBatched,
    recallResult,
    setEdge,
    setEdgeBatch,
    setNode,
    similar,
    termIndex,
};

// Server, database and cluster operations — the commands *about* the server.
//
// Everything here answers a question about the process or the registry of
// databases rather than about the data in one of them: how loaded it is, what
// it has been logging, when it last flushed, which databases exist and with
// what settings, and which node owns which fork. They are cheap enough to poll
// between phases of a long ingest, which is what most of them exist for.
//
// Two scopes are deliberately kept apart here, because the server keeps them
// apart:
//
//   - `DB_CREATE`/`DB_LIST` address the **engine** — the registry — and change
//     nothing about the connection that issues them.
//   - `DATABASE`/`RESET_DB` are **connection-scoped**: they change what *this*
//     socket is pointing at, so on a pool they must be broadcast or the pool
//     must be constructed with the database it should open.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, decodePayload, numericField } = require('./protocol');

/**
 * The per-database settings `DB_CREATE`/`DATABASE`/`RESET_DB` accept. They
 * override the server's own `[database]` section for that database alone and
 * are persisted next to its data (`<db>/settings.ini`), so they survive a
 * restart. The trie-geometry ones only bite when the directory is *created*:
 * `pairs/format.dat` wins on every ordinary open, which is why adopting a new
 * stride means `resetDatabase`.
 */
const DATABASE_SETTINGS = Object.freeze([
    'pair_bytes',
    'pair_index_bytes',
    'adaptive_pair_index',
    'pair_list_max_bytes',
    'pair_list_max_fill_percent',
    'payload_cache_entries',
    'payload_cache_mb',
    'payload_cache_bytes',
]);

function settingTokens(settings) {
    const tokens = [];
    for (const [key, value] of Object.entries(settings || {})) {
        if (value === undefined || value === null) continue;
        if (!DATABASE_SETTINGS.includes(key)) {
            throw new CheetahError(
                `cheetah database setting not understood: ${key}; ` +
                `expected one of ${DATABASE_SETTINGS.join(', ')}`
            );
        }
        tokens.push(`${key}=${typeof value === 'boolean' ? (value ? 1 : 0) : value}`);
    }
    return tokens;
}

async function sendOrThrow(conn, line, what) {
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah ${what} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return response;
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

function buildCreateDatabase(name, settings) {
    return ['DB_CREATE', String(name), ...settingTokens(settings)].join(' ');
}

/**
 * `DB_CREATE` — a **new** database, optionally with settings of its own.
 *
 * Unlike `useDatabase`, which opens-or-creates, this refuses a name that
 * already exists: a creation that silently adopted a populated directory would
 * also silently ignore the settings passed, since trie geometry is decided when
 * the directory is made. It does not point the connection at the new database.
 *
 * Returns the settings the database was actually created with.
 */
async function createDatabase(conn, name, settings) {
    const response = await sendOrThrow(conn, buildCreateDatabase(name, settings), `DB_CREATE ${name}`);
    const created = { ...response.fields };
    delete created.database_created;
    return { name: response.fields.database_created || String(name), settings: created, response };
}

/**
 * `DB_LIST` — every database under `data_dir` and how it would open. Reads the
 * disk rather than the registry, so a database never opened in this process is
 * still listed.
 */
async function listDatabases(conn) {
    const response = await sendOrThrow(conn, 'DB_LIST', 'DB_LIST');
    const payload = decodePayload(response.fields) || [];
    return payload.map((entry) => ({
        name: entry.name,
        path: entry.path,
        loaded: Boolean(entry.loaded),
        adHoc: Boolean(entry.ad_hoc_settings),
        settings: entry.settings || {},
    }));
}

/**
 * `DATABASE` — point **this connection** at a database, creating it if new.
 * Settings given here are recorded and persisted for that name exactly as with
 * `createDatabase`.
 */
async function useDatabase(conn, name, settings) {
    const line = ['DATABASE', String(name), ...settingTokens(settings)].join(' ');
    return sendOrThrow(conn, line, `DATABASE ${name}`);
}

/**
 * `RESET_DB` — delete the directory and reopen it empty. The only way to adopt
 * a new trie geometry, since `pairs/format.dat` is authoritative on every
 * ordinary open. Destructive and not confirmable.
 */
async function resetDatabase(conn, name = null, settings = null) {
    const tokens = settingTokens(settings);
    if (tokens.length > 0 && !name) {
        throw new CheetahError('cheetah RESET_DB needs an explicit database name to carry settings');
    }
    const line = ['RESET_DB', ...(name ? [String(name)] : []), ...tokens].join(' ');
    return sendOrThrow(conn, line, 'RESET_DB');
}

// ---------------------------------------------------------------------------
// Server gauges and maintenance
// ---------------------------------------------------------------------------

/**
 * `SYSTEM_STATS` — live gauges, a cheap heartbeat between ingest and reduce
 * loops. Only the fields every build reports are named; `fields` keeps the whole
 * line so a newer server's additions are not lost on the way through. A metric
 * the platform cannot measure reads `NA`, which parses to null rather than 0.
 */
async function systemStats(conn) {
    const response = await sendOrThrow(conn, 'SYSTEM_STATS', 'SYSTEM_STATS');
    const hits = numericField(response.fields, 'payload_cache_hits', null);
    const misses = numericField(response.fields, 'payload_cache_misses', null);
    return {
        logicalCores: numericField(response.fields, 'logical_cores', null),
        gomaxprocs: numericField(response.fields, 'gomaxprocs', null),
        goroutines: numericField(response.fields, 'goroutines', null),
        processCpuPct: numericField(response.fields, 'process_cpu_pct', null),
        systemCpuPct: numericField(response.fields, 'system_cpu_pct', null),
        payloadCacheEntries: numericField(response.fields, 'payload_cache_entries', null),
        payloadCacheBytes: numericField(response.fields, 'payload_cache_bytes', null),
        payloadCacheHits: hits,
        payloadCacheMisses: misses,
        cacheHitRatio: hits === null || misses === null || hits + misses === 0 ? null : hits / (hits + misses),
        fields: { ...response.fields },
        response,
    };
}

/**
 * `LOG_FLUSH` — dump **and clear** the in-memory log ring. Clearing is the point
 * and the trap: two readers of the same ring each see half the history, so keep
 * one flusher. The entries travel as a base64 JSON array because the protocol
 * is one line per command.
 */
async function logFlush(conn, limit = 0) {
    const line = limit > 0 ? `LOG_FLUSH ${limit}` : 'LOG_FLUSH';
    const response = await sendOrThrow(conn, line, 'LOG_FLUSH');
    const payload = decodePayload(response.fields);
    return Array.isArray(payload) ? payload.map(String) : [];
}

/**
 * `FILE_CHECKPOINT` — force the managed-file layer to act now: flush,
 * optionally drop the sector cache and close idle handles. The manual form of
 * what shutdown does. Its own small dialect (bare uppercase flags) is spelled
 * here so callers do not have to remember it.
 */
async function fileCheckpoint(conn, { idle = null, dropCache = false, closeHandles = false } = {}) {
    const parts = ['FILE_CHECKPOINT'];
    if (idle) parts.push(`IDLE=${idle}`);
    if (dropCache) parts.push('DROP_CACHE');
    if (closeHandles) parts.push('CLOSE_HANDLES');
    const response = await sendOrThrow(conn, parts.join(' '), 'FILE_CHECKPOINT');
    for (const flag of response.flags) {
        if (flag.startsWith('file_checkpoint_flushed=')) {
            const parsed = Number.parseInt(flag.slice('file_checkpoint_flushed='.length), 10);
            return Number.isFinite(parsed) ? parsed : 0;
        }
    }
    return numericField(response.fields, 'file_checkpoint_flushed', 0);
}

// ---------------------------------------------------------------------------
// Cluster placement
// ---------------------------------------------------------------------------

/**
 * `CLUSTER_UPDATE` — register the topology: who exists, where, and how many
 * replicas a fork wants. `nodes` maps a node id to `host:port/weight`. The
 * topology is persisted; the placement *overrides* made with `clusterMove` are
 * not.
 */
async function clusterUpdate(conn, { replication, nodes = {} } = {}) {
    const parts = ['CLUSTER_UPDATE', `replication=${Number(replication)}`];
    for (const [id, address] of Object.entries(nodes || {})) parts.push(`${id}=${address}`);
    return sendOrThrow(conn, parts.join(' '), 'CLUSTER_UPDATE');
}

async function clusterStatus(conn) {
    const response = await sendOrThrow(conn, 'CLUSTER_STATUS', 'CLUSTER_STATUS');
    return { fields: { ...response.fields }, payload: decodePayload(response.fields), response };
}

/** Which fork a prefix hashes to, and which nodes own it. */
async function forkAssign(conn, prefix = null) {
    const target = prefix === null || prefix === '' ? '*' : prefix;
    const response = await sendOrThrow(conn, `FORK_ASSIGN ${target}`, 'FORK_ASSIGN');
    return {
        forkId: response.fields.fork_id || null,
        nodes: (response.fields.nodes || '').split('|').filter(Boolean),
        response,
    };
}

/** Force a fork onto a node and gossip the transfer. The override is in memory only. */
async function clusterMove(conn, { node, prefix = null, fork = null } = {}) {
    if (!prefix && !fork) throw new CheetahError('cheetah CLUSTER_MOVE requires a prefix or a fork id');
    const line = buildKeyValueCommand('CLUSTER_MOVE', fork ? { node, fork } : { node, prefix });
    return sendOrThrow(conn, line, 'CLUSTER_MOVE');
}

module.exports = {
    DATABASE_SETTINGS,
    buildCreateDatabase,
    clusterMove,
    clusterStatus,
    clusterUpdate,
    createDatabase,
    fileCheckpoint,
    forkAssign,
    listDatabases,
    logFlush,
    resetDatabase,
    systemStats,
    useDatabase,
};

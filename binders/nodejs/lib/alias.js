// `ALIAS` — the part of the protocol that describes the protocol.
//
// Two things a client cannot derive on its own:
//
//   - the **command index**, the 2-byte number the binary protocol puts on the
//     wire in place of a command name. It is built from the server's own
//     command inventory, so it changes when a command or an alias is added or
//     removed; hard-coding it here would mean shipping a client that calls the
//     wrong command after a server upgrade.
//   - a table's **numeric widths**. They are a property of the database, not of
//     the client: two processes writing the same table must encode it the same
//     way, which only works if the widths live on the server.
//
// So both are fetched, and both come with a digest to check a cached copy
// against. `aliasDigest` is the cheap call — sixteen characters — and the
// binary handshake already returns the same digest in its ack, so in the normal
// case verifying costs nothing at all.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, decodePayload, numericField } = require('./protocol');

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

/** `ALIAS digest` — index identity only: epoch, digest, counts. */
async function aliasDigest(conn) {
    const response = await sendOrThrow(conn, 'ALIAS digest', 'ALIAS digest');
    return {
        version: numericField(response.fields, 'version', 1),
        epoch: numericField(response.fields, 'epoch', 0),
        digest: response.fields.digest || null,
        commands: numericField(response.fields, 'commands', 0),
        keysDigest: response.fields.keys_digest || null,
        keys: numericField(response.fields, 'keys', 0),
        response,
    };
}

/**
 * `ALIAS list` — the whole command index as `[{id, name, kind}]`.
 *
 * `kind` says which of the server's routing tables a name comes from (`micro`,
 * `alias`, `builtin`, `engine`, `frontend`); it is descriptive, and every kind
 * is callable the same way.
 */
async function listCommands(conn, { from = null, limit = null, kind = null } = {}) {
    const line = buildKeyValueCommand('ALIAS list', { from, limit, kind });
    const response = await sendOrThrow(conn, line, 'ALIAS list');
    return {
        entries: decodePayload(response.fields) || [],
        digest: response.fields.digest || null,
        epoch: numericField(response.fields, 'epoch', 0),
        total: numericField(response.fields, 'total', 0),
        response,
    };
}

/** `ALIAS keys` — the argument-key dictionary, same shape as the command index. */
async function listArgumentKeys(conn, { from = null, limit = null } = {}) {
    const line = buildKeyValueCommand('ALIAS keys', { from, limit });
    const response = await sendOrThrow(conn, line, 'ALIAS keys');
    return {
        entries: decodePayload(response.fields) || [],
        digest: response.fields.digest || null,
        total: numericField(response.fields, 'total', 0),
        response,
    };
}

/** `ALIAS get` — resolve one command, by name or by index. */
async function resolveCommand(conn, { name = null, id = null } = {}) {
    if (name === null && id === null) throw new CheetahError('cheetah ALIAS get requires a name or an id');
    const line = buildKeyValueCommand('ALIAS get', name !== null ? { name } : { id });
    const response = await sendOrThrow(conn, line, 'ALIAS get');
    return {
        id: numericField(response.fields, 'id', null),
        name: response.fields.name || null,
        kind: response.fields.kind || null,
        response,
    };
}

/** `ALIAS types` — the value-type codec and the server's default widths. */
async function describeTypes(conn) {
    const response = await sendOrThrow(conn, 'ALIAS types', 'ALIAS types');
    return { payload: decodePayload(response.fields), response };
}

/**
 * `ALIAS profile` — read or set a table's numeric widths.
 *
 * Reading answers with the **resolved** widths (what the server will actually
 * use, defaults included) alongside the declared ones, because that is the only
 * answer that tells a client how what it writes will be read. Passing any of
 * `uint`/`int`/`float` writes; `reset: true` removes the declaration.
 */
async function tableProfile(conn, table, { uint = null, int = null, float = null, reset = false } = {}) {
    const fields = { table };
    if (reset) fields.reset = 1;
    else {
        if (uint !== null) fields.uint = uint;
        if (int !== null) fields.int = int;
        if (float !== null) fields.float = float;
    }
    const line = buildKeyValueCommand('ALIAS profile', fields);
    const response = await sendOrThrow(conn, line, 'ALIAS profile');
    return {
        table: response.fields.table || table,
        uint: numericField(response.fields, 'uint', 8),
        int: numericField(response.fields, 'int', 8),
        float: numericField(response.fields, 'float', 8),
        declared: response.fields.declared === '1',
        declaredWidths: {
            uint: numericField(response.fields, 'declared_uint', 0),
            int: numericField(response.fields, 'declared_int', 0),
            float: numericField(response.fields, 'declared_float', 0),
        },
        updated: response.fields.updated === '1',
        response,
    };
}

/** `ALIAS profile` with no table — every declared profile. */
async function listProfiles(conn) {
    const response = await sendOrThrow(conn, 'ALIAS profile', 'ALIAS profile');
    return { entries: decodePayload(response.fields) || [], response };
}

/**
 * Fill a `BinarySession` from the server: the command index, the argument keys,
 * and the digests to verify them later. `session.matchesDigest(digest)` is then
 * enough to know whether a cached index is still good.
 */
async function loadSession(conn, session) {
    const [commands, keys] = await Promise.all([listCommands(conn), listArgumentKeys(conn)]);
    session.loadCommands(commands.entries, commands.digest);
    session.loadKeys(keys.entries, keys.digest);
    session.epoch = commands.epoch;
    return session;
}

module.exports = {
    aliasDigest,
    describeTypes,
    listArgumentKeys,
    listCommands,
    listProfiles,
    loadSession,
    resolveCommand,
    tableProfile,
};

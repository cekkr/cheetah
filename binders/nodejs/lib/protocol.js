// Cheetah wire-protocol codec.
//
// Pure functions only: nothing here opens a socket, reads a setting, or keeps
// state. Everything Cheetah says is one line; everything we say is one line.
// The authority for the formats reproduced here is this repository's own
// handbook (AGENTS.md) and the `ExecuteCommand` switch in src/database.go —
// never document Cheetah behavior from memory.
//
// Response grammar, as emitted by the server:
//
//   SUCCESS[,<field>]*            e.g. SUCCESS,pair_set
//                                      SUCCESS,key=42
//                                      SUCCESS,count=2,items=<hex>:<key>;<hex>:<key>
//                                      SUCCESS,reducer=counts,count=1,items=<hex>:<key>:<b64>
//                                      SUCCESS,command=GRAPH_RECALL,count=4,payload=<b64>
//   ERROR,<reason>                e.g. ERROR,not_found
//                                      ERROR,value_size_mismatch (expected 16, got 17)
//   PENDING,<field>*              a job that has not finished yet
//
// A field is either `key=value` or a bare flag (`pair_set`). Fields are comma
// separated, EXCEPT that `value=` (the READ payload) is raw bytes and runs to
// the end of the line — it may legitimately contain commas.

/** `\x01` … `\x08` — graph, record, cache, and record-index namespaces. */
const RESERVED_CONTROL_PREFIXES = Object.freeze([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

/** Reserved textual namespaces (graph.go → `graph/idx/`). */
const RESERVED_TEXT_PREFIXES = Object.freeze(['graph/', 'idx/']);

/**
 * True when `input` can travel as a bare positional argument. Cheetah splits
 * command arguments on whitespace and decodes a leading `x` as hex
 * (src/helpers.go → parseValue), so anything with a space, a non-printable
 * byte, a leading `x`, or the wildcard spelling must be escaped.
 */
function isBareSafeArgument(bytes) {
    if (bytes.length === 0) return false;
    if (bytes[0] === 0x78 /* 'x' */) return false;
    if (bytes.length === 1 && bytes[0] === 0x2a /* '*' */) return false;
    for (const byte of bytes) {
        if (byte < 0x21 || byte > 0x7e) return false;
    }
    return true;
}

/**
 * Encode a byte string as a Cheetah positional argument: raw when it is
 * unambiguous, `x<HEX>` otherwise. Always round-trips.
 */
function encodeArgument(value) {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'latin1');
    if (isBareSafeArgument(bytes)) return bytes.toString('latin1');
    return `x${bytes.toString('hex')}`;
}

/**
 * Throws when a key would collide with a namespace the server reserves for
 * itself (`\x01gn:` … `\x05gt:`, `graph/`, `idx/`). A client's own prefixes
 * normally hold this by construction; this is the assertion that keeps it true.
 */
function assertUnreservedKey(key) {
    const bytes = Buffer.isBuffer(key) ? key : Buffer.from(String(key), 'latin1');
    if (bytes.length === 0) throw new Error('cheetah key must not be empty');
    if (RESERVED_CONTROL_PREFIXES.includes(bytes[0])) {
        throw new Error(`cheetah key uses a reserved control-byte prefix: 0x${bytes[0].toString(16)}`);
    }
    const text = bytes.toString('latin1');
    for (const prefix of RESERVED_TEXT_PREFIXES) {
        if (text.startsWith(prefix)) {
            throw new Error(`cheetah key uses a reserved namespace: ${text.slice(0, 8)}`);
        }
    }
    return text;
}

/**
 * An argument that is already in Cheetah's own spelling and must travel
 * verbatim. The only real case is a `next_cursor` token: it comes back as
 * `x<hex>`, and running it through `encodeArgument` would hex-encode the hex —
 * the server would then resume from a prefix that does not exist and silently
 * return one page instead of the whole sweep.
 */
class RawArgument {
    constructor(text) {
        this.text = String(text);
    }

    toString() {
        return this.text;
    }
}

function rawArgument(text) {
    return new RawArgument(text);
}

/** Build one command line. Arguments are encoded, the line is not terminated. */
function buildCommand(command, ...args) {
    const parts = [String(command)];
    for (const arg of args) {
        if (arg === undefined || arg === null || arg === '') continue;
        if (arg instanceof RawArgument) parts.push(arg.text);
        else parts.push(typeof arg === 'number' ? String(arg) : encodeArgument(arg));
    }
    const line = parts.join(' ');
    if (line.includes('\n')) throw new Error('cheetah command must not contain a newline');
    return line;
}

/** `key=value` tokens, the dialect the GRAPH_ and PREDICT_ families speak. */
function buildKeyValueCommand(command, fields) {
    const parts = [String(command)];
    for (const [key, value] of Object.entries(fields || {})) {
        if (value === undefined || value === null || value === '') continue;
        parts.push(`${key}=${value}`);
    }
    const line = parts.join(' ');
    if (line.includes('\n')) throw new Error('cheetah command must not contain a newline');
    return line;
}

/**
 * Split a response line into `{status, fields, flags}`.
 *
 * `fields` is a plain object of the `key=value` tokens; `flags` collects bare
 * tokens such as the `pair_set` in `SUCCESS,pair_set`.
 */
function parseResponse(line) {
    const raw = typeof line === 'string' ? line.replace(/\r?\n$/, '') : '';
    const firstComma = raw.indexOf(',');
    const status = (firstComma === -1 ? raw : raw.slice(0, firstComma)).trim();
    const rest = firstComma === -1 ? '' : raw.slice(firstComma + 1);

    const result = {
        raw,
        status,
        ok: status === 'SUCCESS',
        pending: status === 'PENDING',
        error: null,
        fields: Object.create(null),
        flags: [],
    };

    if (status === 'ERROR') {
        // The reason runs to end of line: it may contain commas and spaces.
        result.error = rest;
        return result;
    }

    let cursor = 0;
    while (cursor <= rest.length && rest.length > 0) {
        const nextComma = rest.indexOf(',', cursor);
        const token = nextComma === -1 ? rest.slice(cursor) : rest.slice(cursor, nextComma);
        const equals = token.indexOf('=');
        if (equals === -1) {
            if (token !== '') result.flags.push(token);
        } else {
            const key = token.slice(0, equals);
            if (key === 'value') {
                // READ answers `SUCCESS,size=<n>,value=<raw bytes>`; the bytes
                // are unescaped and always last, so they own the rest of line.
                result.fields.value = rest.slice(cursor + equals + 1);
                break;
            }
            result.fields[key] = token.slice(equals + 1);
        }
        if (nextComma === -1) break;
        cursor = nextComma + 1;
    }

    return result;
}

/** Decode `<hex>` → Buffer. Tolerates the `x<hex>` argument spelling. */
function decodeHexKey(hex) {
    if (typeof hex !== 'string' || hex === '') return Buffer.alloc(0);
    const body = hex.startsWith('x') ? hex.slice(1) : hex;
    return Buffer.from(body, 'hex');
}

/**
 * Split an `items=` field.
 *
 * PAIR_SCAN emits `<hexKey>:<absKey>`; PAIR_REDUCE adds a third component, the
 * hydrated payload in base64. Returns `{keyHex, key, bytes, absKey, payloadBase64}`.
 */
function parseItems(itemsField) {
    if (!itemsField) return [];
    return itemsField.split(';').filter(Boolean).map((entry) => {
        const parts = entry.split(':');
        const keyHex = parts[0];
        const bytes = decodeHexKey(keyHex);
        const absKeyRaw = parts.length > 1 ? parts[1] : '';
        const absKeyNumeric = Number.parseInt(absKeyRaw, 10);
        return {
            keyHex,
            bytes,
            key: bytes.toString('latin1'),
            absKey: Number.isFinite(absKeyNumeric) ? absKeyNumeric : null,
            absKeyRaw,
            payloadBase64: parts.length > 2 ? parts.slice(2).join(':') : null,
        };
    });
}

/**
 * `next_cursor` as an opaque token to hand back verbatim, or `null` when the
 * sweep is exhausted. Cheetah spells exhaustion either by omitting the field or
 * by answering `*`.
 */
function parseCursor(fields) {
    const cursor = fields && fields.next_cursor;
    if (!cursor || cursor === '*') return null;
    return cursor;
}

/** Decode a `payload=<base64>` field into JSON. Returns null when absent. */
function decodePayload(fields) {
    const payload = fields && fields.payload;
    if (!payload) return null;
    const text = Buffer.from(payload, 'base64').toString('utf8');
    try {
        return JSON.parse(text);
    } catch (error) {
        throw new Error(`cheetah payload is not JSON: ${error.message}`);
    }
}

/** Decode one `items=` payload component (base64) into raw bytes. */
function decodeItemPayload(item) {
    if (!item || !item.payloadBase64) return null;
    return Buffer.from(item.payloadBase64, 'base64');
}

/**
 * `branches=<hexByte>:<count>;…` from PAIR_SUMMARY — the fan-out histogram that
 * says whether a prefix is worth scanning.
 */
function parseBranches(branchesField) {
    if (!branchesField) return [];
    return branchesField.split(';').filter(Boolean).map((entry) => {
        const [byteHex, count] = entry.split(':');
        return { byteHex, byte: Number.parseInt(byteHex, 16), count: Number.parseInt(count, 10) };
    });
}

/** Numeric field accessor that keeps `0` and rejects garbage. */
function numericField(fields, name, fallback = null) {
    const raw = fields ? fields[name] : undefined;
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
}

module.exports = {
    RESERVED_CONTROL_PREFIXES,
    RESERVED_TEXT_PREFIXES,
    RawArgument,
    assertUnreservedKey,
    // Historical spelling kept because callers embed it in their own key
    // assertions; it is the same check.
    assertKeySpaceIsOurs: assertUnreservedKey,
    buildCommand,
    buildKeyValueCommand,
    decodeHexKey,
    decodeItemPayload,
    decodePayload,
    encodeArgument,
    isBareSafeArgument,
    numericField,
    parseBranches,
    parseCursor,
    parseItems,
    parseResponse,
    rawArgument,
};

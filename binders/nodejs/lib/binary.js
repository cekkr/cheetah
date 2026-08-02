// Cheetah binary-protocol codec.
//
// The binary protocol carries the *same* commands as the text one: the command
// name becomes a 2-byte index and every value travels in its own type — an
// integer as an integer, a float as a float, bytes as bytes — instead of all of
// them as text. The authority is `src/binary_protocol.go`; nothing here may be
// documented from memory.
//
// The single most important property, and the reason this file is small: the
// server decodes a request frame into the **canonical command line** and
// re-encodes the answer line into a response frame. So a binary client is a
// transcoder, not a second command surface — every command layer in this binder
// (kv, graph, records, predict, admin) keeps building text lines, and
// `encodeCommandLine` turns them into frames. Add a command to the server and
// nothing here needs editing.
//
// The corollary is that the canonical line stays the contract. A `key=value`
// value may not contain whitespace in binary mode either; what binary adds is
// that the *typed* escape hatch is free — a key travels as `bytes` and is
// rendered back as `x<hex>` by the server, which is what a text client would
// have had to spell out by hand.
//
// Frame layout:
//
//   0xC7            magic. No text command starts with this byte, which is how
//                   the server tells the two modes apart from byte one.
//   u8              frame type
//   u32be           body length
//   body
//
// Two things a client cannot know on its own: the command index, which changes
// with the server's command inventory, and a table's numeric widths, which are
// a property of the database. Both are published by the `ALIAS` command
// (alias.js) — and the index and the argument-key dictionary additionally
// arrive whole in the handshake ack, because a response names its fields by
// index and a client without the dictionary could not read even the answer to
// `ALIAS keys`. Digests come with them, so a cached copy is verified in sixteen
// characters rather than refetched.

const FRAME_MAGIC = 0xc7;
const FRAME_HEADER_BYTES = 6;
const PROTOCOL_VERSION = 1;
const MAX_BODY_BYTES = 16 << 20;

/** Frame types. */
const FRAME = Object.freeze({
    HANDSHAKE: 0x01,
    HANDSHAKE_ACK: 0x02,
    REQUEST: 0x03,
    RESPONSE: 0x04,
});

/** Value types. The tag byte is `kind << 4 | width`, width 0 = "the default". */
const KIND = Object.freeze({
    STRING: 0x0,
    BYTES: 0x1,
    UINT: 0x2,
    INT: 0x3,
    FLOAT: 0x4,
    BOOL: 0x5,
    ENUM: 0x6,
    NULL: 0x7,
});

/** How an argument names itself. */
const KEY_MODE = Object.freeze({ POSITIONAL: 0x00, INDEXED: 0x01, INLINE: 0x02 });

/** Enumeration families a value of type ENUM can index into. */
const ENUM_FAMILY = Object.freeze({ COMMANDS: 0x01, ARGUMENT_KEYS: 0x02 });

/** Response status codes. */
const STATUS = Object.freeze({ OTHER: 0x00, SUCCESS: 0x01, ERROR: 0x02, PENDING: 0x03 });

const STATUS_WORDS = Object.freeze({
    [STATUS.SUCCESS]: 'SUCCESS',
    [STATUS.ERROR]: 'ERROR',
    [STATUS.PENDING]: 'PENDING',
    [STATUS.OTHER]: '',
});

/** Server-side defaults, mirrored so a session works before any negotiation. */
const DEFAULT_WIDTHS = Object.freeze({ uint: 8, int: 8, float: 8 });

class CheetahBinaryError extends Error {
    constructor(message) {
        super(message);
        this.name = 'CheetahBinaryError';
    }
}

/**
 * Everything a connection negotiated or looked up: the numeric widths, the
 * command index, the argument-key dictionary, and the digests that say whether
 * a cached copy of either is still valid.
 *
 * A session works with an empty index — commands then travel by name, which
 * costs bytes but never correctness. `loadCommands`/`loadKeys` fill it from an
 * `ALIAS list` / `ALIAS keys` payload.
 */
class BinarySession {
    constructor({ widths = {}, version = PROTOCOL_VERSION } = {}) {
        this.version = version;
        this.widths = { ...DEFAULT_WIDTHS, ...widths };
        this.commandIds = new Map();
        this.commandNames = new Map();
        this.keyIds = new Map();
        this.keyNames = new Map();
        this.digest = null;
        this.keysDigest = null;
        this.epoch = 0;
        // Per-table numeric widths, as reported by `ALIAS profile table=…`.
        // Purely informational on this side: the server resolves the widths it
        // will use, and an explicit width in a tag always wins.
        this.tableProfiles = new Map();
    }

    loadCommands(entries, digest = null) {
        this.commandIds = new Map();
        this.commandNames = new Map();
        for (const entry of entries || []) {
            const name = String(entry.name).toUpperCase();
            this.commandIds.set(name, entry.id);
            this.commandNames.set(entry.id, name);
        }
        if (digest !== null) this.digest = digest;
        return this;
    }

    loadKeys(entries, digest = null) {
        this.keyIds = new Map();
        this.keyNames = new Map();
        for (const entry of entries || []) {
            const name = String(entry.name).toLowerCase();
            this.keyIds.set(name, entry.id);
            this.keyNames.set(entry.id, name);
        }
        if (digest !== null) this.keysDigest = digest;
        return this;
    }

    loadProfile(table, profile) {
        this.tableProfiles.set(String(table), { ...DEFAULT_WIDTHS, ...profile });
        return this;
    }

    /** Widths for a table: its profile when known, the session defaults otherwise. */
    widthsFor(table) {
        if (table && this.tableProfiles.has(String(table))) return this.tableProfiles.get(String(table));
        return this.widths;
    }

    commandId(name) {
        return this.commandIds.get(String(name).toUpperCase());
    }

    commandName(id) {
        return this.commandNames.get(id);
    }

    keyId(name) {
        return this.keyIds.get(String(name).toLowerCase());
    }

    keyName(id) {
        return this.keyNames.get(id);
    }

    /**
     * True when a cached index still matches the server's. Pass the `digest`
     * from the handshake ack or from `ALIAS digest`.
     */
    matchesDigest(digest) {
        return this.digest !== null && this.digest === digest;
    }
}

// --- frames ------------------------------------------------------------------

function encodeFrame(type, body) {
    const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || []);
    const frame = Buffer.allocUnsafe(FRAME_HEADER_BYTES + payload.length);
    frame[0] = FRAME_MAGIC;
    frame[1] = type;
    frame.writeUInt32BE(payload.length, 2);
    payload.copy(frame, FRAME_HEADER_BYTES);
    return frame;
}

/**
 * Pull one frame off the front of a buffer.
 *
 * Returns `null` when the buffer does not hold a whole frame yet — a stream
 * splitter calls this in a loop and keeps the remainder.
 */
function readFrame(buffer) {
    if (buffer.length < FRAME_HEADER_BYTES) return null;
    if (buffer[0] !== FRAME_MAGIC) {
        throw new CheetahBinaryError(`cheetah binary frame has a bad magic byte: 0x${buffer[0].toString(16)}`);
    }
    const length = buffer.readUInt32BE(2);
    if (length > MAX_BODY_BYTES) {
        throw new CheetahBinaryError(`cheetah binary frame is too large: ${length}`);
    }
    const total = FRAME_HEADER_BYTES + length;
    if (buffer.length < total) return null;
    return {
        frame: { type: buffer[1], body: buffer.subarray(FRAME_HEADER_BYTES, total) },
        rest: buffer.subarray(total),
    };
}

// --- handshake ---------------------------------------------------------------

/**
 * The first frame of a binary connection. A width of `0` means "whatever the
 * server defaults to", which is how a client states a preference for one type
 * without having to state one for all three.
 */
function encodeHandshake({ version = PROTOCOL_VERSION, uint = 0, int = 0, float = 0 } = {}) {
    return encodeFrame(FRAME.HANDSHAKE, Buffer.from([version, uint, int, float, 0]));
}

/** Command-kind codes, as the ack spells them. Descriptive only. */
const COMMAND_KINDS = Object.freeze({ 1: 'micro', 2: 'alias', 3: 'builtin', 4: 'engine', 5: 'frontend' });

/**
 * Decode the server's answer: the effective widths, the index identity, and
 * **both tables in full**.
 *
 * The tables have to arrive here rather than in a later `ALIAS list`, because a
 * binary response names its fields by index — a client without the argument-key
 * dictionary could not read even the answer to `ALIAS keys`. The ack is the one
 * point in the conversation that can break that circle.
 */
function decodeHandshakeAck(body) {
    if (body.length < 13) throw new CheetahBinaryError('cheetah handshake ack is truncated');
    const cursor = { at: 13 };
    const readShort = () => {
        const length = body[cursor.at];
        const text = body.toString('utf8', cursor.at + 1, cursor.at + 1 + length);
        cursor.at += 1 + length;
        return text;
    };
    const ack = {
        version: body[0],
        widths: { uint: body[1], int: body[2], float: body[3] },
        flags: body[4],
        epoch: Number(body.readBigUInt64BE(5)),
        digest: readShort(),
        keysDigest: readShort(),
        commands: [],
        keys: [],
    };

    let count = body.readUInt16BE(cursor.at);
    cursor.at += 2;
    for (let i = 0; i < count; i += 1) {
        const id = body.readUInt16BE(cursor.at);
        const kind = COMMAND_KINDS[body[cursor.at + 2]] || 'unknown';
        cursor.at += 3;
        ack.commands.push({ id, kind, name: readShort() });
    }
    count = body.readUInt16BE(cursor.at);
    cursor.at += 2;
    for (let i = 0; i < count; i += 1) {
        const id = body.readUInt16BE(cursor.at);
        cursor.at += 2;
        ack.keys.push({ id, name: readShort() });
    }
    return ack;
}

// --- request encoding --------------------------------------------------------

function encodeShortString(text) {
    const raw = Buffer.from(String(text), 'utf8');
    if (raw.length > 255) throw new CheetahBinaryError(`cheetah binary name is too long: ${text}`);
    return Buffer.concat([Buffer.from([raw.length]), raw]);
}

function encodeLengthPrefixed(kind, raw) {
    const header = Buffer.allocUnsafe(5);
    header[0] = kind << 4;
    header.writeUInt32BE(raw.length, 1);
    return Buffer.concat([header, raw]);
}

function encodeUintValue(value, declared, effective) {
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64BE(BigInt(value));
    return Buffer.concat([Buffer.from([(KIND.UINT << 4) | (declared & 0x0f)]), buf.subarray(8 - effective)]);
}

function encodeIntValue(value, declared, effective) {
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(BigInt(value));
    return Buffer.concat([Buffer.from([(KIND.INT << 4) | (declared & 0x0f)]), buf.subarray(8 - effective)]);
}

function encodeFloatValue(value, declared, effective) {
    if (effective !== 4 && effective !== 8) {
        throw new CheetahBinaryError('cheetah float width must be 4 or 8');
    }
    const buf = Buffer.allocUnsafe(effective);
    if (effective === 4) buf.writeFloatBE(value);
    else buf.writeDoubleBE(value);
    return Buffer.concat([Buffer.from([(KIND.FLOAT << 4) | (declared & 0x0f)]), buf]);
}

/**
 * Encode one typed value. `spec` is `{type, value, width}` where `type` is a
 * `KIND` name in lower case (`'uint'`, `'bytes'`, …).
 *
 * A `width` of 0 (or none) declares "the width the server resolves" and writes
 * exactly that many bytes, taken from `widths` — so `widths` must be what the
 * server will resolve for this argument: the table's profile when one is
 * declared, the session defaults otherwise. Getting that wrong is not a
 * rounding error but a misread frame, which is why the transcoder never uses
 * width 0 and states every width outright.
 */
function encodeValue(spec, widths = DEFAULT_WIDTHS) {
    const declared = spec.width || 0;
    switch (spec.type) {
        case 'string':
            return encodeLengthPrefixed(KIND.STRING, Buffer.from(String(spec.value), 'utf8'));
        case 'bytes':
            return encodeLengthPrefixed(
                KIND.BYTES,
                Buffer.isBuffer(spec.value) ? spec.value : Buffer.from(String(spec.value), 'latin1')
            );
        case 'uint':
            return encodeUintValue(spec.value, declared, declared || widths.uint);
        case 'int':
            return encodeIntValue(spec.value, declared, declared || widths.int);
        case 'float':
            return encodeFloatValue(spec.value, declared, declared || widths.float);
        case 'bool':
            return Buffer.from([KIND.BOOL << 4, spec.value ? 1 : 0]);
        case 'enum': {
            const buf = Buffer.allocUnsafe(4);
            buf[0] = KIND.ENUM << 4;
            buf[1] = spec.family || ENUM_FAMILY.COMMANDS;
            buf.writeUInt16BE(spec.value, 2);
            return buf;
        }
        case 'null':
            return Buffer.from([KIND.NULL << 4]);
        default:
            throw new CheetahBinaryError(`cheetah unknown binary value type: ${spec.type}`);
    }
}

/**
 * Build a request frame from an explicit, typed description:
 *
 *   {command: 'RECORD', args: [
 *      {type: 'string', value: 'set'},
 *      {key: 'table', type: 'string', value: 'ngram'},
 *      {key: 'cnt',   type: 'uint',   value: 42, width: 4},
 *   ]}
 *
 * `suffix` carries the `:<n>` of `INSERT:16`. An argument with no `key` is
 * positional; `type: 'null'` is an omitted modifier and disappears from the
 * line, which is how an optional field stays in a caller's object.
 *
 * Widths left at 0 resolve against the table's profile, exactly as the server
 * resolves them: a `table=` argument switches the resolution for every argument
 * after it, which is why `table=` goes first. `table` may also be passed
 * up front when the command names its table some other way.
 */
function encodeRequest({ command, suffix = null, table = null, args = [] }, session = null) {
    const name = String(command).toUpperCase();
    const id = session ? session.commandId(name) : undefined;
    let flags = 0;
    const head = [];
    if (id === undefined) {
        flags |= 0x01;
        head.push(encodeShortString(name));
    } else {
        const buf = Buffer.allocUnsafe(2);
        buf.writeUInt16BE(id);
        head.push(buf);
    }
    if (suffix !== null && suffix !== undefined && suffix !== '') {
        flags |= 0x02;
        head.push(encodeShortString(suffix));
    }

    const count = Buffer.allocUnsafe(2);
    count.writeUInt16BE(args.length);
    const parts = [Buffer.from([flags]), ...head, count];
    let widths = session ? session.widthsFor(table) : DEFAULT_WIDTHS;

    for (const arg of args) {
        if (!arg.key) {
            parts.push(Buffer.from([KEY_MODE.POSITIONAL]));
        } else {
            const keyId = session ? session.keyId(arg.key) : undefined;
            if (keyId === undefined) {
                parts.push(Buffer.from([KEY_MODE.INLINE]), encodeShortString(String(arg.key).toLowerCase()));
            } else {
                const buf = Buffer.allocUnsafe(3);
                buf[0] = KEY_MODE.INDEXED;
                buf.writeUInt16BE(keyId, 1);
                parts.push(buf);
            }
        }
        parts.push(encodeValue(arg, widths));
        // Mirrors the server: the table named in this frame governs the widths
        // of every argument after it.
        if (session && arg.key && String(arg.key).toLowerCase() === 'table' && arg.type === 'string') {
            widths = session.widthsFor(arg.value);
        }
    }
    return encodeFrame(FRAME.REQUEST, Buffer.concat(parts));
}

// --- transcoding a canonical line -------------------------------------------

/**
 * A token is read as `key=value` only when its head looks like a modifier name.
 * Without this an `INSERT` payload carrying base64 padding — or any `=` at all —
 * would be cut into a 500-character "key". The rule is lower case on purpose:
 * the server lower-cases an argument name, so a head that is already lower case
 * re-renders identically whichever way it was read, which is what keeps the
 * transcoding exact in the ambiguous cases.
 */
const ARGUMENT_NAME = /^[a-z][a-z0-9_]{0,63}$/;

/** `123` / `-7` / `0.25` exactly as they would be re-rendered, or null. */
function canonicalNumber(token) {
    if (token === '' || token.length > 32) return null;
    if (/^(0|[1-9][0-9]*)$/.test(token)) {
        const value = Number(token);
        if (Number.isSafeInteger(value)) return { type: 'uint', value };
        return null;
    }
    if (/^-(0|[1-9][0-9]*)$/.test(token)) {
        const value = Number(token);
        if (Number.isSafeInteger(value)) return { type: 'int', value };
        return null;
    }
    if (/^-?(0|[1-9][0-9]*)\.[0-9]+$/.test(token)) {
        const value = Number(token);
        // Go's shortest round-trip formatting must give the token back, or the
        // server would answer on a line the caller never wrote.
        if (Number.isFinite(value) && String(value) === token) return { type: 'float', value };
    }
    return null;
}

/** Minimal byte width for an unsigned/signed value; 0 means "the default". */
function minimalWidth(type, value) {
    const magnitude = type === 'uint' ? value : Math.abs(value) * 2;
    if (magnitude <= 0xff) return 1;
    if (magnitude <= 0xffff) return 2;
    if (magnitude <= 0xffffffff) return 4;
    return 8;
}

/**
 * Classify one already-encoded token from a canonical line.
 *
 * `x<hex>` becomes real bytes (the server renders them straight back to
 * `x<hex>`, so the line is unchanged and the hex stops costing two characters
 * per byte); a canonical number becomes a number; everything else stays a
 * string.
 *
 * Every width is stated outright. The transcoder does not know which table an
 * arbitrary line addresses, so it cannot predict what the server would resolve
 * a width-0 tag to — and the nibble that states it is free.
 */
function typeToken(token) {
    if (/^x([0-9a-fA-F]{2})+$/.test(token)) {
        return { type: 'bytes', value: Buffer.from(token.slice(1), 'hex') };
    }
    const numeric = canonicalNumber(token);
    if (numeric) {
        if (numeric.type === 'float') return { ...numeric, width: 8 };
        return { ...numeric, width: minimalWidth(numeric.type, numeric.value) };
    }
    return { type: 'string', value: token };
}

/**
 * Transcode a canonical command line into a request frame.
 *
 * This is what lets every command layer in this binder keep producing text and
 * still speak binary. It is lossless for any line the server itself would
 * produce: tokens are single-space separated, and the last positional argument
 * keeps its spaces because that is exactly what `INSERT`/`EDIT` treat as the
 * rest of the line.
 */
function encodeCommandLine(line, session = null) {
    const text = String(line);
    if (text.includes('\n') || text.includes('\r')) {
        throw new CheetahBinaryError('cheetah command must not contain a newline');
    }
    const firstSpace = text.indexOf(' ');
    const head = firstSpace === -1 ? text : text.slice(0, firstSpace);
    const rest = firstSpace === -1 ? '' : text.slice(firstSpace + 1);

    const colon = head.indexOf(':');
    const command = colon === -1 ? head : head.slice(0, colon);
    const suffix = colon === -1 ? null : head.slice(colon + 1);

    // Splitting on every single space and letting the server re-join with one
    // is exact even for a payload with runs of spaces: an empty token survives
    // as an empty positional value and reappears as the space it was.
    const args = [];
    if (rest !== '') {
        for (const token of rest.split(' ')) {
            const equals = token.indexOf('=');
            const key = equals > 0 ? token.slice(0, equals) : '';
            if (!ARGUMENT_NAME.test(key)) args.push(typeToken(token));
            else args.push({ key, ...typeToken(token.slice(equals + 1)) });
        }
    }
    return encodeRequest({ command, suffix, args }, session);
}

// --- response decoding -------------------------------------------------------

function decodeValue(body, cursor, widths, session) {
    const tag = body[cursor.at];
    cursor.at += 1;
    const kind = tag >> 4;
    let width = tag & 0x0f;

    switch (kind) {
        case KIND.STRING:
        case KIND.BYTES: {
            const length = body.readUInt32BE(cursor.at);
            cursor.at += 4;
            const raw = body.subarray(cursor.at, cursor.at + length);
            cursor.at += length;
            if (kind === KIND.BYTES) return { bytes: raw, text: `x${raw.toString('hex')}` };
            return { text: raw.toString('utf8') };
        }
        case KIND.UINT: {
            if (width === 0) width = widths.uint;
            let value = 0n;
            for (let i = 0; i < width; i += 1) value = (value << 8n) | BigInt(body[cursor.at + i]);
            cursor.at += width;
            const number = Number(value);
            return { number: Number.isSafeInteger(number) ? number : value, text: value.toString(10) };
        }
        case KIND.INT: {
            if (width === 0) width = widths.int;
            let value = 0n;
            for (let i = 0; i < width; i += 1) value = (value << 8n) | BigInt(body[cursor.at + i]);
            const bits = BigInt(width * 8);
            if (value >= 1n << (bits - 1n)) value -= 1n << bits;
            cursor.at += width;
            const number = Number(value);
            return { number: Number.isSafeInteger(number) ? number : value, text: value.toString(10) };
        }
        case KIND.FLOAT: {
            if (width === 0) width = widths.float;
            const value = width === 4 ? body.readFloatBE(cursor.at) : body.readDoubleBE(cursor.at);
            cursor.at += width;
            return { number: value, text: formatFloat(value) };
        }
        case KIND.BOOL: {
            const value = body[cursor.at] !== 0;
            cursor.at += 1;
            return { boolean: value, text: value ? '1' : '0' };
        }
        case KIND.ENUM: {
            const family = body[cursor.at];
            const id = body.readUInt16BE(cursor.at + 1);
            cursor.at += 3;
            const name =
                family === ENUM_FAMILY.COMMANDS ? session && session.commandName(id) : session && session.keyName(id);
            if (!name) throw new CheetahBinaryError(`cheetah unknown enum id ${id} in family ${family}`);
            return { text: name, enum: { family, id } };
        }
        case KIND.NULL:
            return { skip: true, text: '' };
        default:
            throw new CheetahBinaryError(`cheetah unknown binary value type: ${kind}`);
    }
}

/**
 * Go's `strconv.FormatFloat(v, 'g', -1, 64)`, which is what the server used to
 * render the number before it became a float. JavaScript's own shortest form
 * agrees with it for every value a response carries.
 */
function formatFloat(value) {
    return String(value);
}

/**
 * Decode a response frame body.
 *
 * Returns `{status, line, fields, flags}` where `line` is the canonical
 * response line, byte for byte what a text connection would have received —
 * which is what lets `parseResponse` and every layer above stay untouched.
 */
function decodeResponse(body, session = null, widths = DEFAULT_WIDTHS) {
    const status = body[0];
    const count = body.readUInt16BE(1);
    const cursor = { at: 3 };
    const fields = Object.create(null);
    const flags = [];
    let line = STATUS_WORDS[status] !== undefined ? STATUS_WORDS[status] : '';

    for (let i = 0; i < count; i += 1) {
        const mode = body[cursor.at];
        cursor.at += 1;
        let key = '';
        if (mode === KEY_MODE.INDEXED) {
            const id = body.readUInt16BE(cursor.at);
            cursor.at += 2;
            key = (session && session.keyName(id)) || '';
            if (!key) throw new CheetahBinaryError(`cheetah unknown argument index ${id}`);
        } else if (mode === KEY_MODE.INLINE) {
            const length = body[cursor.at];
            key = body.toString('utf8', cursor.at + 1, cursor.at + 1 + length);
            cursor.at += 1 + length;
        } else if (mode !== KEY_MODE.POSITIONAL) {
            throw new CheetahBinaryError(`cheetah unknown key mode ${mode}`);
        }

        const decoded = decodeValue(body, cursor, widths, session);
        if (decoded.skip) continue;

        // `value=` is READ's raw payload: it left as bytes and goes back to the
        // line as bytes, unescaped, exactly as the text protocol delivers it.
        const text = key === 'value' && decoded.bytes ? decoded.bytes.toString('latin1') : decoded.text;
        if (key) fields[key] = text;
        else flags.push(text);
        line += `,${key ? `${key}=` : ''}${text}`;
    }

    if (status === STATUS.ERROR) {
        return { status, line, fields, flags, error: flags[0] !== undefined ? flags[0] : '' };
    }
    return { status, line, fields, flags, error: null };
}

module.exports = {
    BinarySession,
    COMMAND_KINDS,
    CheetahBinaryError,
    DEFAULT_WIDTHS,
    ENUM_FAMILY,
    FRAME,
    FRAME_HEADER_BYTES,
    FRAME_MAGIC,
    KEY_MODE,
    KIND,
    MAX_BODY_BYTES,
    PROTOCOL_VERSION,
    STATUS,
    canonicalNumber,
    decodeHandshakeAck,
    decodeResponse,
    encodeCommandLine,
    encodeFrame,
    encodeHandshake,
    encodeRequest,
    encodeValue,
    minimalWidth,
    readFrame,
    typeToken,
};

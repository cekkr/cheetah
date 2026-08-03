// Multi-field record tables — one thing described by one row.
//
// A pair name maps to exactly one payload, so describing one thing with several
// quantities used to mean several names (`cnt:<k>`, `prob:<k>`, `meta:<k>`):
// three trie entries, three payloads, three round trips, and nothing keeping
// them consistent. A record table declares those quantities once as fixed-width
// fields and packs them side by side into a single row.
//
//   await records.define(conn, 'ngram', 'cnt:uint:4,prob:float:4,label:string:12');
//   await records.setRow(conn, 'ngram', 'berlin', { cnt: 42, prob: 0.25, label: 'city' });
//   await records.getRow(conn, 'ngram', 'berlin');  // { cnt: 42, prob: 0.25, label: 'city' }
//
// Three things about the family are worth knowing before using it, all of them
// server behaviour this module only spells:
//
//   1. **A field is added or dropped without rewriting a row.** Offsets never
//      move, so a row written before an `add` reads `null` for the new field —
//      not a zero nobody wrote — until it is next written, and a `drop` leaves
//      the retired field's bytes in place as dead space.
//   2. **`compact` is the only operation that touches rows**, which is why it
//      is a separate call rather than a side effect of `alter`.
//   3. **A field name is an argument.** `RECORD set table=t key=k <field>=<v>`
//      puts field names in the same namespace as the command's own modifiers,
//      so the reserved ones are refused — here as well as on the server,
//      because failing at `define` is more useful than failing at every write.
//
// As in `graph.js`, the `build*` functions are pure and the `async` ones are
// those builders plus one `send`.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, decodePayload, encodeArgument, numericField, parseCursor, rawArgument } =
    require('./protocol');

/** Field kinds the server understands (record_schema.go → recordKindAliases). */
const FIELD_TYPES = Object.freeze(['uint', 'int', 'float', 'bool', 'bytes', 'string']);

/**
 * Widths the server assumes when a spec omits them. `bytes`/`string` have none:
 * their width is what decides the cost of every row, so it is required.
 */
const DEFAULT_FIELD_WIDTHS = Object.freeze({ uint: 8, int: 8, float: 8, bool: 1 });

/**
 * Names a field may not take, because in `RECORD set` a field *is* a modifier.
 * Mirrors `recordReservedNames` in src/record_schema.go.
 */
const RESERVED_FIELD_NAMES = Object.freeze(new Set([
    'table', 'key', 'keys', 'fields', 'field', 'prefix', 'limit', 'cursor',
    'add', 'drop', 'compact', 'if_not_exists', 'payloads', 'hidden',
    'type', 'bytes', 'width', 'name', 'id',
]));

const FIELD_NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
const TABLE_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

const DEFAULT_SCAN_LIMIT = 500;

function tableName(name) {
    const text = String(name || '').trim();
    if (!text || text.length > 64 || !TABLE_NAME_PATTERN.test(text)) {
        throw new CheetahError(`cheetah record table name is invalid: ${JSON.stringify(name)}`);
    }
    return text;
}

function fieldName(name) {
    const text = String(name || '').trim().toLowerCase();
    if (!FIELD_NAME_PATTERN.test(text)) {
        throw new CheetahError(`cheetah record field name is invalid: ${JSON.stringify(name)}`);
    }
    if (RESERVED_FIELD_NAMES.has(text)) {
        throw new CheetahError(
            `cheetah record field name "${text}" collides with a RECORD modifier; ` +
            'the server refuses it at define time'
        );
    }
    return text;
}

/**
 * Render one field declaration as the server's `name:type[:bytes]`. Accepts a
 * spec string, `{name, type, bytes|width}`, or `[name, type, width]` — the
 * shapes a caller naturally has.
 */
function fieldSpec(field) {
    let name;
    let type;
    let width = 0;
    if (typeof field === 'string') {
        const parts = field.split(':').map((part) => part.trim());
        if (parts.length < 2 || parts.length > 3) {
            throw new CheetahError(`cheetah record field spec is invalid: ${field}`);
        }
        [name, type] = parts;
        width = parts.length === 3 && parts[2] ? Number.parseInt(parts[2], 10) : 0;
    } else if (Array.isArray(field)) {
        if (field.length < 2 || field.length > 3) {
            throw new CheetahError(`cheetah record field spec is invalid: ${JSON.stringify(field)}`);
        }
        [name, type] = field;
        width = field.length === 3 ? Number(field[2]) : 0;
    } else if (field && typeof field === 'object') {
        name = field.name;
        type = field.type;
        width = Number(field.width ?? field.bytes ?? 0);
    } else {
        throw new CheetahError(`cheetah record field spec is invalid: ${JSON.stringify(field)}`);
    }

    name = fieldName(name);
    type = String(type || '').trim().toLowerCase();
    if (!FIELD_TYPES.includes(type)) {
        throw new CheetahError(`cheetah record field type is unknown: ${JSON.stringify(type)}`);
    }
    if (!width) width = DEFAULT_FIELD_WIDTHS[type] || 0;
    if (!width) {
        throw new CheetahError(
            `cheetah record field "${name}" of type ${type} needs an explicit byte width`
        );
    }
    return `${name}:${type}:${width}`;
}

/** `fields=`/`add=` — a comma-separated list of specs, from any of the shapes above. */
function fieldSpecs(fields) {
    let candidates;
    if (typeof fields === 'string') {
        candidates = fields.split(',').filter((part) => part.trim());
    } else if (Array.isArray(fields)) {
        candidates = fields;
    } else if (fields && typeof fields === 'object') {
        // { cnt: 'uint:4' } — the shape a config object naturally holds.
        candidates = Object.entries(fields).map(([name, spec]) => `${name}:${spec}`);
    } else {
        throw new CheetahError('cheetah RECORD requires at least one field');
    }
    const rendered = candidates.map(fieldSpec);
    if (rendered.length === 0) throw new CheetahError('cheetah RECORD requires at least one field');
    return rendered.join(',');
}

function commaList(value) {
    if (value === undefined || value === null) return null;
    const parts = (Array.isArray(value) ? value : String(value).split(','))
        .map((entry) => String(entry).trim())
        .filter(Boolean);
    return parts.length > 0 ? parts.join(',') : null;
}

/**
 * A field value in the dialect `RECORD set` reads: numbers as decimals,
 * booleans as `1`/`0`, text and bytes through the same `x<hex>` escape as any
 * other argument — which text *must* use when it holds a space or begins with
 * an `x`, or the server re-reads it as hex.
 */
function encodeFieldValue(name, value) {
    if (value === undefined || value === null) {
        throw new CheetahError(`cheetah record field "${name}" cannot be set to null`);
    }
    if (typeof value === 'boolean') return value ? 1 : 0;
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
            throw new CheetahError(`cheetah record field "${name}" must be a finite number`);
        }
        return value;
    }
    return encodeArgument(value);
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

function buildDefine(table, fields, { ifNotExists = false } = {}) {
    return buildKeyValueCommand('RECORD define', {
        table: tableName(table),
        fields: fieldSpecs(fields),
        if_not_exists: ifNotExists ? 1 : null,
    });
}

function buildAlter(table, { add = null, drop = null, compact = false } = {}) {
    if (add === null && drop === null) {
        throw new CheetahError('cheetah RECORD alter needs add or drop');
    }
    return buildKeyValueCommand('RECORD alter', {
        table: tableName(table),
        add: add === null ? null : fieldSpecs(add),
        drop: drop === null ? null : commaList((Array.isArray(drop) ? drop : [drop]).map(fieldName)),
        compact: compact ? 1 : null,
    });
}

function buildCompact(table) {
    return buildKeyValueCommand('RECORD compact', { table: tableName(table) });
}

function buildSchema(table, { rows = false } = {}) {
    return buildKeyValueCommand('RECORD schema', { table: tableName(table), rows: rows ? 1 : null });
}

function buildTables() {
    return 'RECORD tables';
}

function buildSet(table, key, values) {
    const entries = Object.entries(values || {});
    if (entries.length === 0) throw new CheetahError('cheetah RECORD set needs at least one field');
    const fields = { table: tableName(table), key: encodeArgument(key) };
    for (const [name, value] of entries) {
        fields[fieldName(name)] = encodeFieldValue(name, value);
    }
    return buildKeyValueCommand('RECORD set', fields);
}

function buildGet(table, key, { fields = null } = {}) {
    return buildKeyValueCommand('RECORD get', {
        table: tableName(table),
        key: encodeArgument(key),
        fields: fields === null ? null : commaList(fields.map(fieldName)),
    });
}

function buildScan(table, { prefix = null, limit = DEFAULT_SCAN_LIMIT, cursor = null, fields = null } = {}) {
    return buildKeyValueCommand('RECORD scan', {
        table: tableName(table),
        prefix: prefix === null || prefix === '' ? null : encodeArgument(prefix),
        limit: limit || null,
        // The cursor is already in the server's own x<hex> spelling; encoding it
        // again would resume the sweep from a prefix that does not exist.
        cursor: cursor ? rawArgument(cursor) : null,
        fields: fields === null ? null : commaList(fields.map(fieldName)),
    });
}

function buildSelect(
    table,
    { field, op = 'eq', value, prefix = null, limit = DEFAULT_SCAN_LIMIT, budget = null, cursor = null, fields = null } = {}
) {
    const predicate = String(op || 'eq').toLowerCase();
    if (!['eq', 'ne', 'lt', 'lte', 'gt', 'gte'].includes(predicate)) {
        throw new CheetahError(`cheetah RECORD select predicate is invalid: ${JSON.stringify(op)}`);
    }
    return buildKeyValueCommand('RECORD select', {
        table: tableName(table),
        field: fieldName(field),
        op: predicate,
        value: encodeFieldValue(field, value),
        prefix: prefix === null || prefix === '' ? null : encodeArgument(prefix),
        limit: limit || null,
        budget: budget || null,
        cursor: cursor ? rawArgument(cursor) : null,
        fields: fields === null ? null : commaList(fields.map(fieldName)),
    });
}

function buildIndex(table, field = null, { action = 'create' } = {}) {
    const normalized = String(action || 'create').toLowerCase();
    if (!['create', 'rebuild', 'drop', 'list'].includes(normalized)) {
        throw new CheetahError(`cheetah RECORD index action is invalid: ${JSON.stringify(action)}`);
    }
    if (normalized !== 'list' && (field === null || field === undefined)) {
        throw new CheetahError('cheetah RECORD index needs a field');
    }
    return buildKeyValueCommand('RECORD index', {
        table: tableName(table),
        field: normalized === 'list' ? null : fieldName(field),
        action: normalized,
    });
}

function buildDeleteRow(table, key) {
    return buildKeyValueCommand('DEL records', { table: tableName(table), key: encodeArgument(key) });
}

function buildDropTable(table) {
    return buildKeyValueCommand('DEL records', { table: tableName(table), drop: 1 });
}

// ---------------------------------------------------------------------------
// Round-trip helpers
// ---------------------------------------------------------------------------

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

/** The counters every schema-shaped response carries, plus the field list when present. */
function schemaFrom(response) {
    const payload = decodePayload(response.fields);
    const rows = numericField(response.fields, 'rows', null);
    const base = {
        table: response.fields.table || '',
        width: numericField(response.fields, 'width', 0),
        deadBytes: numericField(response.fields, 'dead_bytes', 0),
        generation: numericField(response.fields, 'generation', 0),
        rows,
        fields: [],
        response,
    };
    if (payload && typeof payload === 'object' && Array.isArray(payload.fields)) {
        base.fields = payload.fields.map((field) => ({
            name: field.name,
            type: field.type,
            // The server spells the width `bytes`; `width` reads better beside
            // `offset` and does not shadow anything in caller code.
            width: field.bytes,
            offset: field.offset,
            indexed: field.indexed === true,
        }));
    }
    return base;
}

async function define(conn, table, fields, options) {
    const response = await sendOrThrow(conn, buildDefine(table, fields, options), `RECORD define ${table}`);
    return schemaFrom(response);
}

/**
 * Add and/or remove fields on a live table. Neither rewrites a row; pass
 * `compact: true` to chain the rewrite that reclaims a dropped field's bytes.
 */
async function alter(conn, table, options) {
    const response = await sendOrThrow(conn, buildAlter(table, options), `RECORD alter ${table}`);
    return { ...schemaFrom(response), added: numericField(response.fields, 'added', 0), dropped: numericField(response.fields, 'dropped', 0) };
}

async function compact(conn, table) {
    const response = await sendOrThrow(conn, buildCompact(table), `RECORD compact ${table}`);
    return { ...schemaFrom(response), rewritten: numericField(response.fields, 'rewritten', 0) };
}

/**
 * The table's shape, or null when there is no such table. `rows: true` adds the
 * live row count — opt-in because counting walks the whole table.
 */
async function schema(conn, table, options) {
    const line = buildSchema(table, options);
    const response = await conn.send(line);
    if (response.ok) return schemaFrom(response);
    if (response.error && response.error.includes('record_table_not_found')) return null;
    throw new CheetahError(`cheetah RECORD schema ${table} failed: ${response.error || response.raw}`, {
        command: line,
        response,
    });
}

async function tables(conn) {
    const response = await sendOrThrow(conn, buildTables(), 'RECORD tables');
    const payload = decodePayload(response.fields) || [];
    return payload.map((entry) => ({
        table: entry.table,
        width: entry.width,
        deadBytes: entry.dead_bytes,
        generation: entry.generation,
        fields: (entry.fields || []).map((field) => ({
            name: field.name,
            type: field.type,
            width: field.bytes,
            offset: field.offset,
            indexed: field.indexed === true,
        })),
    }));
}

/**
 * Upsert a row, writing **only** the fields given: the others keep the bytes
 * they had, because the server reads the row, patches it and writes it back.
 */
async function setRow(conn, table, key, values) {
    const response = await sendOrThrow(conn, buildSet(table, key, values), `RECORD set ${table}`);
    return {
        created: numericField(response.fields, 'created', 0) === 1,
        written: numericField(response.fields, 'written', 0),
        absKey: numericField(response.fields, 'abs_key', null),
        response,
    };
}

/**
 * One row as a plain object, or null when there is no such row. A field the row
 * predates reads `null`, which is not the same as a zero somebody wrote.
 */
async function getRow(conn, table, key, options) {
    const line = buildGet(table, key, options);
    const response = await conn.send(line);
    if (response.ok) return decodePayload(response.fields) || {};
    if (response.error && response.error.includes('not_found')) return null;
    throw new CheetahError(`cheetah RECORD get ${table} failed: ${response.error || response.raw}`, {
        command: line,
        response,
    });
}

/** Row keys come back in the unambiguous `x<hex>` spelling. */
function decodeRowKey(raw) {
    if (!raw) return Buffer.alloc(0);
    const text = String(raw);
    return text.startsWith('x') ? Buffer.from(text.slice(1), 'hex') : Buffer.from(text, 'utf8');
}

function rowsFromPayload(response) {
    const payload = decodePayload(response.fields) || [];
    return payload.map((entry) => {
        const bytes = decodeRowKey(entry.key);
        return {
            keyHex: entry.key,
            bytes,
            key: bytes.toString('utf8'),
            absKey: entry.abs_key ?? null,
            fields: entry.fields || {},
        };
    });
}

/** One page of rows, already decoded into their declared fields. */
async function scanPage(conn, table, options = {}) {
    const response = await sendOrThrow(conn, buildScan(table, options), `RECORD scan ${table}`);
    return {
        rows: rowsFromPayload(response),
        cursor: parseCursor(response.fields),
        response,
    };
}

/** One bounded page selected by a decoded field predicate. */
async function selectPage(conn, table, options = {}) {
    const response = await sendOrThrow(conn, buildSelect(table, options), `RECORD select ${table}`);
    return {
        rows: rowsFromPayload(response),
        cursor: parseCursor(response.fields),
        scanned: numericField(response.fields, 'scanned', 0),
        indexed: numericField(response.fields, 'indexed', 0) === 1,
        response,
    };
}

/** Follow selection cursors until the predicate sweep finishes. */
async function* selectRows(conn, table, options = {}) {
    let cursor = options.cursor || null;
    let yielded = 0;
    const maxRows = options.maxRows ?? Infinity;
    do {
        const page = await selectPage(conn, table, { ...options, cursor });
        for (const row of page.rows) {
            if (yielded >= maxRows) return;
            yielded += 1;
            yield row;
        }
        cursor = page.cursor;
    } while (cursor);
}

async function selectAll(conn, table, options) {
    const rows = [];
    for await (const row of selectRows(conn, table, options)) rows.push(row);
    return rows;
}

async function configureIndex(conn, table, field, options) {
    const line = buildIndex(table, field, options);
    const response = await sendOrThrow(conn, line, `RECORD index ${table}`);
    return {
        field: response.fields.field || String(field),
        action: response.fields.action || options?.action || 'create',
        changed: numericField(response.fields, 'changed', 0) === 1,
        indexed: numericField(response.fields, 'indexed', 0) === 1,
        entries: numericField(response.fields, 'entries', 0),
        response,
    };
}

async function listIndexes(conn, table) {
    const response = await sendOrThrow(conn, buildIndex(table, null, { action: 'list' }), `RECORD index ${table}`);
    return decodePayload(response.fields) || [];
}

/** Every row of a table (or of a key prefix), one page at a time. */
async function* scanRows(conn, table, { limit = DEFAULT_SCAN_LIMIT, maxRows = Infinity, prefix = null, fields = null } = {}) {
    let cursor = null;
    let yielded = 0;
    do {
        const page = await scanPage(conn, table, { limit, cursor, prefix, fields });
        for (const row of page.rows) {
            if (yielded >= maxRows) return;
            yielded += 1;
            yield row;
        }
        cursor = page.cursor;
    } while (cursor);
}

/** `scanRows` collected into an array. */
async function scanAll(conn, table, options) {
    const rows = [];
    for await (const row of scanRows(conn, table, options)) rows.push(row);
    return rows;
}

/** Delete one row — name and payload. False when it was not there. */
async function deleteRow(conn, table, key) {
    const line = buildDeleteRow(table, key);
    const response = await conn.send(line);
    if (response.ok) return true;
    if (response.error && response.error.includes('not_found')) return false;
    throw new CheetahError(`cheetah DEL records ${table} failed: ${response.error || response.raw}`, {
        command: line,
        response,
    });
}

/** Delete a whole table: every row of every generation, then the schema. */
async function dropTable(conn, table) {
    const line = buildDropTable(table);
    const response = await conn.send(line);
    if (response.ok) return numericField(response.fields, 'deleted', 0);
    if (response.error && response.error.includes('record_table_not_found')) return 0;
    throw new CheetahError(`cheetah DEL records ${table} drop=1 failed: ${response.error || response.raw}`, {
        command: line,
        response,
    });
}

module.exports = {
    DEFAULT_FIELD_WIDTHS,
    DEFAULT_SCAN_LIMIT,
    FIELD_TYPES,
    RESERVED_FIELD_NAMES,
    alter,
    buildAlter,
    buildCompact,
    buildDefine,
    buildDeleteRow,
    buildDropTable,
    buildGet,
    buildIndex,
    buildScan,
    buildSelect,
    buildSchema,
    buildSet,
    buildTables,
    compact,
    configureIndex,
    define,
    deleteRow,
    dropTable,
    fieldSpec,
    fieldSpecs,
    getRow,
    listIndexes,
    scanAll,
    scanPage,
    scanRows,
    selectAll,
    selectPage,
    selectRows,
    schema,
    setRow,
    tables,
};

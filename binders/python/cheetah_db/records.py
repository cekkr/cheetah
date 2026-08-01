"""Multi-field record tables — one thing described by one row.

A pair name maps to exactly one payload, so describing one thing with several
quantities used to mean several names (``cnt:<k>``, ``prob:<k>``, ``meta:<k>``):
three trie entries, three payloads, three round trips, and nothing keeping them
consistent. A record table declares those quantities once as fixed-width fields
and packs them side by side into a single row::

    from cheetah_db import records

    records.define(conn, "ngram", "cnt:uint:4,prob:float:4,label:string:12")
    records.set_row(conn, "ngram", "berlin", {"cnt": 42, "prob": 0.25, "label": "city"})
    records.get_row(conn, "ngram", "berlin")   # {'cnt': 42, 'prob': 0.25, 'label': 'city'}

Three things about the family are worth knowing before using it, all of them
server behaviour this module only spells:

  - **A field is added or dropped without rewriting a row.** Offsets never move,
    so a row written before an ``add`` reads ``None`` for the new field (not a
    zero nobody wrote) until it is next written, and a ``drop`` leaves the
    retired field's bytes in place as dead space.
  - **``compact`` is the only operation that touches rows**, which is why it is
    a separate call rather than a side effect of ``alter``.
  - **A field name is an argument.** ``RECORD set table=t key=k <field>=<value>``
    puts field names in the same namespace as the command's own modifiers, so
    the reserved ones are refused — here as well as on the server, because
    failing at :func:`define` is more useful than failing at every write.

Every function takes a ``conn`` — a :class:`~cheetah_db.client.CheetahClient` or
anything else exposing ``send(line) -> Response``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .client import CheetahError
from .protocol import (
    RawArgument,
    Response,
    build_key_value_command,
    encode_argument,
    join_csv,
    to_bytes,
)

__all__ = [
    "DEFAULT_FIELD_WIDTHS",
    "FIELD_TYPES",
    "RESERVED_FIELD_NAMES",
    "RecordField",
    "RecordPage",
    "RecordRow",
    "RecordSchema",
    "RecordWrite",
    "alter",
    "compact",
    "define",
    "delete_row",
    "drop_table",
    "field_spec",
    "get_row",
    "iter_rows",
    "scan",
    "schema",
    "set_row",
    "tables",
]

#: Field kinds the server understands (``record_schema.go`` → ``recordKindAliases``).
FIELD_TYPES = ("uint", "int", "float", "bool", "bytes", "string")

#: Widths the server assumes when a spec omits them. ``bytes``/``string`` have
#: none: their width is what decides the cost of every row, so it is required.
DEFAULT_FIELD_WIDTHS = {"uint": 8, "int": 8, "float": 8, "bool": 1}

#: Names a field may not take, because in ``RECORD set`` a field *is* a
#: modifier. Mirrors ``recordReservedNames`` in ``src/record_schema.go``.
RESERVED_FIELD_NAMES = frozenset(
    {
        "table", "key", "keys", "fields", "field", "prefix", "limit", "cursor",
        "add", "drop", "compact", "if_not_exists", "payloads", "hidden",
        "type", "bytes", "width", "name", "id",
    }
)

_FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_TABLE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")

DEFAULT_SCAN_LIMIT = 500


def _raise(what: str, response: Response) -> None:
    raise CheetahError(f"cheetah {what} failed: {response.reason}", command=what, response=response)


def _table(name: str) -> str:
    text = str(name).strip()
    if not text or not _TABLE_NAME.match(text) or len(text) > 64:
        raise CheetahError(f"cheetah record table name is invalid: {name!r}")
    return text


def _field_name(name: str) -> str:
    text = str(name).strip().lower()
    if not _FIELD_NAME.match(text or ""):
        raise CheetahError(f"cheetah record field name is invalid: {name!r}")
    if text in RESERVED_FIELD_NAMES:
        raise CheetahError(
            f"cheetah record field name {text!r} collides with a RECORD modifier; "
            "the server refuses it at define time"
        )
    return text


@dataclass(frozen=True)
class RecordField:
    """One declared field: a name, a kind, a byte width, and where it starts.

    ``offset`` is filled in by the server and is stable for the life of the
    field — that immutability is what lets a table gain and lose fields without
    rewriting rows.
    """

    name: str
    type: str
    width: int
    offset: int = 0

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "RecordField":
        return cls(
            name=str(data.get("name", "")),
            type=str(data.get("type", "")),
            # The server spells the width `bytes`; `width` reads better next to
            # `offset` and does not shadow the builtin in caller code.
            width=int(data.get("bytes", 0) or 0),
            offset=int(data.get("offset", 0) or 0),
        )

    @property
    def spec(self) -> str:
        return f"{self.name}:{self.type}:{self.width}"


@dataclass(frozen=True)
class RecordSchema:
    """The shape of a table, as ``RECORD schema``/``RECORD tables`` report it."""

    table: str
    fields: tuple[RecordField, ...]
    width: int
    dead_bytes: int
    generation: int
    #: Live row count — only when it was asked for (``schema(..., rows=True)``),
    #: because counting walks the whole table.
    rows: int | None = None

    def field(self, name: str) -> RecordField | None:
        for entry in self.fields:
            if entry.name == name:
                return entry
        return None

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(entry.name for entry in self.fields)

    @classmethod
    def from_json(cls, data: Mapping[str, Any], *, rows: int | None = None) -> "RecordSchema":
        return cls(
            table=str(data.get("table", "")),
            fields=tuple(RecordField.from_json(entry) for entry in data.get("fields", []) or []),
            width=int(data.get("width", 0) or 0),
            dead_bytes=int(data.get("dead_bytes", 0) or 0),
            generation=int(data.get("generation", 0) or 0),
            rows=rows,
        )


@dataclass(frozen=True)
class RecordRow:
    """One row of a scan: the key it is stored under and its decoded fields."""

    key: bytes
    abs_key: int | None
    fields: dict[str, Any]

    @property
    def text(self) -> str:
        return self.key.decode("utf-8", "replace")


@dataclass(frozen=True)
class RecordPage:
    rows: tuple[RecordRow, ...]
    cursor: str | None
    response: Response


@dataclass(frozen=True)
class RecordWrite:
    """What ``RECORD set`` reports: whether the row is new, and where it lives."""

    created: bool
    written: int
    abs_key: int | None


# --------------------------------------------------------------------------- #
# Field specs
# --------------------------------------------------------------------------- #
def field_spec(field: Any) -> str:
    """Render one field declaration as the server's ``name:type[:bytes]``.

    Accepts a spec string, a :class:`RecordField`, a ``(name, type[, width])``
    tuple, or a mapping with those keys — the shapes a caller naturally has.
    """
    if isinstance(field, RecordField):
        name, kind, width = field.name, field.type, field.width
    elif isinstance(field, str):
        parts = [part.strip() for part in field.split(":")]
        if len(parts) not in (2, 3):
            raise CheetahError(f"cheetah record field spec is invalid: {field!r}")
        name, kind = parts[0], parts[1]
        width = int(parts[2]) if len(parts) == 3 and parts[2] else 0
    elif isinstance(field, Mapping):
        name = field.get("name", "")
        kind = field.get("type", "")
        width = int(field.get("width", field.get("bytes", 0)) or 0)
    elif isinstance(field, Sequence):
        parts = list(field)
        if len(parts) not in (2, 3):
            raise CheetahError(f"cheetah record field spec is invalid: {field!r}")
        name, kind = parts[0], parts[1]
        width = int(parts[2]) if len(parts) == 3 else 0
    else:
        raise CheetahError(f"cheetah record field spec is invalid: {field!r}")

    name = _field_name(name)
    kind = str(kind).strip().lower()
    if kind not in FIELD_TYPES:
        raise CheetahError(f"cheetah record field type is unknown: {kind!r}")
    if not width:
        width = DEFAULT_FIELD_WIDTHS.get(kind, 0)
    if not width:
        raise CheetahError(
            f"cheetah record field {name!r} of type {kind} needs an explicit byte width"
        )
    return f"{name}:{kind}:{int(width)}"


def _specs(fields: Any) -> str:
    if fields is None:
        raise CheetahError("cheetah RECORD requires at least one field")
    if isinstance(fields, str):
        candidates: Iterable[Any] = [part for part in fields.split(",") if part.strip()]
    elif isinstance(fields, Mapping):
        # {"cnt": "uint:4"} — the shape a config file naturally holds.
        candidates = [f"{name}:{spec}" for name, spec in fields.items()]
    else:
        candidates = list(fields)
    rendered = [field_spec(candidate) for candidate in candidates]
    if not rendered:
        raise CheetahError("cheetah RECORD requires at least one field")
    return ",".join(rendered)


def _encode_field_value(name: str, value: Any) -> Any:
    """A field value in the dialect ``RECORD set`` reads.

    Numbers travel as decimals, booleans as ``1``/``0``, text and bytes through
    the same ``x<hex>`` escape as any other argument — which text *must* use
    when it holds a space or begins with an ``x``, or the server re-reads it as
    hex.
    """
    if value is None:
        raise CheetahError(f"cheetah record field {name!r} cannot be set to None")
    if isinstance(value, RawArgument):
        return value
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return value
    return encode_argument(to_bytes(value))


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
def define(conn: Any, table: str, fields: Any, *, if_not_exists: bool = False) -> RecordSchema:
    """Create a table. Raises when it exists unless ``if_not_exists``."""
    line = build_key_value_command(
        "RECORD define",
        {"table": _table(table), "fields": _specs(fields), "if_not_exists": if_not_exists or None},
    )
    response = conn.send(line)
    if not response.ok:
        _raise(f"RECORD define {table}", response)
    return _schema_from(response)


def alter(
    conn: Any,
    table: str,
    *,
    add: Any = None,
    drop: Any = None,
    compact: bool = False,
) -> RecordSchema:
    """Add and/or remove fields on a live table.

    Neither rewrites a row: a new field reads ``None`` on rows that predate it,
    and a dropped field's bytes stay as dead space. ``compact=True`` chains the
    rewrite that reclaims them, which is the only part that costs anything.
    """
    if add is None and drop is None:
        raise CheetahError("cheetah RECORD alter needs add= or drop=")
    fields: dict[str, Any] = {"table": _table(table)}
    if add is not None:
        fields["add"] = _specs(add)
    if drop is not None:
        names = [drop] if isinstance(drop, str) else list(drop)
        fields["drop"] = join_csv([_field_name(name) for name in names])
    if compact:
        fields["compact"] = 1
    response = conn.send(build_key_value_command("RECORD alter", fields))
    if not response.ok:
        _raise(f"RECORD alter {table}", response)
    return _schema_from(response)


def compact(conn: Any, table: str) -> tuple[RecordSchema, int]:
    """Reclaim the bytes dropped fields left behind. Returns the schema and the
    number of rows rewritten."""
    response = conn.send(build_key_value_command("RECORD compact", {"table": _table(table)}))
    if not response.ok:
        _raise(f"RECORD compact {table}", response)
    return _schema_from(response), response.int_field("rewritten", 0) or 0


def schema(conn: Any, table: str, *, rows: bool = False) -> RecordSchema | None:
    """The table's shape, or ``None`` when there is no such table.

    ``rows=True`` adds the live row count. It is opt-in because counting walks
    the whole table, and describing a table should not cost what reading it does.
    """
    fields: dict[str, Any] = {"table": _table(table)}
    if rows:
        fields["rows"] = 1
    response = conn.send(build_key_value_command("RECORD schema", fields))
    if response.ok:
        return _schema_from(response)
    if response.error and "record_table_not_found" in response.error:
        return None
    _raise(f"RECORD schema {table}", response)
    return None  # pragma: no cover - _raise always raises


def tables(conn: Any) -> list[RecordSchema]:
    """Every record table in this database, with its schema."""
    response = conn.send("RECORD tables")
    if not response.ok:
        _raise("RECORD tables", response)
    payload = response.payload() or []
    return [RecordSchema.from_json(entry) for entry in payload]


def _schema_from(response: Response) -> RecordSchema:
    payload = response.payload()
    rows = response.int_field("rows")
    if isinstance(payload, Mapping):
        return RecordSchema.from_json(payload, rows=rows)
    # `RECORD define`/`alter`/`compact` answer with the counters only; the field
    # list is what `RECORD schema` is for.
    return RecordSchema(
        table=response.field_value("table", "") or "",
        fields=(),
        width=response.int_field("width", 0) or 0,
        dead_bytes=response.int_field("dead_bytes", 0) or 0,
        generation=response.int_field("generation", 0) or 0,
        rows=rows,
    )


# --------------------------------------------------------------------------- #
# Rows
# --------------------------------------------------------------------------- #
def set_row(conn: Any, table: str, key: str | bytes, values: Mapping[str, Any]) -> RecordWrite:
    """Upsert a row, writing **only** the fields given.

    The others keep the bytes they had — the server reads the row, patches it
    and writes it back — so a partial update is a partial update, not a row
    replaced by the fields you happened to pass.
    """
    if not values:
        raise CheetahError("cheetah RECORD set needs at least one field")
    fields: dict[str, Any] = {"table": _table(table), "key": encode_argument(key)}
    for name, value in values.items():
        fields[_field_name(name)] = _encode_field_value(name, value)
    response = conn.send(build_key_value_command("RECORD set", fields))
    if not response.ok:
        _raise(f"RECORD set {table}", response)
    return RecordWrite(
        created=bool(response.int_field("created", 0)),
        written=response.int_field("written", 0) or 0,
        abs_key=response.int_field("abs_key"),
    )


def get_row(
    conn: Any,
    table: str,
    key: str | bytes,
    *,
    fields: Sequence[str] | None = None,
) -> dict[str, Any] | None:
    """One row as a dict, or ``None`` when there is no such row.

    A field the row predates — written before that field was added — reads
    ``None``, which is not the same as a zero somebody wrote.
    """
    request: dict[str, Any] = {"table": _table(table), "key": encode_argument(key)}
    if fields:
        request["fields"] = join_csv([_field_name(name) for name in fields])
    response = conn.send(build_key_value_command("RECORD get", request))
    if response.ok:
        payload = response.payload()
        return dict(payload) if isinstance(payload, Mapping) else {}
    if response.error and ("not_found" in response.error):
        return None
    _raise(f"RECORD get {table}", response)
    return None  # pragma: no cover - _raise always raises


def scan(
    conn: Any,
    table: str,
    *,
    prefix: str | bytes | None = None,
    limit: int = DEFAULT_SCAN_LIMIT,
    cursor: str | None = None,
    fields: Sequence[str] | None = None,
) -> RecordPage:
    """One page of rows, already decoded into their declared fields.

    ``prefix`` filters on the row key exactly as ``PAIR_SCAN`` filters a
    namespace. The cursor travels back **verbatim**: it is already in the
    server's own ``x<hex>`` spelling, and re-encoding it would resume the sweep
    from a prefix that does not exist.
    """
    request: dict[str, Any] = {"table": _table(table), "limit": int(limit) if limit else None}
    if prefix not in (None, b"", ""):
        request["prefix"] = encode_argument(prefix)
    if cursor:
        request["cursor"] = RawArgument(cursor)
    if fields:
        request["fields"] = join_csv([_field_name(name) for name in fields])
    response = conn.send(build_key_value_command("RECORD scan", request))
    if not response.ok:
        _raise(f"RECORD scan {table}", response)
    payload = response.payload() or []
    rows = tuple(
        RecordRow(
            key=_decode_row_key(entry.get("key")),
            abs_key=entry.get("abs_key"),
            fields=dict(entry.get("fields") or {}),
        )
        for entry in payload
    )
    return RecordPage(rows=rows, cursor=response.cursor(), response=response)


def iter_rows(
    conn: Any,
    table: str,
    *,
    prefix: str | bytes | None = None,
    limit: int = DEFAULT_SCAN_LIMIT,
    max_rows: int | None = None,
    fields: Sequence[str] | None = None,
) -> Iterator[RecordRow]:
    """Every row of a table (or of a key prefix), one page at a time."""
    cursor: str | None = None
    yielded = 0
    while True:
        page = scan(conn, table, prefix=prefix, limit=limit, cursor=cursor, fields=fields)
        for row in page.rows:
            if max_rows is not None and yielded >= max_rows:
                return
            yielded += 1
            yield row
        cursor = page.cursor
        if not cursor:
            return


def _decode_row_key(raw: Any) -> bytes:
    """Row keys come back in the unambiguous ``x<hex>`` spelling."""
    if raw is None:
        return b""
    text = str(raw)
    if text.startswith("x"):
        try:
            return bytes.fromhex(text[1:])
        except ValueError:
            return b""
    return text.encode("utf-8")


def delete_row(conn: Any, table: str, key: str | bytes) -> bool:
    """Delete one row — name and payload. ``False`` when it was not there."""
    response = conn.send(
        build_key_value_command("DEL records", {"table": _table(table), "key": encode_argument(key)})
    )
    if response.ok:
        return True
    if response.error and "not_found" in response.error:
        return False
    _raise(f"DEL records {table}", response)
    return False  # pragma: no cover - _raise always raises


def drop_table(conn: Any, table: str) -> int:
    """Delete a whole table: every row of every generation, then the schema.

    Returns the number of rows removed.
    """
    response = conn.send(
        build_key_value_command("DEL records", {"table": _table(table), "drop": 1})
    )
    if response.ok:
        return response.int_field("deleted", 0) or 0
    if response.error and "record_table_not_found" in response.error:
        return 0
    _raise(f"DEL records {table} drop=1", response)
    return 0  # pragma: no cover - _raise always raises

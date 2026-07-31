"""Value + name layer helpers — the two-step Cheetah write, in one place.

Cheetah separates the bytes from the name: ``INSERT`` stores a payload and
returns an absolute key, ``PAIR_SET`` binds a trie prefix to that key. There is
no combined single-record command, so a write is two round trips and a read is
two (``PAIR_GET`` then ``READ``). Everything that persists a record goes through
here so the pairing is never half-done in one caller and whole in another.

Two encodings coexist on purpose:

  - **Text and JSON travel raw.** ``INSERT`` takes the rest of the line as
    bytes, so a UTF-8 document needs no wrapping — and the stored bytes stay
    readable from the CLI, which matters more than it sounds when debugging.
  - **Binary travels base64** (:func:`put_bytes`). A newline-delimited protocol
    cannot carry an arbitrary byte string, and ``value=`` runs to end of line,
    so the wrapping is the client's job. :func:`cheetah_db.protocol.decode_transport_payload`
    is the matching unwrap for reducer output.

Every function takes a ``conn`` — a :class:`~cheetah_db.client.CheetahClient` or
anything else exposing ``send(line) -> Response``.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Sequence

from .client import CheetahError
from .protocol import (
    Branch,
    RawArgument,
    Response,
    ScanItem,
    build_command,
    build_key_value_command,
    encode_argument,
    numeric_field,
    parse_branches,
    to_bytes,
)

__all__ = [
    "PairSummary",
    "ScanPage",
    "delete_pair",
    "delete_value",
    "edit",
    "get_absolute_key",
    "get_bytes",
    "get_json",
    "get_value",
    "insert",
    "pair_get",
    "pair_purge",
    "pair_set",
    "pair_summary",
    "put_bytes",
    "put_json",
    "put_json_batch",
    "put_value",
    "put_values_batch",
    "read_absolute_key",
    "scan_all",
    "scan_page",
    "scan_prefix",
]

#: The server's own cap on one ``PAIR_PUT_BATCH`` request (``pair_batch.go``).
PAIR_PUT_BATCH_MAX_ITEMS = 10000

DEFAULT_SCAN_LIMIT = 500


def _raise(conn_command: str, response: Response) -> None:
    raise CheetahError(
        f"cheetah {conn_command} failed: {response.reason}",
        command=conn_command,
        response=response,
    )


def _assert_payload(payload: str) -> str:
    if "\n" in payload or "\r" in payload:
        raise CheetahError("cheetah payloads must not contain a newline")
    if payload == "":
        raise CheetahError("cheetah payloads must not be empty (INSERT answers missing_value)")
    return payload


# --------------------------------------------------------------------------- #
# Value layer
# --------------------------------------------------------------------------- #
def insert(conn: Any, payload: str | bytes) -> int:
    """Store bytes and return their absolute key."""
    text = _assert_payload(payload if isinstance(payload, str) else payload.decode("utf-8"))
    response = conn.send(f"INSERT:{len(text.encode('utf-8'))} {text}")
    if not response.ok:
        _raise("INSERT", response)
    key = response.int_field("key")
    if key is None:
        raise CheetahError(f"cheetah INSERT returned no key: {response.raw}", response=response)
    return key


def edit(conn: Any, abs_key: int, payload: str | bytes) -> None:
    """Overwrite the value under an existing key; the key stays valid."""
    text = _assert_payload(payload if isinstance(payload, str) else payload.decode("utf-8"))
    response = conn.send(f"EDIT {int(abs_key)} {text}")
    if not response.ok:
        _raise(f"EDIT {abs_key}", response)


def read_absolute_key(conn: Any, abs_key: int) -> str | None:
    """Hydrate the bytes behind an absolute key. ``None`` when deleted."""
    response = conn.send(build_command("READ", int(abs_key)))
    if response.ok:
        return response.fields.get("value", "")
    if response.error and response.error.startswith("key_not_found"):
        return None
    _raise(f"READ {abs_key}", response)
    return None  # pragma: no cover - _raise always raises


def delete_value(conn: Any, abs_key: int) -> bool:
    """Tombstone a value. Any pair name pointing at it is **not** removed."""
    response = conn.send(f"DEL values key={int(abs_key)}")
    if response.ok:
        return True
    if response.error and "not_found" in response.error:
        return False
    _raise(f"DEL values {abs_key}", response)
    return False  # pragma: no cover - _raise always raises


# --------------------------------------------------------------------------- #
# Name layer
# --------------------------------------------------------------------------- #
def pair_set(conn: Any, key: str | bytes, abs_key: int, *, hidden: bool = False) -> None:
    """Bind a byte prefix to a value key. Upserts."""
    command = "PAIR_SET_HIDDEN" if hidden else "PAIR_SET"
    response = conn.send(build_command(command, key, int(abs_key)))
    if not response.ok:
        _raise(command, response)


def pair_get(conn: Any, key: str | bytes) -> int | None:
    """Resolve exactly one name to its key. A point lookup, never a scan."""
    response = conn.send(build_command("PAIR_GET", key))
    if response.ok:
        return response.int_field("key")
    if response.error and response.error.startswith("not_found"):
        return None
    _raise("PAIR_GET", response)
    return None  # pragma: no cover - _raise always raises


get_absolute_key = pair_get


def delete_pair(conn: Any, key: str | bytes) -> int:
    """Unbind one name. Returns the number of pairs removed (0 when absent).

    The key travels through :func:`encode_argument` because ``DEL`` speaks the
    micro dialect, which splits on whitespace: a key holding a space, or
    starting with ``x``, survives only in its ``x<hex>`` spelling.
    """
    response = conn.send(f"DEL pairs key={encode_argument(key)}")
    if response.ok:
        deleted = response.int_field("deleted")
        return 1 if deleted is None else deleted
    if response.error and "not_found" in response.error:
        return 0
    _raise("DEL pairs", response)
    return 0  # pragma: no cover - _raise always raises


def pair_purge(
    conn: Any,
    prefix: str | bytes | None = None,
    *,
    limit: int = 0,
    payloads: bool = True,
) -> int:
    """Unbind a whole namespace, deleting its payloads unless ``payloads=False``.

    ``prefix=None`` means the whole trie. Looping happens inside the server, so
    a namespace wipe is seconds rather than thousands of round trips; prefer
    ``RESET_DB`` when the entire database is disposable.
    """
    fields: dict[str, Any] = {"prefix": "*" if prefix in (None, b"", "", "*") else encode_argument(prefix)}
    if limit > 0:
        fields["limit"] = int(limit)
    if not payloads:
        fields["payloads"] = 0
    response = conn.send(build_key_value_command("DEL pairs", fields))
    if not response.ok:
        _raise("DEL pairs", response)
    return response.int_field("deleted", 0) or 0


# --------------------------------------------------------------------------- #
# Records: the two steps as one call
# --------------------------------------------------------------------------- #
def put_value(conn: Any, key: str | bytes, payload: str | bytes, *, upsert: bool = False) -> int:
    """Bind ``key`` to ``payload``.

    ``upsert=False`` (the default, and what a bulk ingest wants) is the
    two-round-trip blind write: INSERT then PAIR_SET. Rebinding a name this way
    leaves the old value readable by its own absolute key — harmless for
    write-once rows, wasteful for records that are rewritten, which is what
    ``upsert=True`` is for: it EDITs the existing value in place and keeps the
    absolute key stable.
    """
    if upsert:
        existing = pair_get(conn, key)
        if existing is not None:
            edit(conn, existing, payload)
            return existing
    abs_key = insert(conn, payload)
    pair_set(conn, key, abs_key)
    return abs_key


def get_value(conn: Any, key: str | bytes) -> str | None:
    """Read the payload bound to ``key``, or ``None`` when the name is unbound."""
    abs_key = pair_get(conn, key)
    if abs_key is None:
        return None
    return read_absolute_key(conn, abs_key)


def put_json(conn: Any, key: str | bytes, value: Any, *, upsert: bool = False) -> int:
    return put_value(conn, key, json.dumps(value, separators=(",", ":")), upsert=upsert)


def get_json(conn: Any, key: str | bytes) -> Any:
    raw = get_value(conn, key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CheetahError(f"cheetah value at {key!r} is not JSON: {exc}") from exc


def put_bytes(conn: Any, key: str | bytes, payload: bytes, *, upsert: bool = False) -> int:
    """Bind ``key`` to arbitrary bytes, base64-wrapped for the text protocol."""
    return put_value(conn, key, base64.b64encode(payload).decode("ascii"), upsert=upsert)


def get_bytes(conn: Any, key: str | bytes) -> bytes | None:
    """The inverse of :func:`put_bytes`."""
    raw = get_value(conn, key)
    if raw is None:
        return None
    try:
        return base64.b64decode(raw.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise CheetahError(f"cheetah value at {key!r} is not base64: {exc}") from exc


def put_values_batch(
    conn: Any,
    entries: Sequence[tuple[Any, Any]] | Sequence[dict],
    *,
    hidden: bool = False,
    continue_on_error: bool = False,
    want_keys: bool = False,
) -> list[int | None]:
    """Store and bind many pairs in **one** round trip (``PAIR_PUT_BATCH``).

    :func:`put_value` is two requests per record — INSERT then PAIR_SET — which
    is the right separation but the wrong request count for a bulk ingest.
    ``PAIR_PUT_BATCH`` does both server-side for a whole page of records.

    Note what that does and does not buy: the server-side work per record is
    unchanged, so a client that already pipelines its writes across connections
    sees no throughput gain. It is worth reaching for when the client cannot
    pipeline, when the link has latency, or to keep request/parse overhead down.

    It is **not** a transaction. Items are independent and applied in order, so
    a failure part-way leaves the earlier ones written; the server reports
    ``applied``/``failed`` and this raises on any failure rather than returning
    a count nobody checks. A half-written batch is an index with holes in it.

    Returns the assigned absolute keys when ``want_keys`` is set (``None`` for a
    failed item), otherwise an empty list — a large batch should not pay for
    keys that write-once rows never read.
    """
    items = _batch_items(entries)
    if not items:
        return []
    if len(items) > PAIR_PUT_BATCH_MAX_ITEMS:
        raise CheetahError(
            f"cheetah PAIR_PUT_BATCH accepts at most {PAIR_PUT_BATCH_MAX_ITEMS} items, "
            f"got {len(items)}; page the write"
        )
    encoded = base64.b64encode(
        json.dumps(items, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    fields: dict[str, Any] = {"items": encoded}
    if hidden:
        fields["hidden"] = 1
    if continue_on_error:
        fields["continue_on_error"] = 1
    if want_keys:
        fields["keys"] = 1
    response = conn.send(build_key_value_command("PAIR_PUT_BATCH", fields))
    if not response.ok:
        _raise(f"PAIR_PUT_BATCH of {len(items)}", response)
    applied = response.int_field("applied", 0) or 0
    failed = response.int_field("failed", 0) or 0
    if applied != len(items) or failed > 0:
        raise CheetahError(
            f"cheetah PAIR_PUT_BATCH applied {applied}/{len(items)} (failed={failed}): "
            f"{response.field_value('first_error') or 'no reason reported'}",
            response=response,
        )
    if not want_keys:
        return []
    assigned = response.payload() or []
    return [None if value is None else int(value) for value in assigned]


def _batch_items(entries: Iterable[Any]) -> list[dict[str, str]]:
    """``{"k": …, "v": …}`` items, each field in its safe argument spelling.

    Both fields go through ``parseValue`` on the server, so the same ``x<hex>``
    rule as a positional argument applies — and must be applied: a base64
    payload that happens to start with ``x`` would otherwise be read as hex.
    """
    items: list[dict[str, str]] = []
    for entry in entries:
        if isinstance(entry, dict):
            key, value = entry["key"], entry["payload"]
        else:
            key, value = entry
        key_bytes = to_bytes(key)
        value_bytes = value if isinstance(value, (bytes, bytearray)) else to_bytes(value)
        if not key_bytes:
            raise CheetahError("cheetah PAIR_PUT_BATCH item key must not be empty")
        if not value_bytes:
            raise CheetahError("cheetah PAIR_PUT_BATCH item value must not be empty")
        items.append({"k": _batch_field(key_bytes), "v": _batch_field(bytes(value_bytes))})
    return items


def _batch_field(data: bytes) -> str:
    # Inside the JSON blob a space is harmless (the whole blob is base64), so the
    # only spellings that must be escaped are the ones parseValue misreads.
    if data[0:1] == b"x" or any(byte < 0x20 or byte > 0x7E for byte in data):
        return "x" + data.hex()
    return data.decode("latin1")


def put_json_batch(conn: Any, entries: Sequence[Any], **options: Any) -> list[int | None]:
    """:func:`put_values_batch` for JSON payloads."""
    prepared = []
    for entry in entries:
        if isinstance(entry, dict):
            key, payload = entry["key"], entry["payload"]
        else:
            key, payload = entry
        prepared.append((key, json.dumps(payload, separators=(",", ":"))))
    return put_values_batch(conn, prepared, **options)


# --------------------------------------------------------------------------- #
# Reading a namespace
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ScanPage:
    items: tuple[ScanItem, ...]
    cursor: str | None
    response: Response


def scan_page(
    conn: Any,
    prefix: str | bytes | None = None,
    *,
    limit: int = DEFAULT_SCAN_LIMIT,
    cursor: str | None = None,
    reducer: str | None = None,
    include_hidden: bool = False,
) -> ScanPage:
    """One page of a ``PAIR_SCAN`` (or ``PAIR_REDUCE``, with ``reducer``).

    The cursor is handed back **verbatim** through
    :class:`~cheetah_db.protocol.RawArgument`. Encoding it would hex-encode the
    ``x<hex>`` token Cheetah returned, and the server would resume from a prefix
    that does not exist — a sweep that quietly stops after the first page
    instead of failing.
    """
    target = "*" if prefix in (None, b"", "") else prefix
    options = "include_hidden=1" if include_hidden else None
    cursor_arg = RawArgument(cursor) if cursor else None
    if reducer:
        line = build_command("PAIR_REDUCE", reducer, target, limit, cursor_arg, options)
    else:
        line = build_command("PAIR_SCAN", target, limit, cursor_arg, options)
    response = conn.send(line)
    if not response.ok:
        _raise(f"scan of {target!r}", response)
    return ScanPage(items=tuple(response.items()), cursor=response.cursor(), response=response)


def scan_prefix(
    conn: Any,
    prefix: str | bytes | None = None,
    *,
    limit: int = DEFAULT_SCAN_LIMIT,
    max_items: int | None = None,
    reducer: str | None = None,
    include_hidden: bool = False,
) -> Iterator[ScanItem]:
    """Every entry under ``prefix``, one page at a time.

    ``max_items`` bounds a sweep that would otherwise be unbounded — a hot
    namespace holds thousands of rows.
    """
    cursor: str | None = None
    yielded = 0
    while True:
        page = scan_page(
            conn,
            prefix,
            limit=limit,
            cursor=cursor,
            reducer=reducer,
            include_hidden=include_hidden,
        )
        for item in page.items:
            if max_items is not None and yielded >= max_items:
                return
            yielded += 1
            yield item
        cursor = page.cursor
        if not cursor:
            return


def scan_all(conn: Any, prefix: str | bytes | None = None, **options: Any) -> list[ScanItem]:
    """:func:`scan_prefix` collected into a list."""
    return list(scan_prefix(conn, prefix, **options))


@dataclass(frozen=True)
class PairSummary:
    """``PAIR_SUMMARY`` for one prefix: how much is under here, and where.

    ``payload_bytes`` is a **payload-retention** signal, not disk usage: the
    server reports it without hydrating values, and it excludes trie, table and
    filesystem overhead. It is the right input to a retention budget and the
    wrong answer to "how big is this directory".
    """

    prefix: str
    count: int
    payload_bytes: int
    min_payload_bytes: int | None
    max_payload_bytes: int | None
    max_depth: int | None
    branches: tuple[Branch, ...]
    response: Response


def pair_summary(
    conn: Any,
    prefix: str | bytes | None = None,
    *,
    depth: int = 1,
    branch_limit: int | None = None,
    include_hidden: bool = False,
) -> PairSummary:
    target = "*" if prefix in (None, b"", "") else prefix
    args: list[Any] = [target, depth]
    if branch_limit is not None:
        args.append(branch_limit)
    if include_hidden:
        args.append("include_hidden=1")
    response = conn.send(build_command("PAIR_SUMMARY", *args))
    if not response.ok:
        _raise(f"PAIR_SUMMARY {target!r}", response)
    return PairSummary(
        prefix=target if isinstance(target, str) else target.decode("utf-8", "replace"),
        count=int(numeric_field(response.fields, "count", 0) or 0),
        payload_bytes=int(numeric_field(response.fields, "total_payload_bytes", 0) or 0),
        min_payload_bytes=response.int_field("min_payload_bytes"),
        max_payload_bytes=response.int_field("max_payload_bytes"),
        max_depth=response.int_field("max_depth"),
        branches=tuple(parse_branches(response.fields.get("branches"))),
        response=response,
    )

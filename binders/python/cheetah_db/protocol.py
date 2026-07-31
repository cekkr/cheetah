"""Cheetah wire-protocol codec.

Pure functions only: nothing here opens a socket, reads a setting, or keeps
state. Everything Cheetah says is one line; everything we say is one line. The
authority for the formats reproduced here is this repository's own handbook
(``AGENTS.md``) and the ``ExecuteCommand`` switch in ``src/database.go`` — never
document Cheetah behavior from memory.

Response grammar, as emitted by the server::

    SUCCESS[,<field>]*            e.g. SUCCESS,pair_set
                                       SUCCESS,key=42
                                       SUCCESS,count=2,items=<hex>:<key>;<hex>:<key>
                                       SUCCESS,reducer=counts,count=1,items=<hex>:<key>:<b64>
                                       SUCCESS,command=GRAPH_RECALL,count=4,payload=<b64>
    ERROR,<reason>                e.g. ERROR,not_found
                                       ERROR,value_size_mismatch (expected 16, got 17)
    PENDING,<field>*              a job that has not finished yet

A field is either ``key=value`` or a bare flag (``pair_set``). Fields are comma
separated, EXCEPT that ``value=`` (the READ payload) is raw bytes and runs to
the end of the line — it may legitimately contain commas.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "RESERVED_CONTROL_PREFIXES",
    "RESERVED_TEXT_PREFIXES",
    "Branch",
    "RawArgument",
    "Response",
    "ScanItem",
    "assert_unreserved_key",
    "build_command",
    "build_key_value_command",
    "decode_hex_key",
    "decode_item_payload",
    "decode_payload",
    "decode_transport_payload",
    "encode_argument",
    "encode_json_argument",
    "is_bare_safe_argument",
    "join_csv",
    "numeric_field",
    "parse_branches",
    "parse_cursor",
    "parse_items",
    "parse_response",
    "raw_argument",
    "to_bytes",
]

#: ``\x01gn:`` … ``\x05gt:`` — the graph store's reserved first bytes.
RESERVED_CONTROL_PREFIXES = (0x01, 0x02, 0x03, 0x04, 0x05)

#: Reserved textual namespaces (``graph.go`` → ``graph/``, ``idx/``).
RESERVED_TEXT_PREFIXES = ("graph/", "idx/")


def to_bytes(value: Any) -> bytes:
    """Coerce a key/value argument to the bytes that will travel on the wire."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


def is_bare_safe_argument(data: bytes) -> bool:
    """True when ``data`` can travel as a bare positional argument.

    Cheetah splits command arguments on whitespace and decodes a leading ``x``
    as hex (``src/helpers.go`` → ``parseValue``), so anything with a space, a
    non-printable byte, a leading ``x``, or the wildcard spelling must be
    escaped.
    """
    if not data:
        return False
    if data[0] == 0x78:  # 'x'
        return False
    if data == b"*":
        return False
    return all(0x21 <= byte <= 0x7E for byte in data)


def encode_argument(value: Any) -> str:
    """Encode a byte string as a Cheetah positional argument.

    Raw when it is unambiguous, ``x<HEX>`` otherwise. Always round-trips.
    """
    if isinstance(value, RawArgument):
        return value.text
    data = to_bytes(value)
    if is_bare_safe_argument(data):
        return data.decode("latin1")
    return "x" + data.hex()


def assert_unreserved_key(key: Any) -> bytes:
    """Raise when a key would collide with a namespace the server reserves.

    (``\\x01gn:`` … ``\\x05gt:``, ``graph/``, ``idx/``.) A client's own prefixes
    normally hold this by construction; this is the assertion that keeps it
    true.
    """
    data = to_bytes(key)
    if not data:
        raise ValueError("cheetah key must not be empty")
    if data[0] in RESERVED_CONTROL_PREFIXES:
        raise ValueError(
            f"cheetah key uses a reserved control-byte prefix: 0x{data[0]:02x}"
        )
    text = data.decode("latin1")
    for prefix in RESERVED_TEXT_PREFIXES:
        if text.startswith(prefix):
            raise ValueError(f"cheetah key uses a reserved namespace: {text[:8]}")
    return data


class RawArgument:
    """An argument already in Cheetah's own spelling, to travel verbatim.

    The only real case is a ``next_cursor`` token: it comes back as ``x<hex>``,
    and running it through :func:`encode_argument` would hex-encode the hex —
    the server would then resume from a prefix that does not exist and silently
    return one page instead of the whole sweep.
    """

    __slots__ = ("text",)

    def __init__(self, text: Any) -> None:
        self.text = text if isinstance(text, str) else str(text)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.text

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"RawArgument({self.text!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RawArgument) and other.text == self.text


def raw_argument(text: Any) -> RawArgument:
    return RawArgument(text)


def build_command(command: str, *args: Any) -> str:
    """Build one command line. Arguments are encoded, the line is not terminated."""
    parts = [str(command)]
    for arg in args:
        if arg is None or arg == "":
            continue
        if isinstance(arg, RawArgument):
            parts.append(arg.text)
        elif isinstance(arg, bool):
            parts.append("1" if arg else "0")
        elif isinstance(arg, (int, float)):
            parts.append(str(arg))
        else:
            parts.append(encode_argument(arg))
    line = " ".join(parts)
    if "\n" in line or "\r" in line:
        raise ValueError("cheetah command must not contain a newline")
    return line


def build_key_value_command(command: str, fields: Mapping[str, Any] | None) -> str:
    """``key=value`` tokens, the dialect the ``GRAPH_*``/``PREDICT_*`` families speak.

    The server splits these on whitespace (``parseKeyValueArgs`` uses
    ``strings.Fields``), so a value may never contain a space; anything
    free-form travels base64 through :func:`encode_json_argument`. The split is
    on the *first* ``=``, so base64 padding inside a value is safe.
    """
    parts = [str(command)]
    for key, value in (fields or {}).items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            rendered = "1" if value else "0"
        elif isinstance(value, RawArgument):
            rendered = value.text
        elif isinstance(value, (bytes, bytearray, memoryview)):
            rendered = encode_argument(value)
        else:
            rendered = str(value)
        if " " in rendered or "\t" in rendered:
            raise ValueError(
                f"cheetah {command} value for {key}= must not contain whitespace; "
                "encode it (base64 or x<hex>) first"
            )
        parts.append(f"{key}={rendered}")
    line = " ".join(parts)
    if "\n" in line or "\r" in line:
        raise ValueError("cheetah command must not contain a newline")
    return line


def encode_json_argument(value: Any) -> str:
    """JSON → the base64 spelling a ``key=value`` token can carry."""
    encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return base64.b64encode(encoded.encode("utf-8")).decode("ascii")


@dataclass(frozen=True)
class ScanItem:
    """One entry of an ``items=`` field.

    ``PAIR_SCAN`` emits ``<hexKey>:<absKey>``; ``PAIR_REDUCE`` adds a third
    component, the hydrated payload in base64.
    """

    key_hex: str
    key: bytes
    abs_key: int | None
    abs_key_raw: str
    payload_base64: str | None = None

    @property
    def payload(self) -> bytes | None:
        """The reducer payload, base64 removed. ``None`` when absent/invalid."""
        return decode_item_payload(self)

    @property
    def text(self) -> str:
        """The key as UTF-8 text, for the common case of printable keys."""
        return self.key.decode("utf-8", "replace")


@dataclass(frozen=True)
class Branch:
    """One entry of a ``branches=`` histogram from ``PAIR_SUMMARY``."""

    byte_hex: str
    byte: int
    count: int


@dataclass(frozen=True)
class Response:
    """A parsed response line.

    ``fields`` holds the ``key=value`` tokens; ``flags`` collects bare tokens
    such as the ``pair_set`` in ``SUCCESS,pair_set``. ``raw`` is the line as it
    arrived, which is what belongs in an error message.
    """

    raw: str
    status: str
    fields: dict[str, str] = field(default_factory=dict)
    flags: tuple[str, ...] = ()
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "SUCCESS"

    @property
    def pending(self) -> bool:
        return self.status == "PENDING"

    @property
    def unreachable(self) -> bool:
        """True for the placeholder a client returns when nothing came back."""
        return self.status == ""

    @property
    def reason(self) -> str:
        return self.error or self.raw or "no response"

    def has(self, name: str) -> bool:
        return name in self.fields

    def field_value(self, name: str, default: str | None = None) -> str | None:
        value = self.fields.get(name)
        return default if value is None or value == "" else value

    def int_field(self, name: str, default: int | None = None) -> int | None:
        return _coerce_int(self.fields.get(name), default)

    def float_field(self, name: str, default: float | None = None) -> float | None:
        return _coerce_float(self.fields.get(name), default)

    def bool_field(self, name: str, default: bool = False) -> bool:
        raw = self.fields.get(name)
        if raw is None or raw == "":
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def payload(self) -> Any:
        """The ``payload=<base64>`` field decoded from JSON, or ``None``."""
        return decode_payload(self.fields)

    def items(self) -> list[ScanItem]:
        return parse_items(self.fields.get("items"))

    def branches(self) -> list[Branch]:
        return parse_branches(self.fields.get("branches"))

    def cursor(self) -> str | None:
        return parse_cursor(self.fields)


def _coerce_int(raw: str | None, default: int | None) -> int | None:
    if raw is None or raw == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        try:
            return int(float(raw.strip()))
        except ValueError:
            return default


def _coerce_float(raw: str | None, default: float | None) -> float | None:
    if raw is None or raw == "":
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def parse_response(line: str | bytes | None) -> Response:
    """Split a response line into a :class:`Response`.

    ``None`` — what a client returns when the socket gave nothing back — parses
    to a response with an empty status, so callers can classify a dead
    connection the same way they classify ``ERROR``.
    """
    if line is None:
        return Response(raw="", status="", error="no_response")
    if isinstance(line, (bytes, bytearray)):
        line = bytes(line).decode("utf-8", "replace")
    raw = line.rstrip("\r\n")
    head, sep, rest = raw.partition(",")
    status = head.strip()

    if status == "ERROR":
        # The reason runs to end of line: it may contain commas and spaces.
        return Response(raw=raw, status=status, error=rest)

    fields: dict[str, str] = {}
    flags: list[str] = []
    cursor = 0
    while sep and cursor <= len(rest):
        next_comma = rest.find(",", cursor)
        token = rest[cursor:] if next_comma == -1 else rest[cursor:next_comma]
        equals = token.find("=")
        if equals == -1:
            if token:
                flags.append(token)
        else:
            key = token[:equals]
            if key == "value":
                # READ answers `SUCCESS,size=<n>,value=<raw bytes>`; the bytes
                # are unescaped and always last, so they own the rest of line.
                fields["value"] = rest[cursor + equals + 1 :]
                break
            fields[key] = token[equals + 1 :]
        if next_comma == -1:
            break
        cursor = next_comma + 1

    return Response(raw=raw, status=status, fields=fields, flags=tuple(flags))


def decode_hex_key(text: str | None) -> bytes:
    """Decode ``<hex>`` → bytes. Tolerates the ``x<hex>`` argument spelling."""
    if not text:
        return b""
    body = text[1:] if text[0] in "xX" else text
    try:
        return bytes.fromhex(body)
    except ValueError:
        return b""


def parse_items(items_field: str | None) -> list[ScanItem]:
    """Split an ``items=`` field into :class:`ScanItem` records."""
    if not items_field:
        return []
    parsed: list[ScanItem] = []
    for entry in items_field.split(";"):
        if not entry:
            continue
        parts = entry.split(":")
        key_hex = parts[0]
        abs_key_raw = parts[1] if len(parts) > 1 else ""
        parsed.append(
            ScanItem(
                key_hex=key_hex,
                key=decode_hex_key(key_hex),
                abs_key=_coerce_int(abs_key_raw, None),
                abs_key_raw=abs_key_raw,
                payload_base64=":".join(parts[2:]) if len(parts) > 2 else None,
            )
        )
    return parsed


def parse_cursor(fields: Mapping[str, str] | None) -> str | None:
    """``next_cursor`` as an opaque token to hand back verbatim.

    ``None`` when the sweep is exhausted: Cheetah spells exhaustion either by
    omitting the field or by answering ``*``.
    """
    cursor = (fields or {}).get("next_cursor")
    if not cursor or cursor == "*":
        return None
    return cursor


def decode_payload(fields: Mapping[str, str] | None) -> Any:
    """Decode a ``payload=<base64>`` field into JSON. ``None`` when absent."""
    payload = (fields or {}).get("payload")
    if not payload:
        return None
    try:
        text = base64.b64decode(payload).decode("utf-8")
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(f"cheetah payload is not decodable: {exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"cheetah payload is not JSON: {exc}") from exc


def decode_item_payload(item: ScanItem | None) -> bytes | None:
    """Decode one ``items=`` payload component (base64) into raw bytes."""
    if item is None or not item.payload_base64:
        return None
    try:
        return base64.b64decode(item.payload_base64, validate=True)
    except (ValueError, binascii.Error):
        return None


def decode_transport_payload(payload: bytes | None) -> bytes | None:
    """Undo a client-side base64 storage layer applied before ``INSERT``.

    Binary payloads cannot travel raw on a newline-delimited text protocol, so a
    client that stores bytes wraps them (see :func:`cheetah_db.kv.put_bytes`).
    A reducer hands those stored bytes back still wrapped: the transport layer
    comes off first, the application's own format second.
    """
    if payload is None:
        return None
    try:
        return base64.b64decode(payload, validate=True)
    except (ValueError, binascii.Error):
        return None


def parse_branches(branches_field: str | None) -> list[Branch]:
    """``branches=<hexByte>:<count>;…`` from ``PAIR_SUMMARY``.

    The fan-out histogram that says whether a prefix is worth scanning.
    """
    if not branches_field:
        return []
    parsed: list[Branch] = []
    for entry in branches_field.split(";"):
        if not entry:
            continue
        byte_hex, _, count = entry.partition(":")
        try:
            byte_value = int(byte_hex, 16)
        except ValueError:
            continue
        parsed.append(
            Branch(byte_hex=byte_hex, byte=byte_value, count=_coerce_int(count, 0) or 0)
        )
    return parsed


def numeric_field(
    fields: Mapping[str, str] | None,
    name: str,
    default: float | int | None = None,
) -> float | int | None:
    """Numeric field accessor that keeps ``0`` and rejects garbage."""
    raw = (fields or {}).get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return int(value) if value.is_integer() else value


def join_csv(values: Iterable[Any] | None) -> str | None:
    """``a,b,c`` for the comma-separated list arguments (labels, seeds, …)."""
    if values is None:
        return None
    if isinstance(values, (str, bytes)):
        return values if isinstance(values, str) else values.decode("utf-8")
    rendered = [str(value).strip() for value in values]
    kept: Sequence[str] = [value for value in rendered if value]
    return ",".join(kept) if kept else None

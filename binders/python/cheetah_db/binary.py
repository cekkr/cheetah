"""Cheetah binary-protocol codec.

The binary protocol carries the *same* commands as the text one: the command
name becomes a 2-byte index and every value travels in its own type — an
integer as an integer, a float as a float, bytes as bytes — instead of all of
them as text. The authority is ``src/binary_protocol.go``; nothing here may be
documented from memory.

The single most important property, and the reason this module is small: the
server decodes a request frame into the **canonical command line** and
re-encodes the answer line into a response frame. So a binary client is a
transcoder, not a second command surface — every command layer in this binder
(``kv``, ``graph``, ``records``, ``predict``, ``admin``) keeps building text
lines, and :func:`encode_command_line` turns them into frames. Add a command to
the server and nothing here needs editing.

The corollary is that the canonical line stays the contract. A ``key=value``
value may not contain whitespace in binary mode either; what binary adds is
that the *typed* escape hatch is free — a key travels as ``bytes`` and the
server renders it back as ``x<hex>``, which is what a text client would have
had to spell out by hand.

Frame layout::

    0xC7            magic. No text command starts with this byte, which is how
                    the server tells the two modes apart from byte one.
    u8              frame type
    u32be           body length
    body

Two things a client cannot know on its own: the **command index**, which changes
with the server's command inventory, and a **table's numeric widths**, which are
a property of the database. Both are published by the ``ALIAS`` command
(:mod:`cheetah_db.alias`) — and the index and the argument-key dictionary
additionally arrive whole in the handshake ack, because a response names its
fields by index and a client without the dictionary could not read even the
answer to ``ALIAS keys``. Digests come with them, so a cached copy is verified
in sixteen characters rather than refetched.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "ARGUMENT_KEYS_ENUM",
    "COMMANDS_ENUM",
    "COMMAND_KINDS",
    "DEFAULT_WIDTHS",
    "FRAME_HEADER_BYTES",
    "FRAME_MAGIC",
    "FrameType",
    "HandshakeAck",
    "KeyMode",
    "Kind",
    "PROTOCOL_VERSION",
    "Status",
    "BinaryProtocolError",
    "BinarySession",
    "canonical_number",
    "decode_handshake_ack",
    "decode_response",
    "encode_command_line",
    "encode_frame",
    "encode_handshake",
    "encode_request",
    "encode_value",
    "minimal_width",
    "read_frame",
    "type_token",
]

FRAME_MAGIC = 0xC7
FRAME_HEADER_BYTES = 6
PROTOCOL_VERSION = 1
MAX_BODY_BYTES = 16 << 20


class FrameType:
    """Frame types."""

    HANDSHAKE = 0x01
    HANDSHAKE_ACK = 0x02
    REQUEST = 0x03
    RESPONSE = 0x04


class Kind:
    """Value types. The tag byte is ``kind << 4 | width``; width 0 = default."""

    STRING = 0x0
    BYTES = 0x1
    UINT = 0x2
    INT = 0x3
    FLOAT = 0x4
    BOOL = 0x5
    ENUM = 0x6
    NULL = 0x7


class KeyMode:
    """How an argument names itself."""

    POSITIONAL = 0x00
    INDEXED = 0x01
    INLINE = 0x02


class Status:
    """Response status codes."""

    OTHER = 0x00
    SUCCESS = 0x01
    ERROR = 0x02
    PENDING = 0x03


#: Enumeration families a value of type ``ENUM`` can index into.
COMMANDS_ENUM = 0x01
ARGUMENT_KEYS_ENUM = 0x02

#: Command-kind codes, as the ack spells them. Descriptive only.
COMMAND_KINDS = {1: "micro", 2: "alias", 3: "builtin", 4: "engine", 5: "frontend"}

STATUS_WORDS = {
    Status.SUCCESS: "SUCCESS",
    Status.ERROR: "ERROR",
    Status.PENDING: "PENDING",
    Status.OTHER: "",
}

#: Server-side defaults, mirrored so a session works before any negotiation.
DEFAULT_WIDTHS = {"uint": 8, "int": 8, "float": 8}

_UINT_RE = re.compile(r"^(0|[1-9][0-9]*)$")
_INT_RE = re.compile(r"^-(0|[1-9][0-9]*)$")
_FLOAT_RE = re.compile(r"^-?(0|[1-9][0-9]*)\.[0-9]+$")
_HEX_RE = re.compile(r"^x([0-9a-fA-F]{2})+$")
# A token is read as ``key=value`` only when its head looks like a modifier
# name. Without this an ``INSERT`` payload carrying base64 padding — or any
# ``=`` at all — would be cut into a 500-character "key". The rule is lowercase
# on purpose: the server lower-cases an argument name, so a head that is
# already lowercase re-renders identically whichever way it was read, which is
# what keeps the transcoding exact in the ambiguous cases.
_ARG_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class BinaryProtocolError(Exception):
    """A frame that cannot be built or read."""


@dataclass
class HandshakeAck:
    """What the server answers to a handshake: widths, identity, both tables."""

    version: int
    widths: dict[str, int]
    flags: int
    epoch: int
    digest: str
    keys_digest: str
    commands: list[dict[str, Any]] = field(default_factory=list)
    keys: list[dict[str, Any]] = field(default_factory=list)


class BinarySession:
    """Everything a connection negotiated or looked up.

    The numeric widths, the command index, the argument-key dictionary, and the
    digests that say whether a cached copy of either is still valid.

    A session works with an empty index — commands then travel by name, which
    costs bytes but never correctness.
    """

    def __init__(
        self,
        widths: Mapping[str, int] | None = None,
        version: int = PROTOCOL_VERSION,
    ) -> None:
        self.version = version
        self.widths = {**DEFAULT_WIDTHS, **(widths or {})}
        self.command_ids: dict[str, int] = {}
        self.command_names: dict[int, str] = {}
        self.key_ids: dict[str, int] = {}
        self.key_names: dict[int, str] = {}
        self.digest: str | None = None
        self.keys_digest: str | None = None
        self.epoch = 0
        # Per-table numeric widths, as reported by ``ALIAS profile table=…``.
        # Informational on this side: the server resolves the widths it will
        # use, and an explicit width in a tag always wins.
        self.table_profiles: dict[str, dict[str, int]] = {}

    def load_commands(
        self, entries: Iterable[Mapping[str, Any]], digest: str | None = None
    ) -> "BinarySession":
        self.command_ids = {}
        self.command_names = {}
        for entry in entries or ():
            name = str(entry["name"]).upper()
            self.command_ids[name] = int(entry["id"])
            self.command_names[int(entry["id"])] = name
        if digest is not None:
            self.digest = digest
        return self

    def load_keys(
        self, entries: Iterable[Mapping[str, Any]], digest: str | None = None
    ) -> "BinarySession":
        self.key_ids = {}
        self.key_names = {}
        for entry in entries or ():
            name = str(entry["name"]).lower()
            self.key_ids[name] = int(entry["id"])
            self.key_names[int(entry["id"])] = name
        if digest is not None:
            self.keys_digest = digest
        return self

    def load_profile(self, table: str, profile: Mapping[str, int]) -> "BinarySession":
        self.table_profiles[str(table)] = {**DEFAULT_WIDTHS, **profile}
        return self

    def widths_for(self, table: str | None) -> dict[str, int]:
        """A table's widths when known, the session defaults otherwise."""
        if table and str(table) in self.table_profiles:
            return self.table_profiles[str(table)]
        return self.widths

    def command_id(self, name: str) -> int | None:
        return self.command_ids.get(str(name).upper())

    def command_name(self, index: int) -> str | None:
        return self.command_names.get(index)

    def key_id(self, name: str) -> int | None:
        return self.key_ids.get(str(name).lower())

    def key_name(self, index: int) -> str | None:
        return self.key_names.get(index)

    def matches_digest(self, digest: str | None) -> bool:
        """True when a cached index still matches the server's."""
        return self.digest is not None and self.digest == digest

    def adopt(self, ack: HandshakeAck) -> "BinarySession":
        """Take everything the handshake ack delivered."""
        self.version = ack.version
        self.widths = dict(ack.widths)
        self.epoch = ack.epoch
        self.load_commands(ack.commands, ack.digest)
        self.load_keys(ack.keys, ack.keys_digest)
        return self


# --- frames ------------------------------------------------------------------


def encode_frame(frame_type: int, body: bytes) -> bytes:
    return bytes([FRAME_MAGIC, frame_type]) + struct.pack(">I", len(body)) + body


def read_frame(buffer: bytes) -> tuple[int, bytes, bytes] | None:
    """Pull one frame off the front of ``buffer``.

    Returns ``(type, body, rest)``, or ``None`` when the buffer does not hold a
    whole frame yet — a stream splitter calls this in a loop.
    """
    if len(buffer) < FRAME_HEADER_BYTES:
        return None
    if buffer[0] != FRAME_MAGIC:
        raise BinaryProtocolError(
            f"cheetah binary frame has a bad magic byte: 0x{buffer[0]:02x}"
        )
    length = struct.unpack(">I", buffer[2:6])[0]
    if length > MAX_BODY_BYTES:
        raise BinaryProtocolError(f"cheetah binary frame is too large: {length}")
    total = FRAME_HEADER_BYTES + length
    if len(buffer) < total:
        return None
    return buffer[1], buffer[FRAME_HEADER_BYTES:total], buffer[total:]


# --- handshake ---------------------------------------------------------------


def encode_handshake(uint: int = 0, int_: int = 0, float_: int = 0) -> bytes:
    """The first frame of a binary connection.

    A width of ``0`` means "whatever the server defaults to", which is how a
    client states a preference for one type without stating one for all three.
    """
    return encode_frame(
        FrameType.HANDSHAKE, bytes([PROTOCOL_VERSION, uint, int_, float_, 0])
    )


def _read_short(body: bytes, at: int) -> tuple[str, int]:
    length = body[at]
    return body[at + 1 : at + 1 + length].decode("utf-8"), at + 1 + length


def decode_handshake_ack(body: bytes) -> HandshakeAck:
    """Decode the widths, the index identity, and **both tables in full**.

    The tables have to arrive here rather than in a later ``ALIAS list``: a
    binary response names its fields by index, so a client without the
    argument-key dictionary could not read even the answer to ``ALIAS keys``.
    The ack is the one point in the conversation that can break that circle.
    """
    if len(body) < 13:
        raise BinaryProtocolError("cheetah handshake ack is truncated")
    ack = HandshakeAck(
        version=body[0],
        widths={"uint": body[1], "int": body[2], "float": body[3]},
        flags=body[4],
        epoch=struct.unpack(">Q", body[5:13])[0],
        digest="",
        keys_digest="",
    )
    at = 13
    ack.digest, at = _read_short(body, at)
    ack.keys_digest, at = _read_short(body, at)

    count = struct.unpack(">H", body[at : at + 2])[0]
    at += 2
    for _ in range(count):
        index = struct.unpack(">H", body[at : at + 2])[0]
        kind = COMMAND_KINDS.get(body[at + 2], "unknown")
        at += 3
        name, at = _read_short(body, at)
        ack.commands.append({"id": index, "kind": kind, "name": name})

    count = struct.unpack(">H", body[at : at + 2])[0]
    at += 2
    for _ in range(count):
        index = struct.unpack(">H", body[at : at + 2])[0]
        at += 2
        name, at = _read_short(body, at)
        ack.keys.append({"id": index, "name": name})
    return ack


# --- request encoding --------------------------------------------------------


def _short_string(text: str) -> bytes:
    raw = str(text).encode("utf-8")
    if len(raw) > 255:
        raise BinaryProtocolError(f"cheetah binary name is too long: {text}")
    return bytes([len(raw)]) + raw


def encode_value(spec: Mapping[str, Any], widths: Mapping[str, int] = DEFAULT_WIDTHS) -> bytes:
    """Encode one typed value.

    ``spec`` is ``{"type": …, "value": …, "width": …}`` where ``type`` is a
    :class:`Kind` name in lower case (``"uint"``, ``"bytes"``, …).

    A ``width`` of 0 (or none) declares "the width the server resolves" and
    writes exactly that many bytes, taken from ``widths`` — so ``widths`` must
    be what the server will resolve for this argument: the table's profile when
    one is declared, the session defaults otherwise. Getting that wrong is not a
    rounding error but a misread frame, which is why the transcoder never uses
    width 0 and states every width outright.
    """
    kind = spec["type"]
    declared = int(spec.get("width") or 0)

    if kind == "string":
        raw = str(spec["value"]).encode("utf-8")
        return bytes([Kind.STRING << 4]) + struct.pack(">I", len(raw)) + raw
    if kind == "bytes":
        value = spec["value"]
        raw = value if isinstance(value, (bytes, bytearray)) else str(value).encode("latin1")
        return bytes([Kind.BYTES << 4]) + struct.pack(">I", len(raw)) + bytes(raw)
    if kind == "uint":
        width = declared or widths["uint"]
        packed = struct.pack(">Q", int(spec["value"]))
        return bytes([(Kind.UINT << 4) | (declared & 0x0F)]) + packed[8 - width :]
    if kind == "int":
        width = declared or widths["int"]
        packed = struct.pack(">q", int(spec["value"]))
        return bytes([(Kind.INT << 4) | (declared & 0x0F)]) + packed[8 - width :]
    if kind == "float":
        width = declared or widths["float"]
        if width not in (4, 8):
            raise BinaryProtocolError("cheetah float width must be 4 or 8")
        packed = struct.pack(">f" if width == 4 else ">d", float(spec["value"]))
        return bytes([(Kind.FLOAT << 4) | (declared & 0x0F)]) + packed
    if kind == "bool":
        return bytes([Kind.BOOL << 4, 1 if spec["value"] else 0])
    if kind == "enum":
        family = int(spec.get("family") or COMMANDS_ENUM)
        return bytes([Kind.ENUM << 4, family]) + struct.pack(">H", int(spec["value"]))
    if kind == "null":
        return bytes([Kind.NULL << 4])
    raise BinaryProtocolError(f"cheetah unknown binary value type: {kind}")


def encode_request(
    command: str,
    args: Sequence[Mapping[str, Any]] = (),
    session: BinarySession | None = None,
    *,
    suffix: str | None = None,
    table: str | None = None,
) -> bytes:
    """Build a request frame from an explicit, typed description::

        encode_request("RECORD", [
            {"type": "string", "value": "set"},
            {"key": "table", "type": "string", "value": "ngram"},
            {"key": "cnt",   "type": "uint",   "value": 42, "width": 4},
        ], session)

    ``suffix`` carries the ``:<n>`` of ``INSERT:16``. An argument with no
    ``key`` is positional; ``{"type": "null"}`` is an omitted modifier and
    disappears from the line, which is how an optional field stays in a
    caller's dict.

    Widths left at 0 resolve against the table's profile, exactly as the server
    resolves them: a ``table=`` argument switches the resolution for every
    argument after it, which is why ``table=`` goes first.
    """
    name = str(command).upper()
    index = session.command_id(name) if session else None
    flags = 0
    head = b""
    if index is None:
        flags |= 0x01
        head += _short_string(name)
    else:
        head += struct.pack(">H", index)
    if suffix:
        flags |= 0x02
        head += _short_string(suffix)

    body = bytes([flags]) + head + struct.pack(">H", len(args))
    widths = session.widths_for(table) if session else DEFAULT_WIDTHS

    for arg in args:
        key = arg.get("key")
        if not key:
            body += bytes([KeyMode.POSITIONAL])
        else:
            key_index = session.key_id(key) if session else None
            if key_index is None:
                body += bytes([KeyMode.INLINE]) + _short_string(str(key).lower())
            else:
                body += bytes([KeyMode.INDEXED]) + struct.pack(">H", key_index)
        body += encode_value(arg, widths)
        # Mirrors the server: the table named in this frame governs the widths
        # of every argument after it.
        if session and key and str(key).lower() == "table" and arg.get("type") == "string":
            widths = session.widths_for(str(arg["value"]))
    return encode_frame(FrameType.REQUEST, body)


# --- transcoding a canonical line -------------------------------------------


def canonical_number(token: str) -> dict[str, Any] | None:
    """``123`` / ``-7`` / ``0.25`` exactly as they would be re-rendered.

    ``None`` for anything whose re-rendering would differ — ``007`` and ``1e3``
    are numbers, but recoding them would change the line, and the line is the
    contract.
    """
    if not token or len(token) > 32:
        return None
    if _UINT_RE.match(token):
        return {"type": "uint", "value": int(token)}
    if _INT_RE.match(token):
        return {"type": "int", "value": int(token)}
    if _FLOAT_RE.match(token):
        value = float(token)
        if repr(value) == token:
            return {"type": "float", "value": value}
    return None


def minimal_width(kind: str, value: int) -> int:
    """Smallest byte width that holds ``value``."""
    magnitude = value if kind == "uint" else abs(value) * 2
    if magnitude <= 0xFF:
        return 1
    if magnitude <= 0xFFFF:
        return 2
    if magnitude <= 0xFFFFFFFF:
        return 4
    return 8


def type_token(token: str) -> dict[str, Any]:
    """Classify one already-encoded token from a canonical line.

    ``x<hex>`` becomes real bytes (the server renders them straight back to
    ``x<hex>``, so the line is unchanged and the hex stops costing two
    characters per byte); a canonical number becomes a number; everything else
    stays a string.

    Every width is stated outright. The transcoder does not know which table an
    arbitrary line addresses, so it cannot predict what the server would resolve
    a width-0 tag to — and the nibble that states it is free.
    """
    if _HEX_RE.match(token):
        return {"type": "bytes", "value": bytes.fromhex(token[1:])}
    numeric = canonical_number(token)
    if numeric is not None:
        if numeric["type"] == "float":
            return {**numeric, "width": 8}
        return {**numeric, "width": minimal_width(numeric["type"], numeric["value"])}
    return {"type": "string", "value": token}


def encode_command_line(line: str, session: BinarySession | None = None) -> bytes:
    """Transcode a canonical command line into a request frame.

    This is what lets every command layer in this binder keep producing text and
    still speak binary. Splitting on every single space and letting the server
    re-join with one is exact even for a payload with runs of spaces: an empty
    token survives as an empty positional value and reappears as the space it
    was.
    """
    text = str(line)
    if "\n" in text or "\r" in text:
        raise BinaryProtocolError("cheetah command must not contain a newline")
    head, _, rest = text.partition(" ")
    command, colon, suffix = head.partition(":")

    args: list[dict[str, Any]] = []
    if rest:
        for token in rest.split(" "):
            key, equals, value = token.partition("=")
            if not equals or not _ARG_NAME_RE.match(key):
                args.append(type_token(token))
            else:
                args.append({"key": key, **type_token(value)})
    return encode_request(command, args, session, suffix=suffix if colon else None)


# --- response decoding -------------------------------------------------------


def _decode_value(
    body: bytes, at: int, widths: Mapping[str, int], session: BinarySession | None
) -> tuple[Any, str, bool, int]:
    """Returns ``(python value, text, skip, next offset)``."""
    tag = body[at]
    at += 1
    kind = tag >> 4
    width = tag & 0x0F

    if kind in (Kind.STRING, Kind.BYTES):
        length = struct.unpack(">I", body[at : at + 4])[0]
        at += 4
        raw = body[at : at + length]
        at += length
        if kind == Kind.BYTES:
            return raw, "x" + raw.hex(), False, at
        return raw.decode("utf-8", "replace"), raw.decode("utf-8", "replace"), False, at
    if kind == Kind.UINT:
        width = width or widths["uint"]
        value = int.from_bytes(body[at : at + width], "big")
        return value, str(value), False, at + width
    if kind == Kind.INT:
        width = width or widths["int"]
        value = int.from_bytes(body[at : at + width], "big", signed=True)
        return value, str(value), False, at + width
    if kind == Kind.FLOAT:
        width = width or widths["float"]
        value = struct.unpack(">f" if width == 4 else ">d", body[at : at + width])[0]
        return value, _format_float(value), False, at + width
    if kind == Kind.BOOL:
        value = body[at] != 0
        return value, "1" if value else "0", False, at + 1
    if kind == Kind.ENUM:
        family = body[at]
        index = struct.unpack(">H", body[at + 1 : at + 3])[0]
        at += 3
        name = (
            session.command_name(index)
            if family == COMMANDS_ENUM and session
            else session.key_name(index)
            if session
            else None
        )
        if name is None:
            raise BinaryProtocolError(
                f"cheetah unknown enum id {index} in family {family}"
            )
        return name, name, False, at
    if kind == Kind.NULL:
        return None, "", True, at
    raise BinaryProtocolError(f"cheetah unknown binary value type: {kind}")


def _format_float(value: float) -> str:
    """Go's ``strconv.FormatFloat(v, 'g', -1, 64)``.

    Python's ``repr`` is the same shortest round-trip form for every value a
    response carries.
    """
    return repr(value)


@dataclass
class BinaryResponse:
    """A decoded response frame.

    ``line`` is the canonical response line, byte for byte what a text
    connection would have received — which is what lets
    :func:`cheetah_db.protocol.parse_response` and every layer above stay
    untouched by the transport. ``values`` keeps the typed forms for a caller
    that would rather not re-parse them.
    """

    status: int
    line: str
    fields: dict[str, str] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)
    values: dict[str, Any] = field(default_factory=dict)


def decode_response(
    body: bytes,
    session: BinarySession | None = None,
    widths: Mapping[str, int] = DEFAULT_WIDTHS,
) -> BinaryResponse:
    """Decode a response frame body into its canonical line and its fields."""
    status = body[0]
    count = struct.unpack(">H", body[1:3])[0]
    at = 3
    result = BinaryResponse(status=status, line=STATUS_WORDS.get(status, ""))

    for _ in range(count):
        mode = body[at]
        at += 1
        key = ""
        if mode == KeyMode.INDEXED:
            index = struct.unpack(">H", body[at : at + 2])[0]
            at += 2
            resolved = session.key_name(index) if session else None
            if resolved is None:
                raise BinaryProtocolError(f"cheetah unknown argument index {index}")
            key = resolved
        elif mode == KeyMode.INLINE:
            key, at = _read_short(body, at)
        elif mode != KeyMode.POSITIONAL:
            raise BinaryProtocolError(f"cheetah unknown key mode {mode}")

        value, text, skip, at = _decode_value(body, at, widths, session)
        if skip:
            continue
        # ``value=`` is READ's raw payload: it left as bytes and goes back to
        # the line as bytes, unescaped, exactly as the text protocol delivers
        # it.
        if key == "value" and isinstance(value, (bytes, bytearray)):
            text = bytes(value).decode("latin1")
        if key:
            result.fields[key] = text
            result.values[key] = value
        else:
            result.flags.append(text)
        result.line += "," + (f"{key}={text}" if key else text)
    return result

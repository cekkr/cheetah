"""``ALIAS`` — the part of the protocol that describes the protocol.

Two things a client cannot derive on its own:

  - the **command index**, the 2-byte number the binary protocol puts on the
    wire in place of a command name. It is built from the server's own command
    inventory, so it changes when a command or an alias is added or removed;
    hard-coding it here would mean shipping a client that calls the wrong
    command after a server upgrade.
  - a table's **numeric widths**. They are a property of the database, not of
    the client: two processes writing the same table must encode it the same
    way, which only works if the widths live on the server.

So both are fetched, and both come with a digest to check a cached copy
against. :func:`alias_digest` is the cheap call — sixteen characters — and the
binary handshake already returns the same digest in its ack, so in the normal
case verifying costs nothing at all.

Every function takes a ``conn`` — a :class:`~cheetah_db.client.CheetahClient` or
anything else exposing ``send(line) -> Response``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .binary import BinarySession
from .client import CheetahError
from .protocol import Response, build_key_value_command, decode_payload

__all__ = [
    "AliasIdentity",
    "TableProfile",
    "alias_digest",
    "describe_types",
    "list_argument_keys",
    "list_commands",
    "list_profiles",
    "load_session",
    "resolve_command",
    "table_profile",
]


def _send_or_raise(conn: Any, line: str, what: str) -> Response:
    response = conn.send(line)
    if not response.ok:
        raise CheetahError(f"cheetah {what} failed: {response.reason}")
    return response


@dataclass(frozen=True)
class AliasIdentity:
    """What a cached index is verified against."""

    version: int
    epoch: int
    digest: str | None
    commands: int
    keys_digest: str | None
    keys: int


@dataclass(frozen=True)
class TableProfile:
    """A table's numeric widths.

    ``uint``/``int``/``float`` are the **resolved** widths — what the server
    will actually use, defaults included — because that is the only answer that
    tells a client how what it writes will be read. ``declared_*`` are the
    widths this table declares for itself, with ``0`` for "not declared".
    """

    table: str
    uint: int
    int: int
    float: int
    declared: bool
    declared_uint: int = 0
    declared_int: int = 0
    declared_float: int = 0
    updated: bool = False

    def widths(self) -> dict[str, int]:
        return {"uint": self.uint, "int": self.int, "float": self.float}


@dataclass
class AliasPage:
    """One page of the command index or of the argument-key dictionary."""

    entries: list[dict[str, Any]] = field(default_factory=list)
    digest: str | None = None
    epoch: int = 0
    total: int = 0


def alias_digest(conn: Any) -> AliasIdentity:
    """``ALIAS digest`` — index identity only."""
    response = _send_or_raise(conn, "ALIAS digest", "ALIAS digest")
    return AliasIdentity(
        version=response.int_field("version", 1) or 1,
        epoch=response.int_field("epoch", 0) or 0,
        digest=response.field_value("digest"),
        commands=response.int_field("commands", 0) or 0,
        keys_digest=response.field_value("keys_digest"),
        keys=response.int_field("keys", 0) or 0,
    )


def list_commands(
    conn: Any,
    *,
    from_: int | None = None,
    limit: int | None = None,
    kind: str | None = None,
) -> AliasPage:
    """``ALIAS list`` — the whole command index as ``[{id, name, kind}]``.

    ``kind`` says which of the server's routing tables a name comes from
    (``micro``, ``alias``, ``builtin``, ``engine``, ``frontend``); it is
    descriptive, and every kind is callable the same way.
    """
    line = build_key_value_command("ALIAS list", {"from": from_, "limit": limit, "kind": kind})
    response = _send_or_raise(conn, line, "ALIAS list")
    return AliasPage(
        entries=decode_payload(response.fields) or [],
        digest=response.field_value("digest"),
        epoch=response.int_field("epoch", 0) or 0,
        total=response.int_field("total", 0) or 0,
    )


def list_argument_keys(
    conn: Any, *, from_: int | None = None, limit: int | None = None
) -> AliasPage:
    """``ALIAS keys`` — the argument-key dictionary, same shape as the index."""
    line = build_key_value_command("ALIAS keys", {"from": from_, "limit": limit})
    response = _send_or_raise(conn, line, "ALIAS keys")
    return AliasPage(
        entries=decode_payload(response.fields) or [],
        digest=response.field_value("digest"),
        total=response.int_field("total", 0) or 0,
    )


def resolve_command(
    conn: Any, name: str | None = None, index: int | None = None
) -> dict[str, Any]:
    """``ALIAS get`` — resolve one command, by name or by index."""
    if name is None and index is None:
        raise CheetahError("cheetah ALIAS get requires a name or an id")
    fields = {"name": name} if name is not None else {"id": index}
    response = _send_or_raise(conn, build_key_value_command("ALIAS get", fields), "ALIAS get")
    return {
        "id": response.int_field("id"),
        "name": response.field_value("name"),
        "kind": response.field_value("kind"),
    }


def describe_types(conn: Any) -> Any:
    """``ALIAS types`` — the value-type codec and the server's default widths."""
    response = _send_or_raise(conn, "ALIAS types", "ALIAS types")
    return decode_payload(response.fields)


def table_profile(
    conn: Any,
    table: str,
    *,
    uint: int | None = None,
    int_: int | None = None,
    float_: int | None = None,
    reset: bool = False,
    session: BinarySession | None = None,
) -> TableProfile:
    """``ALIAS profile`` — read or set a table's numeric widths.

    Passing any of ``uint``/``int_``/``float_`` writes; ``reset=True`` removes
    the declaration. When ``session`` is given the resolved widths are recorded
    on it, so a later frame that names this table can leave its widths implicit.
    """
    fields: dict[str, Any] = {"table": table}
    if reset:
        fields["reset"] = 1
    else:
        if uint is not None:
            fields["uint"] = uint
        if int_ is not None:
            fields["int"] = int_
        if float_ is not None:
            fields["float"] = float_
    line = build_key_value_command("ALIAS profile", fields)
    response = _send_or_raise(conn, line, "ALIAS profile")
    profile = TableProfile(
        table=response.field_value("table") or table,
        uint=response.int_field("uint", 8) or 8,
        int=response.int_field("int", 8) or 8,
        float=response.int_field("float", 8) or 8,
        declared=response.bool_field("declared"),
        declared_uint=response.int_field("declared_uint", 0) or 0,
        declared_int=response.int_field("declared_int", 0) or 0,
        declared_float=response.int_field("declared_float", 0) or 0,
        updated=response.bool_field("updated"),
    )
    if session is not None:
        session.load_profile(profile.table, profile.widths())
    return profile


def list_profiles(conn: Any) -> list[dict[str, Any]]:
    """``ALIAS profile`` with no table — every declared profile."""
    response = _send_or_raise(conn, "ALIAS profile", "ALIAS profile")
    return decode_payload(response.fields) or []


def load_session(conn: Any, session: BinarySession) -> BinarySession:
    """Fill a :class:`~cheetah_db.binary.BinarySession` from the server.

    Rarely needed on a live binary connection — the handshake ack already
    carries both tables — but it is how a client refreshes an index it kept
    across connections, or builds one over a text socket.
    """
    commands = list_commands(conn)
    keys = list_argument_keys(conn)
    session.load_commands(commands.entries, commands.digest)
    session.load_keys(keys.entries, keys.digest)
    session.epoch = commands.epoch
    return session

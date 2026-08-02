"""The binary protocol codec and the ALIAS discovery layer."""

from __future__ import annotations

import base64
import json
import struct
import unittest

from cheetah_db import alias, binary
from cheetah_db.binary import BinarySession, Kind, KeyMode, Status
from cheetah_db.protocol import parse_response


def test_session() -> BinarySession:
    """A session preloaded with a small index, standing in for a handshake ack."""
    session = BinarySession({"uint": 8, "int": 8, "float": 8})
    session.load_commands(
        [
            {"id": 1, "name": "RECORD", "kind": "micro"},
            {"id": 2, "name": "PAIR_SET", "kind": "builtin"},
            {"id": 3, "name": "INSERT", "kind": "builtin"},
            {"id": 4, "name": "BATCH", "kind": "micro"},
            {"id": 5, "name": "EDIT", "kind": "builtin"},
        ],
        "deadbeefdeadbeef",
    )
    session.load_keys(
        [
            {"id": 1, "name": "table"},
            {"id": 2, "name": "key"},
            {"id": 3, "name": "count"},
            {"id": 4, "name": "limit"},
            {"id": 5, "name": "value"},
        ],
        "feedfacefeedface",
    )
    return session


def decode_request_line(frame: bytes, session: BinarySession) -> str:
    """Rebuild the canonical line from a request frame, as the server does."""
    body = frame[binary.FRAME_HEADER_BYTES :]
    at = 0

    def short() -> str:
        nonlocal at
        length = body[at]
        text = body[at + 1 : at + 1 + length].decode("utf-8")
        at += 1 + length
        return text

    flags = body[at]
    at += 1
    if flags & 0x01:
        command = short()
    else:
        command = session.command_name(struct.unpack(">H", body[at : at + 2])[0])
        at += 2
    if flags & 0x02:
        command += ":" + short()

    count = struct.unpack(">H", body[at : at + 2])[0]
    at += 2
    parts = [command]
    for _ in range(count):
        mode = body[at]
        at += 1
        key = ""
        if mode == KeyMode.INDEXED:
            key = session.key_name(struct.unpack(">H", body[at : at + 2])[0])
            at += 2
        elif mode == KeyMode.INLINE:
            key = short()
        value, text, skip, at = binary._decode_value(body, at, session.widths, session)
        if skip:
            continue
        parts.append(f"{key}={text}" if key else text)
    assert at == len(body), "the frame has trailing bytes"
    return " ".join(parts)


def encode_response_frame(line: str, session: BinarySession) -> bytes:
    """Encode a response line the way the server would, for the decoder tests."""
    head, _, rest = line.partition(",")
    status = {
        "SUCCESS": Status.SUCCESS,
        "ERROR": Status.ERROR,
        "PENDING": Status.PENDING,
    }.get(head, Status.OTHER)

    fields: list[tuple[str, str]] = []
    if status == Status.ERROR:
        fields = [("", rest)]
    elif rest:
        cursor = 0
        while cursor <= len(rest):
            nxt = rest.find(",", cursor)
            token = rest[cursor:] if nxt == -1 else rest[cursor:nxt]
            key, equals, value = token.partition("=")
            if not equals:
                fields.append(("", token))
            elif key == "value":
                fields.append(("value", rest[cursor + len(key) + 1 :]))
                break
            else:
                fields.append((key, value))
            if nxt == -1:
                break
            cursor = nxt + 1

    body = bytes([status]) + struct.pack(">H", len(fields))
    for key, value in fields:
        if not key:
            body += bytes([KeyMode.POSITIONAL])
        else:
            index = session.key_id(key)
            if index is None:
                body += bytes([KeyMode.INLINE, len(key)]) + key.encode("utf-8")
            else:
                body += bytes([KeyMode.INDEXED]) + struct.pack(">H", index)
        if key == "value":
            raw = value.encode("latin1")
            body += bytes([Kind.BYTES << 4]) + struct.pack(">I", len(raw)) + raw
            continue
        numeric = binary.canonical_number(value)
        if numeric is None:
            body += binary.encode_value({"type": "string", "value": value})
        elif numeric["type"] == "float":
            body += binary.encode_value({**numeric, "width": 8})
        else:
            body += binary.encode_value(
                {**numeric, "width": binary.minimal_width(numeric["type"], numeric["value"])}
            )
    return binary.encode_frame(binary.FrameType.RESPONSE, body)


class BinaryRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = test_session()

    def test_command_travels_as_two_bytes(self) -> None:
        frame = binary.encode_command_line("RECORD get table=ngram", self.session)
        self.assertEqual(frame[0], binary.FRAME_MAGIC)
        self.assertEqual(frame[1], binary.FrameType.REQUEST)
        at = binary.FRAME_HEADER_BYTES + 1
        self.assertEqual(struct.unpack(">H", frame[at : at + 2])[0], self.session.command_id("RECORD"))
        self.assertEqual(decode_request_line(frame, self.session), "RECORD get table=ngram")

    def test_unknown_command_falls_back_to_its_name(self) -> None:
        frame = binary.encode_command_line("GRAPH_RECALL seeds=a", self.session)
        self.assertEqual(frame[binary.FRAME_HEADER_BYTES] & 0x01, 0x01)
        self.assertEqual(decode_request_line(frame, self.session), "GRAPH_RECALL seeds=a")

    def test_transcoding_is_lossless(self) -> None:
        lines = [
            "RECORD set table=ngram key=x6265726c696e cnt=42 prob=0.25",
            "PAIR_SET x616263 ctx:BERLIN",
            "RECORD scan table=ngram limit=100 cursor=x00ff",
            "BATCH PAIR_SET items=W10= continue_on_error=1",
            "EDIT 7 hello  world",
            "INSERT:16 sixteen bytes!!",
            "PAIR_SCAN ctx: 50",
            # A payload carrying base64 padding: the "=" is data, not a split.
            "INSERT eyJhIjoxfQ==",
            "EDIT 7 a=b c=d",
            "PAIR_SET x00ff k=1",
        ]
        for line in lines:
            with self.subTest(line=line):
                frame = binary.encode_command_line(line, self.session)
                self.assertEqual(decode_request_line(frame, self.session), line)

    def test_every_transcoded_width_is_stated(self) -> None:
        # The bug this pins: a float written 8 bytes wide but tagged "use the
        # default" is read back at a session's 4 and comes out as 1.625. The
        # transcoder does not know which table a line addresses, so it can never
        # predict what a width-0 tag resolves to.
        frame = binary.encode_command_line("RECORD set table=t prob=0.25", self.session)
        for widths in ({"uint": 4, "int": 4, "float": 4}, {"uint": 8, "int": 8, "float": 8}):
            self.session.widths = widths
            self.assertEqual(
                decode_request_line(frame, self.session), "RECORD set table=t prob=0.25"
            )

    def test_width_zero_follows_a_known_table_profile(self) -> None:
        self.session.load_profile("ngram", {"uint": 2, "int": 8, "float": 4})
        frame = binary.encode_request(
            "RECORD",
            [
                {"type": "string", "value": "set"},
                {"key": "table", "type": "string", "value": "ngram"},
                {"key": "count", "type": "uint", "value": 256},
            ],
            self.session,
        )
        body = frame[binary.FRAME_HEADER_BYTES :]
        # Two bytes written, and a tag that defers to the profile: the server
        # reads the same pair because the profile lives on its side.
        self.assertEqual(body[-3] >> 4, Kind.UINT)
        self.assertEqual(body[-3] & 0x0F, 0)
        self.assertEqual(len(body[-2:]), 2)

    def test_numbers_pick_the_smallest_width(self) -> None:
        self.assertEqual(binary.type_token("42"), {"type": "uint", "value": 42, "width": 1})
        self.assertEqual(binary.type_token("70000"), {"type": "uint", "value": 70000, "width": 4})
        self.assertEqual(binary.type_token("-7"), {"type": "int", "value": -7, "width": 1})
        self.assertEqual(binary.type_token("0.25"), {"type": "float", "value": 0.25, "width": 8})
        # A form that would not re-render identically stays a string: the line
        # is the contract, and "007" must come back as "007".
        self.assertEqual(binary.type_token("007")["type"], "string")
        self.assertEqual(binary.type_token("1e3")["type"], "string")

    def test_typed_request_needs_no_text_line(self) -> None:
        frame = binary.encode_request(
            "RECORD",
            [
                {"type": "string", "value": "set"},
                {"key": "table", "type": "string", "value": "ngram"},
                {"key": "key", "type": "bytes", "value": b"berlin"},
                {"key": "cnt", "type": "uint", "value": 42, "width": 4},
                {"key": "prob", "type": "float", "value": 0.5, "width": 8},
                {"key": "limit", "type": "null"},
            ],
            self.session,
        )
        self.assertEqual(
            decode_request_line(frame, self.session),
            "RECORD set table=ngram key=x6265726c696e cnt=42 prob=0.5",
        )

    def test_insert_carries_its_size_as_a_suffix(self) -> None:
        frame = binary.encode_request(
            "INSERT", [{"type": "string", "value": "payload"}], self.session, suffix="16"
        )
        self.assertEqual(frame[binary.FRAME_HEADER_BYTES] & 0x02, 0x02)
        self.assertEqual(decode_request_line(frame, self.session), "INSERT:16 payload")

    def test_nested_command_travels_as_an_enum(self) -> None:
        frame = binary.encode_request(
            "BATCH",
            [
                {
                    "type": "enum",
                    "family": binary.COMMANDS_ENUM,
                    "value": self.session.command_id("PAIR_SET"),
                },
                {"key": "items", "type": "string", "value": "[]"},
            ],
            self.session,
        )
        self.assertEqual(decode_request_line(frame, self.session), "BATCH PAIR_SET items=[]")

    def test_a_newline_is_refused(self) -> None:
        with self.assertRaises(binary.BinaryProtocolError):
            binary.encode_command_line("READ 1\nREAD 2", self.session)


class BinaryResponseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = test_session()

    def test_round_trip_gives_the_canonical_line(self) -> None:
        lines = [
            "SUCCESS,pair_set",
            "SUCCESS,count=2,limit=100",
            "SUCCESS,size=5,value=a,b c",
            "ERROR,value_size_mismatch (expected 16, got 17)",
            "PENDING,job=reduce_1",
        ]
        for line in lines:
            with self.subTest(line=line):
                frame = encode_response_frame(line, self.session)
                decoded = binary.decode_response(
                    frame[binary.FRAME_HEADER_BYTES :], self.session, self.session.widths
                )
                self.assertEqual(decoded.line, line)
                # And the line still parses exactly as over a text socket.
                self.assertEqual(parse_response(decoded.line).status, line.split(",")[0])

    def test_read_payload_survives_bytes_that_are_not_text(self) -> None:
        raw = "\x00\xff,\x01"
        frame = encode_response_frame(f"SUCCESS,size=4,value={raw}", self.session)
        decoded = binary.decode_response(
            frame[binary.FRAME_HEADER_BYTES :], self.session, self.session.widths
        )
        self.assertEqual(decoded.fields["value"], raw)

    def test_typed_values_are_kept_alongside_the_text(self) -> None:
        frame = encode_response_frame("SUCCESS,count=42,limit=0.5", self.session)
        decoded = binary.decode_response(
            frame[binary.FRAME_HEADER_BYTES :], self.session, self.session.widths
        )
        self.assertEqual(decoded.values["count"], 42)
        self.assertEqual(decoded.values["limit"], 0.5)


class FrameTests(unittest.TestCase):
    def test_read_frame_waits_and_keeps_the_remainder(self) -> None:
        frame = binary.encode_frame(binary.FrameType.RESPONSE, bytes([1, 0, 0]))
        self.assertIsNone(binary.read_frame(frame[:4]))
        self.assertIsNone(binary.read_frame(frame[:-1]))
        frame_type, body, rest = binary.read_frame(frame + b"\xc7")
        self.assertEqual(frame_type, binary.FrameType.RESPONSE)
        self.assertEqual(rest, b"\xc7")
        self.assertEqual(body, bytes([1, 0, 0]))
        with self.assertRaises(binary.BinaryProtocolError):
            binary.read_frame(b"Ahello")

    def test_handshake_ack_fills_a_session_with_both_tables(self) -> None:
        # Rebuilt here in the ack's own layout, which is the contract with
        # src/binary_protocol.go → encodeHandshakeAck.
        def short(text: str) -> bytes:
            return bytes([len(text)]) + text.encode("utf-8")

        body = bytes([1, 4, 8, 4, 0]) + struct.pack(">Q", 3)
        body += short("0123456789abcdef") + short("fedcba9876543210")
        body += struct.pack(">H", 1) + struct.pack(">H", 7) + bytes([1]) + short("RECORD")
        body += struct.pack(">H", 1) + struct.pack(">H", 9) + short("table")

        ack = binary.decode_handshake_ack(body)
        self.assertEqual(ack.widths, {"uint": 4, "int": 8, "float": 4})
        self.assertEqual(ack.epoch, 3)
        self.assertEqual(ack.commands, [{"id": 7, "kind": "micro", "name": "RECORD"}])
        self.assertEqual(ack.keys, [{"id": 9, "name": "table"}])

        session = BinarySession().adopt(ack)
        self.assertEqual(session.command_id("record"), 7)
        self.assertEqual(session.key_name(9), "table")
        self.assertTrue(session.matches_digest("0123456789abcdef"))
        self.assertFalse(session.matches_digest("nope"))

    def test_handshake_asks_for_widths_it_cares_about(self) -> None:
        frame = binary.encode_handshake(uint=4, float_=4)
        body = frame[binary.FRAME_HEADER_BYTES :]
        self.assertEqual(frame[1], binary.FrameType.HANDSHAKE)
        # A zero means "the server's default": a preference for one type does
        # not force one for all three.
        self.assertEqual(list(body), [binary.PROTOCOL_VERSION, 4, 0, 4, 0])


class FakeConnection:
    def __init__(self, responses: dict[str, str]) -> None:
        self.responses = responses
        self.sent: list[str] = []

    def send(self, line: str):
        self.sent.append(line)
        if line not in self.responses:
            raise AssertionError(f"unexpected command: {line}")
        return parse_response(self.responses[line])


def payload(value) -> str:
    return base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")


class AliasCommandTests(unittest.TestCase):
    def test_digest_reads_the_identity_a_cache_is_checked_against(self) -> None:
        conn = FakeConnection(
            {
                "ALIAS digest": "SUCCESS,version=1,epoch=1,digest=abc123,"
                "commands=60,keys_digest=def456,keys=80"
            }
        )
        identity = alias.alias_digest(conn)
        self.assertEqual(identity.digest, "abc123")
        self.assertEqual(identity.keys_digest, "def456")
        self.assertEqual(identity.commands, 60)

    def test_load_session_fills_a_binary_session(self) -> None:
        conn = FakeConnection(
            {
                "ALIAS list": "SUCCESS,epoch=1,digest=abc123,total=2,count=2,payload="
                + payload(
                    [
                        {"id": 1, "name": "ALIAS", "kind": "micro"},
                        {"id": 2, "name": "RECORD", "kind": "micro"},
                    ]
                ),
                "ALIAS keys": "SUCCESS,digest=def456,total=1,count=1,payload="
                + payload([{"id": 1, "name": "table"}]),
            }
        )
        session = alias.load_session(conn, BinarySession())
        self.assertEqual(session.command_id("RECORD"), 2)
        self.assertEqual(session.key_id("table"), 1)
        self.assertTrue(session.matches_digest("abc123"))

    def test_table_profile_reads_resolved_and_writes_declared(self) -> None:
        conn = FakeConnection(
            {
                "ALIAS profile table=ngram": "SUCCESS,table=ngram,uint=8,int=8,float=8,"
                "declared=0,declared_uint=0,declared_int=0,declared_float=0,updated=0",
                "ALIAS profile table=ngram uint=4 float=4": "SUCCESS,table=ngram,uint=4,int=8,"
                "float=4,declared=1,declared_uint=4,declared_int=0,declared_float=4,updated=1",
                "ALIAS profile table=ngram reset=1": "SUCCESS,table=ngram,uint=8,int=8,float=8,"
                "declared=0,declared_uint=0,declared_int=0,declared_float=0,updated=0",
            }
        )
        session = BinarySession()

        profile = alias.table_profile(conn, "ngram", session=session)
        self.assertFalse(profile.declared)
        self.assertEqual(profile.uint, 8)

        profile = alias.table_profile(conn, "ngram", uint=4, float_=4, session=session)
        self.assertTrue(profile.updated)
        self.assertEqual(profile.float, 4)
        # int was never declared, so it reads as the default rather than zero.
        self.assertEqual(profile.int, 8)
        self.assertEqual(profile.declared_int, 0)
        # And the session now knows how to leave a width implicit for it.
        self.assertEqual(session.widths_for("ngram"), {"uint": 4, "int": 8, "float": 4})

        profile = alias.table_profile(conn, "ngram", reset=True, session=session)
        self.assertFalse(profile.declared)

    def test_resolve_command_needs_a_name_or_an_id(self) -> None:
        with self.assertRaises(Exception):
            alias.resolve_command(FakeConnection({}))


if __name__ == "__main__":
    unittest.main()

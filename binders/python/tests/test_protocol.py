"""Codec regressions: the encodings a hand-written client gets subtly wrong."""

from __future__ import annotations

import base64
import unittest

from cheetah_db import protocol


class ArgumentEncodingTests(unittest.TestCase):
    def test_printable_argument_travels_bare(self) -> None:
        self.assertEqual(protocol.encode_argument("ctx:0001"), "ctx:0001")

    def test_leading_x_is_escaped_because_the_server_reads_it_as_hex(self) -> None:
        self.assertEqual(protocol.encode_argument("x:thing"), "x" + b"x:thing".hex())

    def test_spaces_and_wildcards_are_escaped(self) -> None:
        self.assertEqual(protocol.encode_argument("two words"), "x" + b"two words".hex())
        self.assertEqual(protocol.encode_argument("*"), "x2a")

    def test_binary_round_trips_through_hex(self) -> None:
        value = bytes([0x00, 0x9F, 0x20, 0x41])
        self.assertEqual(protocol.decode_hex_key(protocol.encode_argument(value)), value)

    def test_reserved_namespaces_are_refused(self) -> None:
        for key in (
            b"\x01gn:node", b"\x05gt:term", b"\x06rr:row", b"\x07gc:cache",
            b"\x08ri:index", "graph/edges", "idx/terms",
        ):
            with self.assertRaises(ValueError):
                protocol.assert_unreserved_key(key)
        self.assertEqual(protocol.assert_unreserved_key("ctx:1"), b"ctx:1")

    def test_cursor_travels_verbatim(self) -> None:
        cursor = "x0001ff"
        line = protocol.build_command("PAIR_SCAN", "ctx:", 64, protocol.raw_argument(cursor))
        self.assertEqual(line, "PAIR_SCAN ctx: 64 x0001ff")

    def test_command_rejects_an_embedded_newline(self) -> None:
        with self.assertRaises(ValueError):
            protocol.build_command("INSERT", protocol.raw_argument("a\nb"))

    def test_key_value_command_refuses_a_spaced_value(self) -> None:
        # GRAPH_* arguments are split on whitespace, so a spaced value would be
        # silently truncated into a positional argument the server ignores.
        with self.assertRaises(ValueError):
            protocol.build_key_value_command("GRAPH_NODE_SET", {"id": "a b"})

    def test_key_value_command_skips_empty_fields(self) -> None:
        line = protocol.build_key_value_command(
            "GRAPH_DEGREE", {"id": "alice", "type": None, "weighted": True, "limit": ""}
        )
        self.assertEqual(line, "GRAPH_DEGREE id=alice weighted=1")


class ResponseParsingTests(unittest.TestCase):
    def test_flags_and_fields_are_separated(self) -> None:
        parsed = protocol.parse_response("SUCCESS,node_set,id=alice")
        self.assertTrue(parsed.ok)
        self.assertEqual(parsed.flags, ("node_set",))
        self.assertEqual(parsed.fields["id"], "alice")

    def test_value_owns_the_rest_of_the_line(self) -> None:
        parsed = protocol.parse_response('SUCCESS,size=17,value={"a":1,"b":"x,y"}')
        self.assertEqual(parsed.fields["value"], '{"a":1,"b":"x,y"}')

    def test_error_reason_keeps_its_commas_and_spaces(self) -> None:
        parsed = protocol.parse_response("ERROR,value_size_mismatch (expected 16, got 17)")
        self.assertFalse(parsed.ok)
        self.assertEqual(parsed.error, "value_size_mismatch (expected 16, got 17)")

    def test_missing_response_is_classified_rather_than_crashing(self) -> None:
        parsed = protocol.parse_response(None)
        self.assertFalse(parsed.ok)
        self.assertTrue(parsed.unreachable)
        self.assertEqual(parsed.error, "no_response")

    def test_items_carry_key_absolute_key_and_reducer_payload(self) -> None:
        parsed = protocol.parse_response(
            "SUCCESS,reducer=counts,count=1,items=636e743a:42:" + base64.b64encode(b"Hello").decode()
        )
        item = parsed.items()[0]
        self.assertEqual(item.key, b"cnt:")
        self.assertEqual(item.abs_key, 42)
        self.assertEqual(item.payload, b"Hello")

    def test_exhausted_cursor_is_none_in_both_spellings(self) -> None:
        self.assertIsNone(protocol.parse_response("SUCCESS,count=0").cursor())
        self.assertIsNone(protocol.parse_response("SUCCESS,next_cursor=*").cursor())
        self.assertEqual(protocol.parse_response("SUCCESS,next_cursor=x01").cursor(), "x01")

    def test_payload_decodes_json(self) -> None:
        payload = base64.b64encode(b'{"associations":[]}').decode()
        parsed = protocol.parse_response(f"SUCCESS,count=0,payload={payload}")
        self.assertEqual(parsed.payload(), {"associations": []})

    def test_transport_layer_comes_off_before_the_application_format(self) -> None:
        original = b"\x01\x02fixed-size-counts"
        self.assertEqual(
            protocol.decode_transport_payload(base64.b64encode(original)), original
        )
        self.assertIsNone(protocol.decode_transport_payload(b"not base64!"))

    def test_branches_histogram(self) -> None:
        branches = protocol.parse_branches("61:3;62:1")
        self.assertEqual([(branch.byte, branch.count) for branch in branches], [(0x61, 3), (0x62, 1)])

    def test_numeric_field_keeps_zero_and_rejects_garbage(self) -> None:
        fields = {"count": "0", "score": "1.5", "bad": "n/a"}
        self.assertEqual(protocol.numeric_field(fields, "count", 7), 0)
        self.assertEqual(protocol.numeric_field(fields, "score"), 1.5)
        self.assertEqual(protocol.numeric_field(fields, "bad", -1), -1)
        self.assertEqual(protocol.numeric_field(fields, "absent", -1), -1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

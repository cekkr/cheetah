"""Multi-field record tables: the command spellings and the schema contract."""

from __future__ import annotations

import unittest

from cheetah_db import records
from cheetah_db.client import CheetahError
from cheetah_db.protocol import RawArgument

from .fakes import FakeConnection


class FieldSpecTests(unittest.TestCase):
    """The spec is rendered client-side, so a bad field fails before the wire."""

    def test_accepts_the_shapes_a_caller_naturally_has(self) -> None:
        self.assertEqual(records.field_spec("cnt:uint:4"), "cnt:uint:4")
        self.assertEqual(records.field_spec(("cnt", "uint", 4)), "cnt:uint:4")
        self.assertEqual(records.field_spec({"name": "cnt", "type": "uint", "width": 4}), "cnt:uint:4")
        self.assertEqual(records.field_spec(records.RecordField("cnt", "uint", 4)), "cnt:uint:4")

    def test_fills_in_the_default_width_only_where_the_server_has_one(self) -> None:
        self.assertEqual(records.field_spec("w:float"), "w:float:8")
        self.assertEqual(records.field_spec("flag:bool"), "flag:bool:1")
        # bytes/string have no sensible default: the width is what decides the
        # cost of every row.
        with self.assertRaises(CheetahError):
            records.field_spec("label:string")

    def test_refuses_a_field_name_that_would_collide_with_a_modifier(self) -> None:
        # In `RECORD set table=t key=k <field>=<value>` a field *is* an argument.
        for name in ("table", "key", "fields", "limit", "cursor"):
            with self.assertRaises(CheetahError):
                records.field_spec(f"{name}:uint:4")

    def test_refuses_an_unknown_type_and_a_malformed_spec(self) -> None:
        with self.assertRaises(CheetahError):
            records.field_spec("cnt:number:4")
        with self.assertRaises(CheetahError):
            records.field_spec("cnt")


class SchemaCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_define_sends_one_fields_token_and_reports_the_shape(self) -> None:
        schema = records.define(
            self.conn, "ngram", ["cnt:uint:4", ("prob", "float", 4), "label:string:12"]
        )
        self.assertEqual(
            self.conn.commands[-1],
            "RECORD define table=ngram fields=cnt:uint:4,prob:float:4,label:string:12",
        )
        self.assertEqual((schema.table, schema.width, schema.generation), ("ngram", 20, 1))

    def test_define_twice_raises_unless_if_not_exists(self) -> None:
        records.define(self.conn, "ngram", "cnt:uint:4")
        with self.assertRaises(CheetahError):
            records.define(self.conn, "ngram", "cnt:uint:4")
        again = records.define(self.conn, "ngram", "cnt:uint:4", if_not_exists=True)
        self.assertEqual(again.table, "ngram")
        self.assertIn("if_not_exists=1", self.conn.commands[-1])

    def test_schema_returns_none_for_a_table_that_is_not_there(self) -> None:
        self.assertIsNone(records.schema(self.conn, "absent"))

    def test_schema_asks_for_the_row_count_only_when_told_to(self) -> None:
        records.define(self.conn, "t", "cnt:uint:4")
        records.set_row(self.conn, "t", "a", {"cnt": 1})
        plain = records.schema(self.conn, "t")
        self.assertIsNone(plain.rows)
        self.assertNotIn("rows=", self.conn.commands[-1])
        counted = records.schema(self.conn, "t", rows=True)
        self.assertEqual(counted.rows, 1)
        self.assertIn("rows=1", self.conn.commands[-1])

    def test_tables_lists_every_schema(self) -> None:
        records.define(self.conn, "a", "cnt:uint:4")
        records.define(self.conn, "b", "w:float:8")
        listed = records.tables(self.conn)
        self.assertEqual([schema.table for schema in listed], ["a", "b"])
        self.assertEqual(listed[1].fields[0].name, "w")


class RowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()
        records.define(self.conn, "ngram", "cnt:uint:4,prob:float:4,label:string:12")

    def test_set_and_get_round_trip_the_declared_types(self) -> None:
        write = records.set_row(
            self.conn, "ngram", "berlin", {"cnt": 42, "prob": 0.25, "label": "city"}
        )
        self.assertTrue(write.created)
        self.assertEqual(write.written, 3)
        self.assertEqual(
            records.get_row(self.conn, "ngram", "berlin"),
            {"cnt": 42, "prob": 0.25, "label": "city"},
        )

    def test_a_second_write_patches_only_the_fields_it_names(self) -> None:
        records.set_row(self.conn, "ngram", "berlin", {"cnt": 42, "label": "city"})
        write = records.set_row(self.conn, "ngram", "berlin", {"cnt": 43})
        self.assertFalse(write.created)
        row = records.get_row(self.conn, "ngram", "berlin")
        self.assertEqual(row["cnt"], 43)
        self.assertEqual(row["label"], "city")

    def test_text_with_a_space_travels_hex_encoded(self) -> None:
        records.set_row(self.conn, "ngram", "berlin", {"label": "old town"})
        # RECORD splits its arguments on whitespace, so the value can only
        # survive as x<hex> — and must come back as the original text.
        self.assertIn("label=x", self.conn.commands[-1])
        self.assertEqual(records.get_row(self.conn, "ngram", "berlin")["label"], "old town")

    def test_a_key_with_a_space_travels_hex_encoded_too(self) -> None:
        records.set_row(self.conn, "ngram", "two words", {"cnt": 1})
        self.assertIn("key=x", self.conn.commands[-1])
        self.assertEqual(records.get_row(self.conn, "ngram", "two words")["cnt"], 1)

    def test_booleans_travel_as_one_and_zero(self) -> None:
        records.define(self.conn, "flags", "seen:bool")
        records.set_row(self.conn, "flags", "a", {"seen": True})
        self.assertIn("seen=1", self.conn.commands[-1])
        self.assertIs(records.get_row(self.conn, "flags", "a")["seen"], True)

    def test_a_raw_argument_passes_through_untouched(self) -> None:
        records.set_row(self.conn, "ngram", "berlin", {"label": RawArgument("x63697479")})
        self.assertIn("label=x63697479", self.conn.commands[-1])

    def test_projection_asks_for_a_subset(self) -> None:
        records.set_row(self.conn, "ngram", "berlin", {"cnt": 1, "label": "city"})
        row = records.get_row(self.conn, "ngram", "berlin", fields=["cnt"])
        self.assertEqual(row, {"cnt": 1})
        self.assertIn("fields=cnt", self.conn.commands[-1])

    def test_missing_row_reads_as_none_rather_than_raising(self) -> None:
        self.assertIsNone(records.get_row(self.conn, "ngram", "lisbon"))

    def test_set_refuses_an_empty_update_and_a_reserved_field_name(self) -> None:
        with self.assertRaises(CheetahError):
            records.set_row(self.conn, "ngram", "berlin", {})
        with self.assertRaises(CheetahError):
            records.set_row(self.conn, "ngram", "berlin", {"table": 1})


class SchemaEvolutionTests(unittest.TestCase):
    """The contract that makes the family worth having."""

    def setUp(self) -> None:
        self.conn = FakeConnection()
        records.define(self.conn, "doc", "cnt:uint:4,label:string:8")
        records.set_row(self.conn, "doc", "a", {"cnt": 7, "label": "alpha"})

    def test_a_row_predating_a_field_reads_none_until_it_is_rewritten(self) -> None:
        records.alter(self.conn, "doc", add="score:float:8")
        row = records.get_row(self.conn, "doc", "a")
        self.assertIsNone(row["score"])  # not 0.0 — nobody wrote a zero
        self.assertEqual(row["cnt"], 7)
        records.set_row(self.conn, "doc", "a", {"score": 1.5})
        self.assertEqual(records.get_row(self.conn, "doc", "a")["score"], 1.5)

    def test_dropping_a_field_leaves_the_others_where_they_are(self) -> None:
        schema = records.alter(self.conn, "doc", drop="label")
        self.assertEqual(schema.width, 12)  # the hole stays; width does not shrink
        self.assertEqual(schema.dead_bytes, 8)
        row = records.get_row(self.conn, "doc", "a")
        self.assertNotIn("label", row)
        self.assertEqual(row["cnt"], 7)

    def test_compact_reclaims_the_hole_and_bumps_the_generation(self) -> None:
        records.alter(self.conn, "doc", drop="label")
        schema, rewritten = records.compact(self.conn, "doc")
        self.assertEqual(rewritten, 1)
        self.assertEqual(schema.dead_bytes, 0)
        self.assertEqual(schema.width, 4)
        self.assertEqual(schema.generation, 2)
        self.assertEqual(records.get_row(self.conn, "doc", "a")["cnt"], 7)

    def test_alter_can_chain_the_compaction(self) -> None:
        schema = records.alter(self.conn, "doc", drop="label", compact=True)
        self.assertIn("compact=1", self.conn.commands[-1])
        self.assertEqual(schema.dead_bytes, 0)

    def test_alter_needs_something_to_do(self) -> None:
        with self.assertRaises(CheetahError):
            records.alter(self.conn, "doc")


class ScanAndDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()
        records.define(self.conn, "ctx", "cnt:uint:2")
        for index in range(6):
            records.set_row(self.conn, "ctx", f"de/{index}", {"cnt": index})
        records.set_row(self.conn, "ctx", "it/0", {"cnt": 99})

    def test_scan_filters_on_the_key_prefix(self) -> None:
        page = records.scan(self.conn, "ctx", prefix="de/")
        self.assertEqual(len(page.rows), 6)
        self.assertEqual(page.rows[0].text, "de/0")
        self.assertEqual(page.rows[0].fields["cnt"], 0)
        self.assertIsNone(page.cursor)

    def test_a_truncated_page_carries_a_cursor_that_travels_back_verbatim(self) -> None:
        first = records.scan(self.conn, "ctx", prefix="de/", limit=2)
        self.assertIsNotNone(first.cursor)
        second = records.scan(self.conn, "ctx", prefix="de/", limit=2, cursor=first.cursor)
        # The cursor is already in the server's own x<hex> spelling: re-encoding
        # it would resume from a prefix that does not exist.
        self.assertIn(f"cursor={first.cursor}", self.conn.commands[-1])
        self.assertEqual([row.text for row in second.rows], ["de/2", "de/3"])

    def test_iter_rows_pages_through_everything_once(self) -> None:
        rows = list(records.iter_rows(self.conn, "ctx", limit=2))
        self.assertEqual(len(rows), 7)
        self.assertEqual(len({row.text for row in rows}), 7)

    def test_iter_rows_honours_max_rows(self) -> None:
        self.assertEqual(len(list(records.iter_rows(self.conn, "ctx", limit=2, max_rows=3))), 3)

    def test_select_filters_decoded_fields_and_pages(self) -> None:
        page = records.select(
            self.conn, "ctx", "cnt", 2, op="gte", prefix="de/", limit=2, budget=3,
            fields=["cnt"],
        )
        self.assertEqual([row.fields["cnt"] for row in page.rows], [2])
        self.assertEqual(page.scanned, 3)
        self.assertFalse(page.indexed)
        self.assertIn("field=cnt op=gte value=2", self.conn.commands[-1])
        self.assertIsNotNone(page.cursor)
        rest = list(
            records.iter_selected(
                self.conn, "ctx", "cnt", 2, op="gte", prefix="de/", limit=2,
                budget=3,
            )
        )
        self.assertEqual([row.fields["cnt"] for row in rest], [2, 3, 4, 5])

    def test_secondary_index_lifecycle_is_opt_in(self) -> None:
        change = records.configure_index(self.conn, "ctx", "cnt")
        self.assertTrue(change.changed)
        self.assertTrue(change.indexed)
        self.assertEqual(change.entries, 7)
        self.assertEqual(records.list_indexes(self.conn, "ctx"), ("cnt",))
        self.assertTrue(records.schema(self.conn, "ctx").field("cnt").indexed)
        page = records.select(self.conn, "ctx", "cnt", 99)
        self.assertTrue(page.indexed)
        dropped = records.configure_index(self.conn, "ctx", "cnt", action="drop")
        self.assertFalse(dropped.indexed)
        self.assertEqual(records.list_indexes(self.conn, "ctx"), ())

    def test_delete_row_is_idempotent_and_drop_takes_the_table(self) -> None:
        self.assertTrue(records.delete_row(self.conn, "ctx", "de/0"))
        self.assertFalse(records.delete_row(self.conn, "ctx", "de/0"))
        self.assertEqual(records.drop_table(self.conn, "ctx"), 6)
        self.assertIsNone(records.schema(self.conn, "ctx"))
        self.assertEqual(records.drop_table(self.conn, "ctx"), 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

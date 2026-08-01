"""Server and database operations: gauges, the log ring, and DB_CREATE/DB_LIST."""

from __future__ import annotations

import unittest

from cheetah_db import admin
from cheetah_db.client import CheetahError

from .fakes import FakeConnection


class ServerOperationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_system_stats_keeps_the_whole_line_alongside_the_named_gauges(self) -> None:
        stats = admin.system_stats(self.conn)
        self.assertEqual(stats.logical_cores, 8)
        # `NA` is how a platform-unavailable metric reports: not zero, not an error.
        self.assertIsNone(stats.system_cpu_pct)
        self.assertAlmostEqual(stats.cache_hit_ratio, 0.9)
        self.assertIn("payload_cache_enabled", stats.fields)

    def test_log_flush_decodes_the_payload_list(self) -> None:
        self.assertEqual(admin.log_flush(self.conn), ["one", "two"])

    def test_file_checkpoint_spells_its_bare_uppercase_flags(self) -> None:
        self.assertEqual(admin.file_checkpoint(self.conn, idle="0s", close_handles=True), 4)
        self.assertEqual(self.conn.commands[-1], "FILE_CHECKPOINT IDLE=0s CLOSE_HANDLES")


class DatabaseOperationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_create_database_reports_the_settings_it_was_created_with(self) -> None:
        created = admin.create_database(self.conn, "bench", pair_bytes=2, payload_cache_mb=16)
        self.assertEqual(self.conn.commands[-1], "DB_CREATE bench pair_bytes=2 payload_cache_mb=16")
        self.assertEqual(created["name"], "bench")
        self.assertEqual(created["settings"]["pair_index_bytes"], "2")

    def test_creating_an_existing_database_raises_rather_than_adopting_it(self) -> None:
        admin.create_database(self.conn, "bench")
        with self.assertRaises(CheetahError):
            admin.create_database(self.conn, "bench")

    def test_unknown_settings_are_caught_before_the_wire(self) -> None:
        with self.assertRaises(CheetahError):
            admin.create_database(self.conn, "bench", cache_size_mb=16)

    def test_booleans_travel_as_one_and_zero(self) -> None:
        admin.create_database(self.conn, "bench", adaptive_pair_index=False)
        self.assertIn("adaptive_pair_index=0", self.conn.commands[-1])

    def test_list_databases_reports_which_ones_carry_their_own_settings(self) -> None:
        admin.create_database(self.conn, "bench", pair_bytes=2)
        admin.create_database(self.conn, "plain")
        listed = {info.name: info for info in admin.list_databases(self.conn)}
        self.assertEqual(set(listed), {"default", "bench", "plain"})
        self.assertTrue(listed["bench"].ad_hoc)
        self.assertFalse(listed["plain"].ad_hoc)
        self.assertEqual(listed["bench"].settings["pair_index_bytes"], 2)

    def test_use_database_is_connection_scoped(self) -> None:
        admin.use_database(self.conn, "notes", payload_cache_entries=0)
        self.assertEqual(self.conn.commands[-1], "DATABASE notes payload_cache_entries=0")

    def test_reset_needs_a_name_before_it_can_carry_settings(self) -> None:
        with self.assertRaises(CheetahError):
            admin.reset_database(self.conn, settings={"pair_bytes": 2})
        admin.reset_database(self.conn, "notes", pair_bytes=2)
        self.assertEqual(self.conn.commands[-1], "RESET_DB notes pair_bytes=2")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

"""Client-level behavior over a real socket: handshake, framing, recovery."""

from __future__ import annotations

import socket
import unittest

from cheetah_db import admin, jobs
from cheetah_db.client import CheetahClient, CheetahError, ThreadLocalClientPool

from .fakes import FakeServerSocket


class ClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeServerSocket()
        self.addCleanup(self.server.close)

    def client(self, **options) -> CheetahClient:
        options.setdefault("timeout", 1.0)
        client = CheetahClient(self.server.host, self.server.port, **options)
        self.addCleanup(client.close)
        return client

    def test_database_selection_is_the_first_line_on_the_connection(self) -> None:
        client = self.client(database="app")
        self.assertTrue(client.connect())
        self.assertEqual(self.server.server.commands[0], "DATABASE app")
        self.assertEqual(self.server.server.database, "app")

    def test_database_overrides_travel_with_the_selection(self) -> None:
        client = self.client(database="app", database_options={"pair_bytes": 2})
        client.connect()
        self.assertEqual(self.server.server.commands[0], "DATABASE app pair_bytes=2")

    def test_a_refused_database_leaves_the_client_unconnected(self) -> None:
        self.server.server._do_database = lambda rest: "ERROR,invalid_database"  # type: ignore[assignment]
        client = self.client(database="app")
        self.assertFalse(client.connect())
        self.assertIn("DATABASE app failed", client.describe_failures())

    def test_unreachable_host_reports_its_targets_rather_than_raising(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            dead_port = probe.getsockname()[1]
        client = CheetahClient("127.0.0.1", dead_port, timeout=0.2)
        self.assertFalse(client.connect())
        self.assertIsNone(client.command("SYSTEM_STATS"))
        self.assertIn(f"127.0.0.1:{dead_port}", client.describe_targets())

    def test_response_is_matched_to_its_own_command(self) -> None:
        client = self.client()
        self.assertTrue(client.execute("PAIR_SET", "a:1", 5).ok)
        self.assertEqual(client.execute("PAIR_GET", "a:1").int_field("key"), 5)

    def test_a_dropped_connection_is_reopened_on_the_next_command(self) -> None:
        client = self.client()
        client.connect()
        client._sock.close()  # type: ignore[union-attr]
        self.assertTrue(client.execute("PAIR_SET", "a:1", 5).ok)
        self.assertTrue(client.healthy())

    def test_readline_survives_socket_timeouts_within_the_idle_grace(self) -> None:
        client = self.client(timeout=0.01)
        client.set_idle_grace(0.5)

        class FlakySocket:
            def __init__(self) -> None:
                self._timeouts = 3
                self._payload = b"SUCCESS\n"
                self._offset = 0

            def recv(self, size: int) -> bytes:
                if self._timeouts > 0:
                    self._timeouts -= 1
                    raise socket.timeout()
                chunk = self._payload[self._offset : self._offset + size]
                self._offset += size
                return chunk

            def close(self) -> None:  # pragma: no cover - not reached
                pass

        client._sock = FlakySocket()  # type: ignore[assignment]
        self.assertEqual(client._readline(), "SUCCESS")

    def test_or_raise_turns_an_error_line_into_an_exception(self) -> None:
        client = self.client()
        with self.assertRaises(CheetahError):
            client.execute_or_raise("PAIR_GET", "missing:1")

    def test_admin_surface_reads_the_server_gauges(self) -> None:
        client = self.client()
        stats = admin.system_stats(client)
        self.assertEqual(stats.logical_cores, 8)
        self.assertIsNone(stats.system_cpu_pct)  # `NA` is not zero
        self.assertAlmostEqual(stats.cache_hit_ratio or 0, 0.9)
        self.assertEqual(admin.log_flush(client), ["one", "two"])
        self.assertEqual(admin.file_checkpoint(client, drop_cache=True), 4)

    def test_job_api_probe_distinguishes_an_old_server(self) -> None:
        client = self.client()
        self.assertTrue(jobs.supports_job_api(client))
        self.server.server._do_job = None  # type: ignore[attr-defined]
        self.assertFalse(jobs.supports_job_api(client))


class PoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeServerSocket()
        self.addCleanup(self.server.close)

    def test_each_thread_gets_its_own_socket_and_close_all_closes_them(self) -> None:
        import threading

        pool = ThreadLocalClientPool(
            lambda: CheetahClient(self.server.host, self.server.port, timeout=1.0)
        )
        seen: list[int] = []

        def use() -> None:
            seen.append(id(pool.acquire()))

        first = threading.Thread(target=use)
        second = threading.Thread(target=use)
        first.start()
        second.start()
        first.join()
        second.join()
        self.assertEqual(len(set(seen)), 2)
        self.assertIn("cheetah-db://", pool.describe())
        pool.close_all()

    def test_the_same_thread_reuses_one_client(self) -> None:
        pool = ThreadLocalClientPool(
            lambda: CheetahClient(self.server.host, self.server.port, timeout=1.0)
        )
        self.addCleanup(pool.close_all)
        self.assertIs(pool.acquire(), pool.acquire())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

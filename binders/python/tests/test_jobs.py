"""Detached commands: submit, poll, fetch once."""

from __future__ import annotations

import base64
import unittest

from cheetah_db import jobs, kv
from cheetah_db.client import CheetahError

from .fakes import FakeConnection


class JobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()
        kv.put_values_batch(self.conn, [(f"cnt:{index}", f"v{index}") for index in range(3)])

    def test_the_command_line_travels_base64(self) -> None:
        job_id = jobs.submit(self.conn, "PAIR_REDUCE counts cnt: 256")
        submitted = [c for c in self.conn.commands if c.startswith("JOB submit")][0]
        encoded = submitted.split("command=", 1)[1]
        self.assertEqual(
            base64.b64decode(encoded).decode("utf-8"), "PAIR_REDUCE counts cnt: 256"
        )
        self.assertTrue(job_id)

    def test_only_registered_commands_are_submittable(self) -> None:
        with self.assertRaises(CheetahError) as raised:
            jobs.submit(self.conn, "PAIR_SCAN cnt:")
        self.assertIn("command_not_submittable", str(raised.exception))

    def test_await_polls_then_returns_the_submitted_commands_own_response(self) -> None:
        job_id = jobs.submit(self.conn, "PAIR_REDUCE counts cnt: 256")
        slept: list[float] = []
        result = jobs.await_job(self.conn, job_id, poll_interval=1.0, sleep=slept.append)
        self.assertTrue(result.ok)
        self.assertEqual(len(result.items()), 3)
        self.assertEqual(slept, [])  # a job already complete is never waited on

    def test_a_fetch_consumes_the_job(self) -> None:
        job_id = jobs.submit(self.conn, "PAIR_REDUCE counts cnt: 256")
        self.assertIsNotNone(jobs.fetch(self.conn, job_id))
        with self.assertRaises(CheetahError) as raised:
            jobs.fetch(self.conn, job_id)
        self.assertIn("job_not_found", str(raised.exception))

    def test_a_failed_job_raises_with_the_servers_reason(self) -> None:
        job_id = jobs.submit(self.conn, "PAIR_REDUCE counts cnt: 256")
        self.conn.server._do_job = lambda rest: (  # type: ignore[assignment]
            f"SUCCESS,job={job_id},state=failed,progress=0.00,error=reducer_exploded"
        )
        with self.assertRaises(CheetahError) as raised:
            jobs.await_job(self.conn, job_id, poll_interval=0.1, sleep=lambda _s: None)
        self.assertIn("reducer_exploded", str(raised.exception))

    def test_a_job_that_never_finishes_times_out(self) -> None:
        job_id = jobs.submit(self.conn, "PAIR_REDUCE counts cnt: 256")
        self.conn.server._do_job = lambda rest: (  # type: ignore[assignment]
            f"SUCCESS,job={job_id},state=running,progress=10.00,completed=1,total=10"
        )
        with self.assertRaises(CheetahError) as raised:
            jobs.await_job(self.conn, job_id, poll_interval=0.01, timeout=0.0, sleep=lambda _s: None)
        self.assertIn("did not finish", str(raised.exception))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

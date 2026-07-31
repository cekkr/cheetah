"""Key primitives: byte ordering and the integer bucketing rule."""

from __future__ import annotations

import unittest

from cheetah_db import keys


class HexSegmentTests(unittest.TestCase):
    def test_fixed_width_hex_sorts_the_way_a_scan_walks(self) -> None:
        # A PAIR_SCAN is byte-ordered, so str(n) would sort 10 before 9.
        rendered = sorted(keys.hex_segment(value, 4) for value in (9, 10, 255, 4096))
        self.assertEqual(rendered, ["0009", "000a", "00ff", "1000"])

    def test_overflow_is_an_error_not_a_truncation(self) -> None:
        with self.assertRaises(ValueError):
            keys.hex_segment(0x10000, 4)

    def test_unhex_round_trips_and_rejects_uppercase(self) -> None:
        self.assertEqual(keys.unhex(keys.hex_segment(4095, 4)), 4095)
        with self.assertRaises(TypeError):
            keys.unhex("00FF")

    def test_sha1_is_a_stable_forty_character_key(self) -> None:
        digest = keys.sha1("/some/path")
        self.assertEqual(keys.assert_sha1(digest), digest)
        self.assertEqual(len(digest), 40)


class BucketingTests(unittest.TestCase):
    def test_bucketing_happens_in_integers(self) -> None:
        # (v - tol) / width lands on 224.99999999999997 in floats where exact
        # arithmetic gives 225, which widens a tolerance sweep from two buckets
        # to three for about half of all probes.
        value, width, tolerance = 0.2255, 1000, 500
        self.assertEqual(keys.quantize(value), 225500)
        self.assertEqual(keys.bucketize(value, width), 225)
        self.assertEqual(keys.bucket_sweep(value, width, tolerance), [225, 226])

    def test_sweep_stays_within_two_buckets_while_width_is_twice_the_tolerance(self) -> None:
        width, tolerance = 1000, 500
        for step in range(0, 20):
            value = step * 0.05001
            self.assertLessEqual(len(keys.bucket_sweep(value, width, tolerance)), 2)

    def test_bucket_width_must_be_a_positive_integer_of_quanta(self) -> None:
        with self.assertRaises(ValueError):
            keys.bucketize(1.0, 0)
        with self.assertRaises(ValueError):
            keys.bucketize(1.0, 0.5)  # type: ignore[arg-type]


class ValidationTests(unittest.TestCase):
    def test_valid_key_rejects_whitespace_hex_prefixes_and_reserved_spaces(self) -> None:
        self.assertEqual(keys.assert_valid_key("a/0001"), "a/0001")
        for bad in ("has space", "xthing", "graph/edges"):
            with self.assertRaises(ValueError):
                keys.assert_valid_key(bad)

    def test_join_segments_uses_the_conventional_separator(self) -> None:
        self.assertEqual(keys.join_segments("a", "0001", "ff"), "a/0001/ff")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

"""Key-building primitives.

Cheetah assigns no meaning to a key: it walks the raw bytes through the pair
trie. That freedom is also the trap, because in a trie **the key bytes are the
index** — a layout change means rebuilding the database. Every client therefore
ends up needing the same handful of primitives, and getting one of them subtly
wrong is expensive, so they live here rather than in each client.

Three rules they enforce:

  1. Fixed-width, zero-padded lowercase hex for every numeric segment.
     ``PAIR_SCAN`` is byte-ordered, so ``str(n)`` sorts ``10`` before ``9``.
  2. ``/`` separates segments and never appears inside one. Hex guarantees it.
  3. No key may start with a byte the server reserves — see
     :func:`cheetah_db.protocol.assert_unreserved_key`.

This module owns no namespace of its own. Deciding what a key *means* is the
application's job; this only makes the bytes well-formed.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Any

from .protocol import assert_unreserved_key

__all__ = [
    "KEY_QUANTUM",
    "QUANTA_PER_UNIT",
    "SEGMENT",
    "assert_sha1",
    "assert_valid_key",
    "bucket_sweep",
    "bucketize",
    "hex_segment",
    "join_segments",
    "quantize",
    "sha1",
    "unhex",
]

#: The conventional segment separator. Nothing forces it; consistency helps.
SEGMENT = "/"

# Bucketing happens in integers, not floats.
#
# A continuous value that reaches a key is first quantized to KEY_QUANTUM, then
# bucketed by integer division. This is not tidiness: with float division,
# `(v - tol) / width` lands on 224.99999999999997 where exact arithmetic gives
# 225, so a tolerance sweep silently widens from two buckets to three for
# roughly half of all probes — when the tolerance is an exact multiple of the
# rounding grid, boundary alignment is the common case, not the rare one.
KEY_QUANTUM = 1e-6
QUANTA_PER_UNIT = 1_000_000

_LOWER_HEX = re.compile(r"^[0-9a-f]+$")
_SHA1_HEX = re.compile(r"^[0-9a-f]{40}$")


def hex_segment(value: int, width: int, what: str = "value") -> str:
    """A non-negative integer as fixed-width lowercase hex. Raises on overflow."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{what} must be a non-negative integer, got {value!r}")
    text = format(value, "x")
    if len(text) > width:
        raise ValueError(f"{what} {value} does not fit in {width} hex digits")
    return text.rjust(width, "0")


def unhex(text: str, what: str = "value") -> int:
    """Inverse of :func:`hex_segment`. Rejects anything that is not lowercase hex."""
    if not isinstance(text, str) or not _LOWER_HEX.match(text):
        raise TypeError(f"{what} is not lowercase hex: {text!r}")
    return int(text, 16)


def sha1(text: Any) -> str:
    """SHA-1 hex of a string — the usual way to key free-form text like a path."""
    return hashlib.sha1(str(text).encode("utf-8")).hexdigest()


def assert_sha1(value: str, what: str = "value") -> str:
    if not isinstance(value, str) or not _SHA1_HEX.match(value):
        raise TypeError(f"{what} must be a 40-char lowercase sha1 hex string, got {value!r}")
    return value


def join_segments(*segments: str) -> str:
    """Join pre-formatted segments with :data:`SEGMENT`."""
    return SEGMENT.join(segments)


def quantize(value: float) -> int:
    """A value in its integer quantum domain. The only entry point to bucketing."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise TypeError(f"cannot quantize non-finite value {value!r}")
    return round(value * QUANTA_PER_UNIT)


def bucketize(value: float, width_units: int) -> int:
    """``floor(quantize(v) / width_units)``.

    Signed; the caller applies any bias needed to keep the hex spelling
    byte-ordered. ``width_units`` is a bucket width **in quanta**, never in
    value units.
    """
    if not isinstance(width_units, int) or isinstance(width_units, bool) or width_units <= 0:
        raise ValueError(f"bucket width must be a positive integer of quanta, got {width_units!r}")
    return quantize(value) // width_units


def bucket_sweep(value: float, width_units: int, tolerance_units: int) -> list[int]:
    """The distinct buckets that can hold a row within ``tolerance_units`` of ``value``.

    Ordered ascending; length 1 or 2 while ``width_units >= 2 * tolerance_units``,
    which is the property a caller's frozen widths must keep so that sweeping
    ``bucket(v - tol) … bucket(v + tol)`` cannot miss a matching row.
    """
    if not isinstance(width_units, int) or width_units <= 0:
        raise ValueError(f"bucket width must be a positive integer of quanta, got {width_units!r}")
    centre = quantize(value)
    low = (centre - tolerance_units) // width_units
    high = (centre + tolerance_units) // width_units
    return list(range(low, high + 1))


def assert_valid_key(key: str) -> str:
    """Validate a key against the reserved namespaces and the bare-argument rules.

    Cheap enough to call on every write in tests.
    """
    assert_unreserved_key(key)
    text = key if isinstance(key, str) else key.decode("utf-8", "replace")
    if any(character.isspace() for character in text):
        raise ValueError(f"cheetah key must not contain whitespace: {text!r}")
    if text.startswith("x"):
        raise ValueError(f"cheetah key must not start with 'x' (it would be read as hex): {text!r}")
    return key

"""String → uint32 token vocabulary.

Trie keys want short, fixed-width segments, and n-gram contexts want them
fixed-width by construction. A 40-character sha1 (or any other free-form
identifier) is neither, so clients end up interning identifiers into small
integers. This is that allocator, with both directions of the mapping
persisted.

**Concurrency.** Cheetah has no compare-and-swap, so the counter is guarded by a
lock here. That makes allocation safe within one process. Two processes
allocating into the same database concurrently would race and can hand the same
id to two names. Either confine allocation to one process, or move it
server-side.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Callable, Sequence

from .client import CheetahError
from .keys import hex_segment
from .kv import get_value, put_value

__all__ = ["FIRST_TOKEN", "MAX_TOKEN", "TokenVocabulary"]

#: 0 is reserved as "no token", so ids start at 1.
FIRST_TOKEN = 1
MAX_TOKEN = 0xFFFFFFFF

DEFAULT_COUNTER_KEY = "cfg:next_token"
DEFAULT_CACHE_LIMIT = 200_000


class TokenVocabulary:
    def __init__(
        self,
        conn: Any,
        *,
        counter_key: str = DEFAULT_COUNTER_KEY,
        token_key: Callable[[str], str] | None = None,
        reverse_token_key: Callable[[int], str] | None = None,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
    ) -> None:
        self.conn = conn
        self.counter_key = counter_key
        self.token_key = token_key or (lambda name: f"t:{name}")
        self.reverse_token_key = reverse_token_key or (
            lambda token: f"r:{hex_segment(token, 8, 'token')}"
        )
        self.cache_limit = max(1, int(cache_limit))
        self._forward: "OrderedDict[str, int]" = OrderedDict()
        self._reverse: dict[int, str] = {}
        self._lock = threading.Lock()

    # -- cache ---------------------------------------------------------- #
    def _remember(self, name: str, token: int) -> int:
        if len(self._forward) >= self.cache_limit:
            # Cheapest bounded policy that cannot thrash: drop the oldest insert.
            oldest, stale_token = self._forward.popitem(last=False)
            self._reverse.pop(stale_token, None)
        self._forward[name] = token
        self._reverse[token] = name
        return token

    # -- allocation ----------------------------------------------------- #
    def _read_counter(self) -> int:
        raw = get_value(self.conn, self.counter_key)
        if raw is None:
            return FIRST_TOKEN
        try:
            value = int(raw)
        except ValueError as exc:
            raise CheetahError(
                f"cheetah {self.counter_key} holds a non-token value: {raw!r}"
            ) from exc
        if value < FIRST_TOKEN:
            raise CheetahError(f"cheetah {self.counter_key} holds a non-token value: {raw!r}")
        return value

    def allocate(self, count: int = 1) -> int:
        """Reserve ``count`` consecutive ids and return the first.

        The counter is advanced *before* any mapping is written, so a crash
        mid-allocation loses ids rather than reusing them.
        """
        amount = max(1, int(count))
        with self._lock:
            first = self._read_counter()
            if first + amount - 1 > MAX_TOKEN:
                raise CheetahError("cheetah token vocabulary exhausted (uint32)")
            put_value(self.conn, self.counter_key, str(first + amount), upsert=True)
            return first

    # -- mapping -------------------------------------------------------- #
    def token_for(self, name: str) -> int:
        """The token for a name, allocating and persisting on first sight."""
        cached = self._forward.get(name)
        if cached is not None:
            self._forward.move_to_end(name)
            return cached
        stored = get_value(self.conn, self.token_key(name))
        if stored is not None:
            try:
                token = int(stored)
            except ValueError as exc:
                raise CheetahError(f"cheetah token for {name!r} is not numeric: {stored!r}") from exc
            return self._remember(name, token)
        token = self.allocate(1)
        # Reverse first: a reverse entry without a forward one is a readable
        # orphan, while a forward without a reverse breaks every key→name
        # explanation.
        put_value(self.conn, self.reverse_token_key(token), name, upsert=True)
        put_value(self.conn, self.token_key(name), str(token), upsert=True)
        return self._remember(name, token)

    def name_for(self, token: int) -> str | None:
        """The name behind a token, or ``None`` when unknown."""
        cached = self._reverse.get(token)
        if cached is not None:
            return cached
        stored = get_value(self.conn, self.reverse_token_key(token))
        if stored is None:
            return None
        self._remember(stored, token)
        return stored

    def tokens_for(self, names: Sequence[str]) -> list[int]:
        """Resolve many names, preserving input order.

        Misses are allocated as one consecutive block rather than one id per
        name: the counter read-modify-write is the expensive part, and doing it
        once per batch keeps a cold ingest from serializing on it.
        """
        requested = list(names or ())
        unique = list(dict.fromkeys(requested))
        missing: list[str] = []
        resolved: dict[str, int] = {}
        for name in unique:
            cached = self._forward.get(name)
            if cached is not None:
                resolved[name] = cached
                continue
            stored = get_value(self.conn, self.token_key(name))
            if stored is None:
                missing.append(name)
                continue
            try:
                resolved[name] = self._remember(name, int(stored))
            except ValueError as exc:
                raise CheetahError(f"cheetah token for {name!r} is not numeric: {stored!r}") from exc
        if missing:
            first = self.allocate(len(missing))
            for offset, name in enumerate(missing):
                token = first + offset
                put_value(self.conn, self.reverse_token_key(token), name, upsert=True)
                put_value(self.conn, self.token_key(name), str(token), upsert=True)
                resolved[name] = self._remember(name, token)
        return [resolved[name] for name in requested]

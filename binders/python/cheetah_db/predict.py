"""Prediction tables — the ``PREDICT_*`` family.

A prediction table maps a prefix (``key=``) to candidate values with
probabilities and per-context weights, and can be *trained*: ``PREDICT_TRAIN``
moves the stored weights toward a target, which is what makes this a learned
table rather than a cache.

The family is thin on purpose here. Cheetah owns the numerics; the binder owns
the encodings that are easy to get wrong — the ``key=value`` dialect's
whitespace rule, and the base64-JSON envelopes that ``weights=``, ``windows=``,
``key_windows=`` and ``items=`` travel in.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .client import CheetahError
from .protocol import (
    Response,
    build_key_value_command,
    encode_json_argument,
    join_csv,
)

__all__ = [
    "backend",
    "bench",
    "context_adjust",
    "inherit",
    "inherit_batch",
    "query",
    "set_value",
    "train",
]


def _send(conn: Any, command: str, fields: Mapping[str, Any]) -> Response:
    response = conn.send(build_key_value_command(command, fields))
    if not response.ok:
        raise CheetahError(f"cheetah {command} failed: {response.reason}", command=command, response=response)
    return response


def _json_field(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else encode_json_argument(value)


def set_value(
    conn: Any,
    *,
    key: str,
    value: str,
    prob: float,
    weights: Any = None,
    table: str | None = None,
) -> Response:
    """Declare a candidate value for a prefix with its probability. The write path."""
    return _send(
        conn,
        "PREDICT_SET",
        {"key": key, "value": value, "prob": prob, "weights": _json_field(weights), "table": table},
    )


def query(
    conn: Any,
    *,
    key: str,
    keys: Sequence[str] | str | None = None,
    ctx: Any = None,
    windows: Any = None,
    key_windows: Any = None,
    merge: str | None = None,
    table: str | None = None,
) -> dict[str, Any]:
    """Evaluate one or many prefixes and merge their probability windows.

    Returns the response fields plus the decoded ``payload``; the numeric shape
    of that payload is the server's contract, not this binder's.
    """
    response = _send(
        conn,
        "PREDICT_QUERY",
        {
            "key": key,
            "keys": join_csv(keys),
            "ctx": _json_field(ctx),
            "windows": _json_field(windows),
            "key_windows": _json_field(key_windows),
            "merge": merge,
            "table": table,
        },
    )
    return {
        "count": response.int_field("count", 0),
        "backend": response.field_value("backend"),
        "payload": response.payload(),
        "response": response,
    }


def train(
    conn: Any,
    *,
    key: str,
    target: str,
    ctx: Any = None,
    lr: float | None = None,
    negatives: Sequence[str] | str | None = None,
    table: str | None = None,
) -> Response:
    """Move the stored weights toward ``target``. Persistent learning."""
    return _send(
        conn,
        "PREDICT_TRAIN",
        {
            "key": key,
            "target": target,
            "ctx": _json_field(ctx),
            "lr": lr,
            "negatives": join_csv(negatives),
            "table": table,
        },
    )


def context_adjust(
    conn: Any,
    *,
    key: str,
    ctx: Any,
    mode: str | None = None,
    strength: float | None = None,
    table: str | None = None,
) -> Response:
    """A nudge to this query, not a lesson: ``PREDICT_CTX`` trains nothing."""
    return _send(
        conn,
        "PREDICT_CTX",
        {"key": key, "ctx": _json_field(ctx), "mode": mode, "strength": strength, "table": table},
    )


def inherit(
    conn: Any,
    *,
    key: str,
    target: str,
    sources: Sequence[str] | str,
    merge: str | None = None,
    table: str | None = None,
) -> Response:
    """Seed a new value by merging existing ones under the same prefix.

    Every source must already exist under ``key`` — otherwise the command
    answers ``inherit_sources_missing``, which is a statement about the table,
    not a transport failure.
    """
    return _send(
        conn,
        "PREDICT_INHERIT",
        {"key": key, "target": target, "sources": join_csv(sources), "merge": merge, "table": table},
    )


def inherit_batch(
    conn: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    key: str | None = None,
    merge: str | None = None,
    table: str | None = None,
) -> Response:
    """The same merge for many targets in one call.

    Submit it through :mod:`cheetah_db.jobs` when the batch is large: it is one
    of the two commands the server accepts as a detached job.
    """
    if not items:
        raise CheetahError("cheetah PREDICT_INHERIT_BATCH requires items")
    return _send(
        conn,
        "PREDICT_INHERIT_BATCH",
        {"items": encode_json_argument(list(items)), "key": key, "merge": merge, "table": table},
    )


def backend(conn: Any, *, mode: str | None = None, table: str | None = None) -> dict[str, Any]:
    """Read or switch which merger a table uses.

    The ``gpu`` path is ``webgpu-simulated`` — CPU fan-out, not a real WebGPU
    binding — so treat a switch as a scheduling choice, not an accelerator.
    """
    response = _send(conn, "PREDICT_BACKEND", {"mode": mode, "table": table})
    return {
        "backend": response.field_value("backend") or response.field_value("mode"),
        "table": response.field_value("table"),
        "response": response,
    }


def bench(
    conn: Any, *, samples: int, window: int, table: str | None = None
) -> dict[str, Any]:
    """Compare the two mergers on this host, so the choice above is measured."""
    response = _send(
        conn, "PREDICT_BENCH", {"samples": samples, "window": window, "table": table}
    )
    return {"fields": dict(response.fields), "payload": response.payload(), "response": response}

"""Typed access to Cheetah's graph, query and associative-recall commands.

The authority for every format here is this repository's handbook
(``AGENTS.md``) and the ``ExecuteCommand`` switch — this module only spells the
commands, it does not restate their semantics.

Four encoding rules the family imposes, and why each one matters here:

  1. ``GRAPH_*`` speaks ``key=value`` tokens split on **whitespace**
     (``src/database.go`` → ``parseKeyValueArgs`` uses ``strings.Fields``), so no
     value may contain a space. Anything free-form — props, references, batch
     items — travels base64. A sentence is the common case.
  2. The split is on the **first** ``=`` (``strings.Cut``), so base64 padding is
     safe inside a value.
  3. Ids are single tokens. A caller that mints ids from free text must slug
     them first; :func:`normalize_id` rejects a value that would be silently
     truncated rather than sending half an id.
  4. ``GRAPH_RECALL`` accepts at most 32 seeds per call (``graphRecallMaxSeeds``)
     and clamps ``hops``/``branch_limit``/``budget``/``reference_limit`` to its
     own maxima. Callers with more seeds must batch and merge, which is what
     :func:`recall_batched` does.
"""

from __future__ import annotations

import base64
from typing import Any, Iterable, Mapping, Sequence

from . import jobs as job_ops
from .client import CheetahError
from .protocol import (
    Response,
    build_key_value_command,
    encode_json_argument,
    join_csv,
    numeric_field,
)

__all__ = [
    "MAX_RECALL_BRANCH",
    "MAX_RECALL_BUDGET",
    "MAX_RECALL_HOPS",
    "MAX_RECALL_REFERENCES",
    "MAX_RECALL_SEEDS",
    "ambiguity_get",
    "ambiguity_resolve",
    "ambiguity_set",
    "await_recall",
    "degree",
    "delete_edge",
    "delete_node",
    "edge_set_batch",
    "encode_json_argument",
    "fetch_recall",
    "get_edge",
    "get_node",
    "neighbor_types",
    "neighbors",
    "normalize_id",
    "query",
    "recall",
    "recall_async",
    "recall_batched",
    "set_edge",
    "set_node",
    "similar",
    "term_index",
]

#: Server-side caps, mirrored from ``src/graph_recall.go``. Requests above them
#: are clamped by the server; clamping here keeps the intent visible client-side.
MAX_RECALL_SEEDS = 32
MAX_RECALL_HOPS = 6
MAX_RECALL_BRANCH = 1024
MAX_RECALL_BUDGET = 262144
MAX_RECALL_REFERENCES = 256


def normalize_id(value: Any) -> str:
    """A graph id as a single protocol token, or a raised error.

    Truncating is never right: a node written under half its id is a node the
    caller can neither find nor delete, and nothing about the response says so.
    """
    text = str(value or "").strip()
    if not text:
        raise CheetahError("cheetah graph id must not be empty")
    if any(character.isspace() for character in text) or "," in text:
        raise CheetahError(
            f"cheetah graph id must be a single token without commas: {text!r}"
        )
    return text


def _send(conn: Any, command: str, fields: Mapping[str, Any], what: str) -> Response:
    response = conn.send(build_key_value_command(command, fields))
    if not response.ok:
        raise CheetahError(f"cheetah {what} failed: {response.reason}", command=command, response=response)
    return response


def _props(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return encode_json_argument(value)


# --------------------------------------------------------------------------- #
# Writing what is the case
# --------------------------------------------------------------------------- #
def set_node(
    conn: Any,
    node_id: str,
    *,
    labels: Iterable[str] | str | None = None,
    props: Any = None,
    references: Sequence[Mapping[str, Any]] | str | None = None,
) -> Response:
    """Upsert one node. Omitted fields keep whatever is stored.

    ``references`` **replaces** the stored sentence list — there is no
    server-side merge — and ``references='-'`` clears it. A caller extending
    provenance across runs must read the stored list back with :func:`get_node`
    and write the union.
    """
    fields: dict[str, Any] = {
        "id": normalize_id(node_id),
        "labels": join_csv(labels),
        "props": _props(props),
    }
    if references is not None:
        fields["references"] = references if isinstance(references, str) else encode_json_argument(references)
    return _send(conn, "GRAPH_NODE_SET", fields, f"GRAPH_NODE_SET {node_id}")


def get_node(conn: Any, node_id: str) -> Any:
    """One node record, or ``None`` when nothing was ever written about it.

    ``ERROR,node_not_found`` is an answer, not a failure.
    """
    response = conn.send(build_key_value_command("GRAPH_NODE_GET", {"id": normalize_id(node_id)}))
    if response.ok:
        return response.payload()
    if response.error and "not_found" in response.error:
        return None
    raise CheetahError(f"cheetah GRAPH_NODE_GET {node_id} failed: {response.reason}", response=response)


def delete_node(conn: Any, node_id: str, *, cascade: bool = False) -> bool:
    """Forget an entity. Without ``cascade`` its incident edges are left dangling."""
    fields: dict[str, Any] = {"node": normalize_id(node_id)}
    if cascade:
        fields["cascade"] = 1
    response = conn.send(build_key_value_command("DEL graph", fields))
    if response.ok:
        return True
    if response.error and "not_found" in response.error:
        return False
    raise CheetahError(f"cheetah DEL graph {node_id} failed: {response.reason}", response=response)


def set_edge(
    conn: Any,
    *,
    from_id: str,
    to_id: str,
    edge_type: str | None = None,
    weight: float | None = None,
    directed: bool | None = None,
    confidence: Any = None,
    modality: str | None = None,
    ambiguity: str | None = None,
    props: Any = None,
    autocreate: bool | None = None,
) -> Response:
    """Upsert one relation, identified by ``(from, to, type, directed)``."""
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "to": normalize_id(to_id),
        "type": edge_type,
        "weight": weight,
        "directed": None if directed is None else int(bool(directed)),
        "confidence": confidence,
        "modality": modality,
        "ambiguity": ambiguity,
        "props": _props(props),
        "autocreate": None if autocreate is None else int(bool(autocreate)),
    }
    return _send(conn, "GRAPH_EDGE_SET", fields, f"GRAPH_EDGE_SET {from_id}->{to_id}")


def edge_set_batch(
    conn: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    continue_on_error: bool = True,
    **defaults: Any,
) -> dict[str, int]:
    """Upsert many edges in one round trip; ``defaults`` fill the shared fields.

    Returns the server's own accounting, ``applied`` included: a batch that
    reports fewer applied than requested has silently dropped edges, and a
    caller that ignores the difference builds an index with holes in it.
    """
    if not items:
        return {"requested": 0, "applied": 0, "created": 0, "updated": 0, "failed": 0}
    fields: dict[str, Any] = {
        "items": encode_json_argument(list(items)),
        "continue_on_error": 1 if continue_on_error else 0,
    }
    for key, value in defaults.items():
        if key == "props":
            value = _props(value)
        if value is not None:
            fields[key] = value
    response = _send(
        conn, "GRAPH_EDGE_SET_BATCH", fields, f"GRAPH_EDGE_SET_BATCH of {len(items)}"
    )
    return {
        "requested": int(numeric_field(response.fields, "requested", len(items)) or 0),
        "applied": int(numeric_field(response.fields, "applied", 0) or 0),
        "created": int(numeric_field(response.fields, "created", 0) or 0),
        "updated": int(numeric_field(response.fields, "updated", 0) or 0),
        "failed": int(numeric_field(response.fields, "failed", 0) or 0),
    }


def delete_edge(
    conn: Any,
    *,
    from_id: str,
    to_id: str,
    edge_type: str | None = None,
    directed: bool | None = None,
) -> bool:
    """Forget a relation.

    Different from writing ``confidence=ruled_out``, which keeps it on record as
    excluded and still answerable.
    """
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "to": normalize_id(to_id),
        "type": edge_type,
        "directed": None if directed is None else int(bool(directed)),
    }
    response = conn.send(build_key_value_command("DEL graph", fields))
    if response.ok:
        return True
    if response.error and "not_found" in response.error:
        return False
    raise CheetahError(
        f"cheetah DEL graph {from_id}->{to_id} failed: {response.reason}", response=response
    )


# --------------------------------------------------------------------------- #
# Calling it back
# --------------------------------------------------------------------------- #
def get_edge(
    conn: Any,
    *,
    from_id: str,
    to_id: str,
    edge_type: str | None = None,
    directed: bool | None = None,
) -> Any:
    """One edge record addressed by its identifying tuple, or ``None``."""
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "to": normalize_id(to_id),
        "type": edge_type,
        "directed": None if directed is None else int(bool(directed)),
    }
    response = conn.send(build_key_value_command("GRAPH_EDGE_GET", fields))
    if response.ok:
        return response.payload()
    if response.error and "not_found" in response.error:
        return None
    raise CheetahError(f"cheetah GRAPH_EDGE_GET failed: {response.reason}", response=response)


def neighbors(
    conn: Any,
    node_id: str,
    *,
    direction: str = "out",
    edge_type: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> tuple[list[Any], str | None]:
    """A page of edge records around a node, plus the resumption cursor."""
    fields: dict[str, Any] = {
        "id": normalize_id(node_id),
        "direction": direction,
        "type": edge_type,
        "limit": limit,
        "cursor": cursor,
    }
    response = conn.send(build_key_value_command("GRAPH_NEIGHBORS", fields))
    if not response.ok:
        if response.error and "not_found" in response.error:
            return [], None
        raise CheetahError(f"cheetah GRAPH_NEIGHBORS {node_id} failed: {response.reason}", response=response)
    payload = response.payload() or []
    return list(payload), response.cursor()


def degree(
    conn: Any,
    node_id: str,
    *,
    direction: str = "out",
    edge_type: str | None = None,
    weighted: bool = False,
) -> dict[str, float]:
    """How many edges a node carries, and how heavy.

    The cheapest question in the family — no edge record is hydrated — which is
    what makes it usable as a per-seed stop-word test at query time.
    """
    fields: dict[str, Any] = {
        "id": normalize_id(node_id),
        "direction": direction,
        "type": edge_type,
        "weighted": 1 if weighted else 0,
    }
    response = conn.send(build_key_value_command("GRAPH_DEGREE", fields))
    if not response.ok:
        # A node nobody has written yet is a legitimate answer of zero.
        if response.error and "not_found" in response.error:
            return {"degree": 0, "weighted": 0.0}
        raise CheetahError(f"cheetah GRAPH_DEGREE {node_id} failed: {response.reason}", response=response)
    return {
        "degree": int(numeric_field(response.fields, "degree", 0) or 0),
        "weighted": float(numeric_field(response.fields, "weighted_degree", 0) or 0),
    }


def neighbor_types(
    conn: Any,
    node_id: str,
    *,
    direction: str = "out",
    limit: int | None = None,
    cursor: str | None = None,
    weighted: bool = False,
) -> tuple[list[Any], str | None]:
    """``[{type,count,weighted}]`` without hydrating a single edge.

    The fast probe before deciding what to hydrate.
    """
    fields: dict[str, Any] = {
        "id": normalize_id(node_id),
        "direction": direction,
        "limit": limit,
        "cursor": cursor,
        "weighted": 1 if weighted else 0,
    }
    response = conn.send(build_key_value_command("GRAPH_NEIGHBOR_TYPES", fields))
    if not response.ok:
        if response.error and "not_found" in response.error:
            return [], None
        raise CheetahError(
            f"cheetah GRAPH_NEIGHBOR_TYPES {node_id} failed: {response.reason}", response=response
        )
    return list(response.payload() or []), response.cursor()


def query(conn: Any, clause: str) -> dict[str, Any]:
    """``GRAPH_QUERY`` — the clause dialect, passed through verbatim.

    The only command with its own grammar (``MATCH … [WHERE …] [HOPS …]
    [RETURN …]``); the left node must be ID-anchored so execution stays
    index-backed. Nothing here validates the clause: the server owns it, and a
    client-side parser would only drift from it.
    """
    text = " ".join(str(clause).split())
    if not text:
        raise CheetahError("cheetah GRAPH_QUERY requires a clause")
    response = conn.send(f"GRAPH_QUERY {text}")
    if not response.ok:
        raise CheetahError(f"cheetah GRAPH_QUERY failed: {response.reason}", response=response)
    return {
        "return": response.field_value("return", ""),
        "matches": int(numeric_field(response.fields, "matches", 0) or 0),
        "payload": response.payload(),
        "cursor": response.cursor(),
    }


def recall(
    conn: Any,
    seeds: Sequence[str],
    *,
    hops: int = 1,
    decay: float = 1.0,
    precision: float = 0.05,
    direction: str = "out",
    edge_type: str | None = None,
    limit: int = 64,
    branch_limit: int = 1024,
    budget: int = 65536,
    min_sources: int = 1,
    expand: bool | None = None,
    references: bool = False,
    reference_limit: int | None = None,
    include_seeds: bool = False,
) -> dict[str, Any]:
    """Spread activation from every seed at once and return what they co-activate.

    ``associations[].source_count`` is how many of the seeds reached that node
    and ``score`` is the noisy-OR of their activations — which is exactly the
    question "given these observations, what do they have in common?".

    ``references=True`` also hydrates the stored sentences. Ask for
    ``include_seeds`` with it: a seed node is otherwise excluded from its own
    answer, so the node the turn is *about* never returns its own provenance.
    """
    seed_list = [str(seed).strip() for seed in seeds if str(seed).strip()]
    if not seed_list:
        return {"seeds": [], "associations": [], "truncated": False, "response": None}
    if len(seed_list) > MAX_RECALL_SEEDS:
        raise CheetahError(
            f"GRAPH_RECALL accepts at most {MAX_RECALL_SEEDS} seeds, got {len(seed_list)}; "
            "use recall_batched"
        )
    fields = _recall_fields(
        seed_list,
        hops=hops,
        decay=decay,
        precision=precision,
        direction=direction,
        edge_type=edge_type,
        limit=limit,
        branch_limit=branch_limit,
        budget=budget,
        min_sources=min_sources,
        expand=expand,
        references=references,
        reference_limit=reference_limit,
        include_seeds=include_seeds,
    )
    response = _send(
        conn, "GRAPH_RECALL", fields, f"GRAPH_RECALL over {len(seed_list)} seeds"
    )
    return _recall_result(response)


def _recall_fields(
    seed_list: Sequence[str],
    *,
    hops: int,
    decay: float,
    precision: float,
    direction: str,
    edge_type: str | None,
    limit: int,
    branch_limit: int,
    budget: int | None,
    min_sources: int,
    expand: bool | None,
    references: bool,
    reference_limit: int | None,
    include_seeds: bool,
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "seeds": _encode_seeds(seed_list),
        "hops": max(1, min(int(hops), MAX_RECALL_HOPS)),
        "decay": _precision(decay),
        "precision": _precision(precision),
        "direction": direction,
        "type": edge_type,
        "limit": limit,
        "branch_limit": max(1, min(int(branch_limit), MAX_RECALL_BRANCH)),
        "budget": None if budget is None else max(1, min(int(budget), MAX_RECALL_BUDGET)),
        "min_sources": min_sources,
    }
    if expand is not None:
        fields["expand"] = 1 if expand else 0
    if references:
        fields["references"] = 1
        if reference_limit is not None:
            fields["reference_limit"] = max(1, min(int(reference_limit), MAX_RECALL_REFERENCES))
    if include_seeds:
        fields["include_seeds"] = 1
    return fields


def _recall_result(response: Response, job_id: str | None = None) -> dict[str, Any]:
    payload = response.payload() or {}
    result = {
        "seeds": payload.get("seeds") or [],
        "associations": payload.get("associations") or [],
        "truncated": (numeric_field(response.fields, "truncated", 0) or 0) > 0,
        "response": response,
    }
    resolved_job_id = response.field_value("job", job_id)
    if resolved_job_id:
        result["job_id"] = resolved_job_id
    return result


def recall_async(
    conn: Any,
    seeds: Sequence[str],
    *,
    hops: int = 1,
    decay: float = 1.0,
    precision: float = 0.05,
    direction: str = "out",
    edge_type: str | None = None,
    limit: int = 64,
    branch_limit: int = 1024,
    budget: int | None = None,
    min_sources: int = 1,
    expand: bool | None = None,
    references: bool = False,
    reference_limit: int | None = None,
    include_seeds: bool = False,
) -> str:
    """Detach one recall and return its ``graph_recall_<n>`` id.

    When ``budget`` is omitted the server assigns the maximum bounded async
    sweep; passing a value keeps that explicit limit.
    """
    seed_list = [str(seed).strip() for seed in seeds if str(seed).strip()]
    if not seed_list:
        raise CheetahError("cheetah GRAPH_RECALL requires at least one seed")
    if len(seed_list) > MAX_RECALL_SEEDS:
        raise CheetahError(
            f"GRAPH_RECALL accepts at most {MAX_RECALL_SEEDS} seeds, got {len(seed_list)}"
        )
    fields = _recall_fields(
        seed_list,
        hops=hops,
        decay=decay,
        precision=precision,
        direction=direction,
        edge_type=edge_type,
        limit=limit,
        branch_limit=branch_limit,
        budget=budget,
        min_sources=min_sources,
        expand=expand,
        references=references,
        reference_limit=reference_limit,
        include_seeds=include_seeds,
    )
    command = build_key_value_command("GRAPH_RECALL", fields)
    return job_ops.submit(conn, command)


def fetch_recall(conn: Any, job_id: str) -> dict[str, Any] | None:
    """Retrieve and decode a detached recall by id, or ``None`` while running."""
    response = job_ops.fetch(conn, job_id)
    return None if response is None else _recall_result(response, job_id)


def await_recall(conn: Any, job_id: str, **options: Any) -> dict[str, Any]:
    """Poll a detached recall by id and decode its terminal result."""
    return _recall_result(job_ops.await_job(conn, job_id, **options), job_id)


def _encode_seeds(seeds: Sequence[str]) -> str:
    """``seeds=`` as one whitespace-free token.

    Free-text seeds legitimately contain spaces, and the whole argument list is
    split on whitespace, so a spaced seed list travels as
    ``base64:<b64 of the comma-joined list>`` — the spelling
    ``graphParseRecallSeeds`` decodes before splitting on commas. A seed's own
    commas are turned into spaces first: they are the list separator, so a seed
    containing one cannot survive as a single seed either way.
    """
    cleaned = [" ".join(str(seed).replace(",", " ").split()) for seed in seeds]
    joined = ",".join(seed for seed in cleaned if seed)
    if any(character.isspace() for character in joined):
        return "base64:" + base64.b64encode(joined.encode("utf-8")).decode("ascii")
    return joined


def _precision(value: float | str) -> str:
    if isinstance(value, str):
        return value
    return f"{float(value):.6f}".rstrip("0").rstrip(".") or "0"


def recall_batched(conn: Any, seeds: Sequence[str], **options: Any) -> list[dict[str, Any]]:
    """:func:`recall` over any number of seeds.

    Scores are combined with a noisy-OR across batches, which is the same rule
    the server uses to combine seeds *inside* one batch — so splitting 40 seeds
    into two calls ranks the same way as one impossible call of 40 would.
    ``source_count`` sums instead, because the batches are disjoint sets of
    seeds.

    ``sources`` — **which** seeds reached each hit, with how much activation —
    is kept rather than collapsed. It is the only part of the answer a caller
    can reweight: the server has no way to know that some seeds are far more
    telling than others.
    """
    unique = list(dict.fromkeys(str(seed).strip() for seed in seeds if str(seed).strip()))
    merged: dict[str, dict[str, Any]] = {}
    for start in range(0, len(unique), MAX_RECALL_SEEDS):
        batch = unique[start : start + MAX_RECALL_SEEDS]
        for association in recall(conn, batch, **options)["associations"]:
            node_id = str(association.get("id") or "")
            if not node_id:
                continue
            current = merged.setdefault(
                node_id,
                {"id": node_id, "score": 0.0, "source_count": 0, "sources": {}, "references": []},
            )
            score = float(association.get("score") or 0.0)
            current["score"] = 1 - (1 - current["score"]) * (1 - score)
            current["source_count"] += int(association.get("source_count") or 0)
            for source in association.get("sources") or ():
                seed = str(source.get("seed") or "")
                activation = float(source.get("activation") or 0.0)
                current["sources"][seed] = max(current["sources"].get(seed, 0.0), activation)
            for reference in association.get("references") or ():
                current["references"].append(reference)
    return sorted(
        merged.values(), key=lambda entry: (entry["score"], entry["source_count"]), reverse=True
    )


def similar(
    conn: Any,
    node_id: str,
    *,
    by: str = "all",
    limit: int | None = None,
    precision: float | None = None,
) -> list[Any]:
    """"What else behaves like this?" — same neighbours, or same words in the id.

    No edge between the two nodes is required.
    """
    fields: dict[str, Any] = {
        "id": normalize_id(node_id),
        "by": by,
        "limit": limit,
        "precision": None if precision is None else _precision(precision),
    }
    response = conn.send(build_key_value_command("GRAPH_SIMILAR", fields))
    if not response.ok:
        if response.error and "not_found" in response.error:
            return []
        raise CheetahError(f"cheetah GRAPH_SIMILAR {node_id} failed: {response.reason}", response=response)
    return list(response.payload() or [])


def term_index(
    conn: Any,
    *,
    action: str = "stats",
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Maintenance of the derived lexical index free-text seeds resolve through.

    It is never authoritative: exact ids and synonym edges keep working without
    it, and ``rebuild`` is resumable through ``next_cursor``.
    """
    fields: dict[str, Any] = {"action": action, "limit": limit, "cursor": cursor}
    response = _send(conn, "GRAPH_TERM_INDEX", fields, f"GRAPH_TERM_INDEX {action}")
    return {
        "entries": response.int_field("entries"),
        "enabled": response.bool_field("enabled", True),
        "nodes": response.int_field("nodes"),
        "terms": response.int_field("terms"),
        "removed": response.int_field("removed"),
        "cursor": response.cursor(),
        "response": response,
    }


# --------------------------------------------------------------------------- #
# Ambiguity: the readings that exclude each other
# --------------------------------------------------------------------------- #
def ambiguity_set(
    conn: Any,
    *,
    from_id: str,
    group: str,
    options: Mapping[str, float] | Sequence[str],
    edge_type: str | None = None,
    normalize: bool = True,
) -> Response:
    """Write a whole set of mutually exclusive readings and normalize their shares.

    The engine has no ``OR``, so a disjunction is stored as a group rather than
    expressed as a query.
    """
    if isinstance(options, Mapping):
        rendered = ",".join(f"{normalize_id(key)}={value}" for key, value in options.items())
    else:
        rendered = ",".join(normalize_id(option) for option in options)
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "group": group,
        "options": rendered,
        "type": edge_type,
        "normalize": 1 if normalize else 0,
    }
    return _send(conn, "GRAPH_AMBIGUITY_SET", fields, f"GRAPH_AMBIGUITY_SET {group}")


def ambiguity_get(
    conn: Any,
    *,
    from_id: str,
    group: str,
    direction: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """One alternative group read back, strongest reading first."""
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "group": group,
        "direction": direction,
        "limit": limit,
    }
    response = conn.send(build_key_value_command("GRAPH_AMBIGUITY_GET", fields))
    if not response.ok:
        if response.error and "not_found" in response.error:
            return {"count": 0, "alternatives": [], "top": None, "confidence_sum": 0.0}
        raise CheetahError(f"cheetah GRAPH_AMBIGUITY_GET failed: {response.reason}", response=response)
    return {
        "count": int(numeric_field(response.fields, "count", 0) or 0),
        "confidence_sum": float(numeric_field(response.fields, "confidence_sum", 0) or 0),
        "top": response.field_value("top"),
        "top_modality": response.field_value("top_modality"),
        "alternatives": list(response.payload() or []),
    }


def ambiguity_resolve(
    conn: Any, *, from_id: str, group: str, winner: str, drop: bool = False
) -> Response:
    """Collapse the set: the winner becomes ``certain``, the others ``ruled_out``."""
    fields: dict[str, Any] = {
        "from": normalize_id(from_id),
        "group": group,
        "winner": normalize_id(winner),
        "drop": 1 if drop else 0,
    }
    return _send(conn, "GRAPH_AMBIGUITY_RESOLVE", fields, f"GRAPH_AMBIGUITY_RESOLVE {group}")

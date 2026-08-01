"""cheetah-db — Python binder.

Everything a Python client needs to talk to a cheetah-server, and nothing about
any particular schema. Layers, each usable on its own::

    protocol            pure codec — build a command line, parse a response line
    hosts               where a client should actually dial (0.0.0.0, WSL)
    client              CheetahClient (one socket) + ThreadLocalClientPool
    kv / graph / jobs   free functions over a connection: the two-step write,
    records / predict   batches, scans, nodes, edges, recall, multi-field rows,
    admin               detached jobs, prediction tables, server and database
                        operations
    database            CheetahDatabase — the plumbing an application ends up
                        writing around all of the above, meant to be subclassed

Plus ``keys`` (key-building primitives), ``vocabulary`` (a persisted
string→uint32 allocator) and ``server`` (spawn a server for development and
tests)::

    from cheetah_db import CheetahClient, kv

    with CheetahClient("127.0.0.1", 4455, database="app") as conn:
        kv.put_json(conn, "user:42", {"name": "Ada"}, upsert=True)
        print(kv.get_json(conn, "user:42"))

Requires Python 3.10+ and nothing else.
"""

from __future__ import annotations

from . import admin, graph, hosts, jobs, keys, kv, predict, protocol, records, server, vocabulary
from .client import (
    CheetahClient,
    CheetahConnectionError,
    CheetahError,
    ThreadLocalClientPool,
)
from .database import CheetahDatabase, hydrate_json
from .protocol import RawArgument, Response, ScanItem, build_command, build_key_value_command, parse_response
from .vocabulary import TokenVocabulary

__all__ = [
    "CheetahClient",
    "CheetahConnectionError",
    "CheetahDatabase",
    "CheetahError",
    "RawArgument",
    "Response",
    "ScanItem",
    "ThreadLocalClientPool",
    "TokenVocabulary",
    "admin",
    "build_command",
    "build_key_value_command",
    "graph",
    "hosts",
    "hydrate_json",
    "jobs",
    "keys",
    "kv",
    "parse_response",
    "predict",
    "protocol",
    "records",
    "server",
    "vocabulary",
]

__version__ = "0.1.0"

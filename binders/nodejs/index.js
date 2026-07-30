// cheetah-db — Node.js binder.
//
// Everything a Node client needs to talk to a cheetah-server, and nothing about
// any particular schema. Four layers, each usable on its own:
//
//   protocol    pure codec — build a command line, parse a response line
//   client      one socket (CheetahClient) or several (CheetahPool)
//   kv / graph  free functions over a connection: the two-step write, scans,
//               nodes, edges, associative recall
//   database    CheetahDatabase — the plumbing an application ends up writing
//               around all of the above, meant to be subclassed
//
// Plus `keys` (key-building primitives), `vocabulary` (a persisted string→uint32
// allocator) and `server` (spawn a server for development and tests).
//
//   const { CheetahDatabase } = require('cheetah-db');
//
//   class MyStore extends CheetahDatabase {
//       constructor(options) {
//           super({ ...options, layout: { key: 'cfg:layout', version: 1 } });
//       }
//       async putThing(id, thing) {
//           return this.putJson(`thing:${id}`, thing, { upsert: true });
//       }
//   }

const client = require('./lib/client');
const database = require('./lib/database');
const graph = require('./lib/graph');
const keys = require('./lib/keys');
const kv = require('./lib/kv');
const protocol = require('./lib/protocol');
const server = require('./lib/server');
const vocabulary = require('./lib/vocabulary');

module.exports = {
    // Submodules, for callers who want the free-function layers.
    client,
    database,
    graph,
    keys,
    kv,
    protocol,
    server,
    vocabulary,

    // The names most callers reach for.
    CheetahClient: client.CheetahClient,
    CheetahPool: client.CheetahPool,
    CheetahError: client.CheetahError,
    CheetahConnectionError: client.CheetahConnectionError,
    CheetahDatabase: database.CheetahDatabase,
    CheetahServerProcess: server.CheetahServerProcess,
    TokenVocabulary: vocabulary.TokenVocabulary,
    hydrateJson: database.hydrateJson,
    startServer: server.startServer,
};

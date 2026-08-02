// cheetah-db — Node.js binder.
//
// Everything a Node client needs to talk to a cheetah-server, and nothing about
// any particular schema. Four layers, each usable on its own:
//
//   protocol    pure codec — build a command line, parse a response line
//   binary      the byte-wise protocol: a command as a 2-byte index, values in
//               their own type. A transcoder over the same lines, so every
//               layer below keeps working unchanged (`new CheetahClient({binary: true})`)
//   alias       ALIAS — the command index and a table's numeric widths, which
//               are the two things a client cannot derive on its own
//   client      one socket (CheetahClient) or several (CheetahPool)
//   kv / graph  free functions over a connection: the two-step write, scans,
//   records     nodes, edges, associative recall, multi-field rows
//   jobs        detached commands: submit, poll, fetch once
//   batch       BATCH — one command, N argument sets, one round trip (and the
//               coalescer CheetahClient runs by itself)
//   predict     prediction tables — PREDICT_*
//   admin       the server and the registry of databases, not the data
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

const admin = require('./lib/admin');
const alias = require('./lib/alias');
const batch = require('./lib/batch');
const binary = require('./lib/binary');
const client = require('./lib/client');
const database = require('./lib/database');
const graph = require('./lib/graph');
const jobs = require('./lib/jobs');
const keys = require('./lib/keys');
const kv = require('./lib/kv');
const predict = require('./lib/predict');
const protocol = require('./lib/protocol');
const records = require('./lib/records');
const server = require('./lib/server');
const vocabulary = require('./lib/vocabulary');

module.exports = {
    // Submodules, for callers who want the free-function layers.
    admin,
    alias,
    batch,
    binary,
    client,
    database,
    graph,
    jobs,
    keys,
    kv,
    predict,
    protocol,
    records,
    server,
    vocabulary,

    // The names most callers reach for.
    BinarySession: binary.BinarySession,
    CheetahBinaryError: binary.CheetahBinaryError,
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

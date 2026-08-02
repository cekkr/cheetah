// Cheetah TCP client.
//
// The protocol is newline-delimited text with **no request ids**: the server
// answers one line per command, in the order the commands arrived on that
// socket (one goroutine per connection, see src/server.go). So a connection
// owns a strict FIFO of pending commands and matches responses by arrival
// order. Two callers may share one connection safely — the queue, not the
// caller, decides write order — but a caller that needs several commands to
// land back-to-back with nothing interleaved must lease a connection from the
// pool.
//
// Commands are pipelined up to `maxInFlight` because bulk work is round-trip
// bound rather than payload bound. Pipelining preserves the FIFO contract; it
// only stops the client from waiting for each response before writing the next
// line.
//
// On top of that, a connection **auto-batches**: when the same command is
// issued often enough to look like bulk work, the commands a caller starts in
// one turn of the event loop are folded into a single `BATCH` line (batch.js)
// and their responses are handed back individually, so nothing above notices.
// A caller that awaits each command in turn never has two outstanding and
// therefore never batches — it is bursts, not volume, that the coalescing can
// act on. `autoBatch: {enabled: false}` restores the exact pre-batching wire
// behavior; every knob is in `AUTO_BATCH_DEFAULTS`.
//
// No dependency is used: the protocol is `net` and newlines.

const net = require('net');
const { EventEmitter } = require('events');
const { buildCommand, parseResponse } = require('./protocol');
const binaryCodec = require('./binary');

const DEFAULTS = Object.freeze({
    host: '127.0.0.1',
    port: 4455,
    database: null,
    databaseOptions: null,
    // `true`, or `{uint, int, float}` widths, switches this socket to the
    // binary protocol (binary.js). Everything above keeps producing command
    // lines: the client transcodes them and turns each response frame back into
    // the canonical line, so no command layer notices.
    binary: false,
    connectTimeoutMs: 5000,
    commandTimeoutMs: 30000,
    maxInFlight: 64,
    reconnectBaseMs: 100,
    reconnectMaxMs: 5000,
    maxReconnectAttempts: 5,
    // Overrides layered over batch.js's AUTO_BATCH_DEFAULTS. `null` keeps the
    // defaults; `{enabled: false}` turns coalescing off entirely.
    autoBatch: null,
});

class CheetahError extends Error {
    constructor(message, { command = null, response = null } = {}) {
        super(message);
        this.name = 'CheetahError';
        this.command = command;
        this.response = response;
    }
}

class CheetahConnectionError extends CheetahError {
    constructor(message, options) {
        super(message, options);
        this.name = 'CheetahConnectionError';
    }
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Drop keys whose value is `undefined`.
 *
 * `{...DEFAULTS, ...options}` lets an explicitly-undefined option overwrite a
 * default with `undefined`, which is exactly what an options object assembled
 * as `{host: opts.host, port: opts.port}` produces when the caller passed
 * neither. The failure is a connection attempt to `undefined:undefined`.
 */
function defined(options) {
    const result = {};
    for (const [key, value] of Object.entries(options || {})) {
        if (value !== undefined) result[key] = value;
    }
    return result;
}

class CheetahClient extends EventEmitter {
    constructor(options = {}) {
        super();
        this.options = { ...DEFAULTS, ...defined(options) };
        this.socket = null;
        this.connected = false;
        this.closing = false;
        this.buffer = '';
        // Written to the socket, waiting for a response line. Strict FIFO.
        this.inflight = [];
        // Accepted from callers, not yet written (maxInFlight backpressure).
        this.waiting = [];
        this.connecting = null;
        this.lastError = null;
        // Binary mode: a session (negotiated widths + the command index) once
        // the handshake has run, null while the socket speaks text.
        this.binary = null;
        this.binaryHandshake = null;
        // Required here rather than at module scope: batch.js reaches back for
        // CheetahError, and two top-level requires would deadlock the cycle.
        const { createAutoBatcher } = require('./batch');
        this.batcher = createAutoBatcher((line) => this.sendDirect(line), this.options.autoBatch || {});
    }

    get pendingCount() {
        return this.inflight.length + this.waiting.length;
    }

    /** Coalescing counters: `{batched, batches, direct}`. */
    get batchStats() {
        return { ...this.batcher.stats };
    }

    async connect() {
        if (this.connected) return this;
        if (this.connecting) return this.connecting;
        this.connecting = this.#connectWithBackoff().finally(() => {
            this.connecting = null;
        });
        return this.connecting;
    }

    async #connectWithBackoff() {
        const { maxReconnectAttempts, reconnectBaseMs, reconnectMaxMs } = this.options;
        let attempt = 0;
        for (;;) {
            try {
                await this.#openSocket();
                if (this.options.binary) {
                    // Before anything else: the server picks the mode from the
                    // first byte, and the widths it fixes apply to every later
                    // frame on this socket.
                    await this.#binaryHandshakeExchange();
                }
                if (this.options.database) {
                    await this.#selectDatabase();
                }
                this.emit('connect');
                return this;
            } catch (error) {
                attempt += 1;
                this.lastError = error;
                if (attempt > maxReconnectAttempts) {
                    throw new CheetahConnectionError(
                        `cheetah connect failed after ${attempt} attempts: ${error.message}`
                    );
                }
                const delay = Math.min(reconnectMaxMs, reconnectBaseMs * 2 ** (attempt - 1));
                this.emit('reconnecting', { attempt, delay, error });
                await sleep(delay);
            }
        }
    }

    #openSocket() {
        return new Promise((resolve, reject) => {
            const socket = net.createConnection({
                host: this.options.host,
                port: this.options.port,
            });
            socket.setNoDelay(true);
            // Text mode reads latin1 strings (a lossless byte↔char mapping);
            // binary mode keeps Buffers, because a frame is split by declared
            // length rather than by a delimiter.
            if (!this.options.binary) socket.setEncoding('latin1');

            const onConnectTimeout = setTimeout(() => {
                socket.destroy();
                reject(new CheetahConnectionError('cheetah connect timed out'));
            }, this.options.connectTimeoutMs);

            const cleanupHandshake = () => {
                clearTimeout(onConnectTimeout);
                socket.removeListener('error', onHandshakeError);
            };

            const onHandshakeError = (error) => {
                cleanupHandshake();
                reject(new CheetahConnectionError(`cheetah connect failed: ${error.message}`));
            };

            socket.once('error', onHandshakeError);
            socket.once('connect', () => {
                cleanupHandshake();
                this.socket = socket;
                this.connected = true;
                this.buffer = '';
                this.binaryBuffer = Buffer.alloc(0);
                socket.on('data', (chunk) => this.#onData(chunk));
                socket.on('error', (error) => this.#onFailure(error));
                socket.on('close', () => this.#onFailure(new CheetahConnectionError('cheetah connection closed')));
                resolve();
            });
        });
    }

    async #selectDatabase() {
        // DATABASE is handled by the front-end, not the dispatcher, and it
        // changes what this connection points at — so it must be the first
        // line on every (re)connection, ahead of any queued work.
        const overrides = Object.entries(this.options.databaseOptions || {})
            .map(([key, value]) => `${key}=${value}`);
        const line = ['DATABASE', this.options.database, ...overrides].join(' ');
        const response = await this.#writeNow(line);
        if (!response.ok) {
            throw new CheetahConnectionError(
                `cheetah DATABASE ${this.options.database} refused: ${response.error || response.raw}`
            );
        }
    }

    /**
     * Run the binary handshake and adopt what it returns.
     *
     * The ack carries the command index and the argument-key dictionary in
     * full, which is not an optimisation: a response frame names its fields by
     * index, so without the dictionary this client could not decode even the
     * answer to `ALIAS keys`. The ack is the one message that can break that
     * circle.
     */
    #binaryHandshakeExchange() {
        const requested = this.options.binary === true ? {} : this.options.binary || {};
        const session = new binaryCodec.BinarySession();
        const frame = binaryCodec.encodeHandshake({
            uint: requested.uint || 0,
            int: requested.int || 0,
            float: requested.float || 0,
        });
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.binaryHandshake = null;
                reject(new CheetahConnectionError('cheetah binary handshake timed out'));
            }, this.options.connectTimeoutMs);
            this.binaryHandshake = {
                resolve: (ack) => {
                    clearTimeout(timer);
                    if (ack.version !== binaryCodec.PROTOCOL_VERSION) {
                        reject(new CheetahConnectionError(
                            `cheetah binary protocol version ${ack.version} is not supported by this binder`
                        ));
                        return;
                    }
                    session.version = ack.version;
                    session.widths = ack.widths;
                    session.epoch = ack.epoch;
                    session.loadCommands(ack.commands, ack.digest);
                    session.loadKeys(ack.keys, ack.keysDigest);
                    this.binary = session;
                    this.emit('binary', { digest: ack.digest, epoch: ack.epoch, widths: ack.widths });
                    resolve(session);
                },
                reject: (error) => {
                    clearTimeout(timer);
                    reject(error);
                },
            };
            this.socket.write(frame);
        });
    }

    #onData(chunk) {
        if (this.options.binary) {
            this.#onBinaryData(chunk);
            return;
        }
        this.buffer += chunk;
        let newline = this.buffer.indexOf('\n');
        while (newline !== -1) {
            const line = this.buffer.slice(0, newline);
            this.buffer = this.buffer.slice(newline + 1);
            this.#resolveNext(line);
            newline = this.buffer.indexOf('\n');
        }
    }

    #onBinaryData(chunk) {
        this.binaryBuffer = this.binaryBuffer.length === 0 ? chunk : Buffer.concat([this.binaryBuffer, chunk]);
        for (;;) {
            let taken;
            try {
                taken = binaryCodec.readFrame(this.binaryBuffer);
            } catch (error) {
                // A frame we cannot even delimit means we no longer know where
                // the next one starts: the connection goes down with it.
                this.#failBinary(new CheetahConnectionError(error.message));
                return;
            }
            if (!taken) return;
            this.binaryBuffer = Buffer.from(taken.rest);
            if (!this.#onBinaryFrame(taken.frame)) return;
        }
    }

    /** Returns false when the connection has been torn down. */
    #onBinaryFrame(frame) {
        if (frame.type === binaryCodec.FRAME.HANDSHAKE_ACK) {
            const pending = this.binaryHandshake;
            this.binaryHandshake = null;
            if (!pending) {
                this.#failBinary(new CheetahConnectionError('cheetah sent an unexpected handshake ack'));
                return false;
            }
            try {
                pending.resolve(binaryCodec.decodeHandshakeAck(frame.body));
            } catch (error) {
                pending.reject(new CheetahConnectionError(error.message));
                return false;
            }
            return true;
        }
        if (frame.type !== binaryCodec.FRAME.RESPONSE) {
            this.#failBinary(new CheetahConnectionError(`cheetah sent an unexpected frame type ${frame.type}`));
            return false;
        }
        let decoded;
        try {
            decoded = binaryCodec.decodeResponse(frame.body, this.binary, this.binary ? this.binary.widths : undefined);
        } catch (error) {
            this.#failBinary(new CheetahConnectionError(error.message));
            return false;
        }
        // Back to the canonical line: everything above this point — parsing,
        // the command layers, CheetahDatabase — is untouched by the transport.
        this.#resolveNext(decoded.line);
        return true;
    }

    #failBinary(error) {
        const pending = this.binaryHandshake;
        this.binaryHandshake = null;
        if (pending) pending.reject(error);
        this.#onFailure(error);
    }

    #resolveNext(line) {
        const entry = this.inflight.shift();
        if (!entry) {
            // A response with nothing waiting means the FIFO invariant broke.
            // Surfacing it is the only safe move: every later response would be
            // matched to the wrong command.
            this.emit('desync', line);
            this.#onFailure(new CheetahConnectionError(`cheetah sent an unmatched response: ${line}`));
            return;
        }
        clearTimeout(entry.timer);
        entry.resolve(parseResponse(line));
        this.#drainWaiting();
    }

    #onFailure(error) {
        if (!this.connected && this.inflight.length === 0 && this.waiting.length === 0) return;
        this.connected = false;
        const socket = this.socket;
        this.socket = null;
        if (socket) socket.destroy();
        this.lastError = error;
        // The negotiated session belongs to the socket that is going away: a
        // reconnection re-handshakes, and the index it gets back may differ.
        this.binary = null;
        this.binaryBuffer = Buffer.alloc(0);

        const pending = [...this.inflight, ...this.waiting];
        this.inflight = [];
        this.waiting = [];
        for (const entry of pending) {
            clearTimeout(entry.timer);
            entry.reject(new CheetahConnectionError(error.message, { command: entry.line }));
        }
        if (!this.closing) this.emit('disconnect', error);
    }

    #drainWaiting() {
        while (this.waiting.length > 0 && this.inflight.length < this.options.maxInFlight) {
            const entry = this.waiting.shift();
            this.#dispatch(entry);
        }
    }

    #dispatch(entry) {
        if (!this.socket || !this.connected) {
            entry.reject(new CheetahConnectionError('cheetah connection is not open', { command: entry.line }));
            return;
        }
        this.inflight.push(entry);
        entry.timer = setTimeout(() => {
            // A timed-out command still owns its slot in the FIFO: we cannot
            // drop it without misaligning every later response, so the
            // connection goes down with it.
            this.#onFailure(new CheetahConnectionError(`cheetah command timed out: ${entry.line}`));
        }, this.options.commandTimeoutMs);
        if (this.options.binary) {
            // The line is the contract on both sides: encode it, and the server
            // decodes the very same line back before routing it.
            let frame;
            try {
                frame = binaryCodec.encodeCommandLine(entry.line, this.binary);
            } catch (error) {
                clearTimeout(entry.timer);
                this.inflight.pop();
                entry.reject(new CheetahError(error.message, { command: entry.line }));
                return;
            }
            this.socket.write(frame);
            return;
        }
        this.socket.write(`${entry.line}\n`, 'latin1');
    }

    #writeNow(line) {
        return new Promise((resolve, reject) => {
            this.#dispatch({ line, resolve, reject, timer: null });
        });
    }

    /**
     * Send one raw command line and resolve with its parsed response.
     *
     * The line may be coalesced with others into a `BATCH` — see `autoBatch`.
     * That is invisible to the caller (one line in, one parsed response out)
     * and it preserves order: anything that cannot join the pending queue
     * flushes it first, and the server applies a batch's items in order.
     */
    async send(line) {
        if (this.closing) throw new CheetahError('cheetah client is closed', { command: line });
        if (String(line).includes('\n')) {
            throw new CheetahError('cheetah command must not contain a newline', { command: line });
        }
        return this.batcher.submit(String(line));
    }

    /** `send` with auto-batching bypassed. The batcher itself writes through here. */
    async sendDirect(line) {
        if (this.closing) throw new CheetahError('cheetah client is closed', { command: line });
        if (!this.connected) await this.connect();
        return new Promise((resolve, reject) => {
            const entry = { line, resolve, reject, timer: null };
            if (this.inflight.length < this.options.maxInFlight) this.#dispatch(entry);
            else this.waiting.push(entry);
        });
    }

    /** Write any queued auto-batch right now. */
    flush() {
        this.batcher.flush();
        return this;
    }

    /** `send` with positional-argument encoding (PAIR_* / KV dialect). */
    async command(name, ...args) {
        return this.send(buildCommand(name, ...args));
    }

    /** `command`, but an `ERROR,…` response throws instead of resolving. */
    async commandOrThrow(name, ...args) {
        const line = buildCommand(name, ...args);
        const response = await this.send(line);
        if (!response.ok) {
            throw new CheetahError(`cheetah ${name} failed: ${response.error || response.raw}`, {
                command: line,
                response,
            });
        }
        return response;
    }

    async close() {
        // Ahead of `closing`: a queued item that is never written would leave
        // its caller's promise pending forever. Flushed, it becomes an in-flight
        // command and is rejected below with everything else.
        this.batcher.flush();
        this.closing = true;
        const socket = this.socket;
        this.connected = false;
        this.socket = null;
        const pending = [...this.inflight, ...this.waiting];
        this.inflight = [];
        this.waiting = [];
        for (const entry of pending) {
            clearTimeout(entry.timer);
            entry.reject(new CheetahError('cheetah client closed while the command was in flight', {
                command: entry.line,
            }));
        }
        if (socket) {
            await new Promise((resolve) => {
                socket.end(() => resolve());
                socket.once('close', resolve);
                setTimeout(resolve, 250).unref?.();
            });
            socket.destroy();
        }
        this.emit('close');
    }
}

/**
 * A fixed-size pool. `send` spreads single commands over the least-loaded
 * connection; `withConnection` leases one so a multi-command sequence cannot be
 * interleaved with anyone else's.
 */
class CheetahPool {
    constructor(options = {}) {
        const { size = 4, ...clientOptions } = defined(options);
        this.size = Math.max(1, Number(size) || 1);
        this.clientOptions = clientOptions;
        this.clients = Array.from({ length: this.size }, () => new CheetahClient(clientOptions));
        this.leased = new Set();
        this.leaseQueue = [];
        this.closing = false;
    }

    async connect() {
        await Promise.all(this.clients.map((client) => client.connect()));
        return this;
    }

    /** The unleased connection with the shortest queue. */
    #leastLoaded() {
        let best = null;
        for (const client of this.clients) {
            if (this.leased.has(client)) continue;
            if (!best || client.pendingCount < best.pendingCount) best = client;
        }
        return best;
    }

    async send(line) {
        if (this.closing) throw new CheetahError('cheetah pool is closed', { command: line });
        const client = this.#leastLoaded();
        if (client) return client.send(line);
        // Every connection is leased; wait for one to come back.
        return this.withConnection((leasedClient) => leasedClient.send(line));
    }

    async command(name, ...args) {
        return this.send(buildCommand(name, ...args));
    }

    /** Pool equivalent of `CheetahClient.commandOrThrow`. */
    async commandOrThrow(name, ...args) {
        const line = buildCommand(name, ...args);
        const response = await this.send(line);
        if (!response.ok) {
            throw new CheetahError(`cheetah ${name} failed: ${response.error || response.raw}`, {
                command: line,
                response,
            });
        }
        return response;
    }

    /** Write every connection's queued auto-batch right now. */
    flush() {
        for (const client of this.clients) client.flush();
        return this;
    }

    async acquire() {
        if (this.closing) throw new CheetahError('cheetah pool is closed');
        const free = this.#leastLoaded();
        if (free) {
            this.leased.add(free);
            await free.connect();
            return free;
        }
        return new Promise((resolve, reject) => {
            this.leaseQueue.push({ resolve, reject });
        });
    }

    release(client) {
        if (!this.leased.has(client)) return;
        const next = this.leaseQueue.shift();
        if (next) {
            next.resolve(client);
            return;
        }
        this.leased.delete(client);
    }

    async withConnection(fn) {
        const client = await this.acquire();
        try {
            return await fn(client);
        } finally {
            this.release(client);
        }
    }

    /**
     * Send one line on **every** connection, sequentially.
     *
     * For commands that change a connection's own server-side state rather than
     * the data. The case this exists for is `RESET_DB`: database selection is
     * per-connection, `Engine.ResetDatabase` closes that database and drops it
     * from the engine's registry, and `server.go` re-selects it only for the
     * socket that issued the command. The other connections keep a pointer to a
     * closed `Database` whose per-database `FileManager` has been shut down.
     *
     * Broadcasting `DATABASE <name>` after a reset re-points them at the live
     * database. This is reasoning from the server's model, not from a reproduced
     * failure — stale connections are hard to catch in a test because
     * `ManagedFile.acquireHandle` opens with `O_CREATE`, so a read on one lands
     * on the same path and answers plausibly rather than erroring.
     *
     * The caller must have the pool quiesced: this makes no attempt to order
     * itself against in-flight work, because there is no correct order for
     * "the database you were using no longer exists".
     */
    async broadcast(line) {
        if (this.closing) throw new CheetahError('cheetah pool is closed', { command: line });
        const responses = [];
        for (const client of this.clients) {
            await client.connect();
            responses.push(await client.send(line));
        }
        return responses;
    }

    async close() {
        this.closing = true;
        for (const waiter of this.leaseQueue.splice(0)) {
            waiter.reject(new CheetahError('cheetah pool closed while waiting for a connection'));
        }
        await Promise.all(this.clients.map((client) => client.close()));
    }
}

module.exports = {
    CheetahClient,
    CheetahPool,
    CheetahError,
    CheetahConnectionError,
    CLIENT_DEFAULTS: DEFAULTS,
};

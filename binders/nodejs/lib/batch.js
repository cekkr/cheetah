// The `BATCH` command: run one command N times in one round trip.
//
// Cheetah's bottleneck under bulk work is the round trip, not the disk. `BATCH`
// is the server's general answer to it — one command name, any target command:
//
//     BATCH <COMMAND> items=<base64 JSON> [continue_on_error=] [results=] [async=]
//
// An item is one of three shapes, and this module builds all three:
//
//     "ctx:BERLIN 42"        a raw argument line
//     ["ctx:BERLIN", 42]     positional arguments
//     {from: 'a', to: 'b'}   the key=value dialect, over the shared modifiers
//
// It is **not** a transaction: items apply in order and independently, so the
// response always carries `applied`/`failed` rather than collapsing to one
// ERROR. With `results=1` (the default) every item's own response line comes
// back in `payload=`.
//
// Two ways to run one:
//
//   - **inline** — `runBatch`, one line out, one line back.
//   - **detached** — `runBatchAsync` submits it as a job and streams the item
//     responses back with `JOB results` *while it runs*, then closes with the
//     aggregate. Use it when the batch is big enough that holding the socket
//     for its whole duration is the wrong trade.
//
// The third way is not to think about it at all: `createAutoBatcher` watches a
// connection and coalesces bursts of the same command into `BATCH` on its own.
// `CheetahClient` wires it in by default — see `autoBatch` in client.js.

const { parseResponse } = require('./protocol');

// client.js requires this module to wire auto-batching into CheetahClient, so
// requiring it back at load time would hand us a half-built module object.
// The error class is only ever needed at call time.
function cheetahError(message, options) {
    const { CheetahError } = require('./client');
    return new CheetahError(message, options);
}

/** Server-side cap on the items of one request (src/batch.go). */
const BATCH_MAX_ITEMS = 10000;

/** Server-side cap on one `JOB results` page (src/micro_job.go). */
const JOB_RESULTS_MAX_PAGE = 1000;

/**
 * Commands `BATCH` refuses as a target, mirrored here so the client fails
 * before the round trip. `BATCH`/`JOB` would recurse; the other three are
 * connection-scoped and never reach the dispatcher at all.
 */
const NON_BATCHABLE = Object.freeze(['BATCH', 'JOB', 'DATABASE', 'RESET_DB', 'EXIT', 'QUIT']);

/**
 * Commands auto-batching leaves alone on top of `NON_BATCHABLE`.
 *
 * These are not *wrong* to batch — they are simply never the shape auto-batching
 * exists for. A burst of them is a client doing something unusual, and folding
 * an administrative sweep into someone else's batch buys nothing while making
 * the failure harder to read.
 */
const AUTO_BATCH_EXCLUDED = Object.freeze([
    ...NON_BATCHABLE,
    'LOG_FLUSH',
    'SYSTEM_STATS',
    'FILE_CHECKPOINT',
    'CLUSTER_UPDATE',
    'CLUSTER_GOSSIP',
    'CLUSTER_MOVE',
    'FORK_ASSIGN',
]);

const AUTO_BATCH_DEFAULTS = Object.freeze({
    /** Master switch. `false` restores the pre-batching wire behavior exactly. */
    enabled: true,
    /** Calls of one command inside `windowMs` before that command goes hot. */
    threshold: 8,
    windowMs: 200,
    /** A hot command cools down after this long without a call. */
    idleMs: 2000,
    /** Flush when the queue reaches this many items. */
    maxSize: 256,
    /** …or this many bytes of command line, whichever comes first. */
    maxBytes: 512 * 1024,
    /**
     * A queue of fewer than this many items is sent as plain commands instead.
     * This is what keeps a *sequential* caller (await, await, await) paying
     * nothing: it can never have two commands outstanding, so it never batches.
     */
    minSize: 2,
    /**
     * Milliseconds to hold the queue open. `0` means "until the end of the
     * current tick", which is what makes `Promise.all([...])` land in one batch
     * without adding latency to anything.
     */
    flushMs: 0,
    /** Only these commands (uppercase) may batch. `null` = all but the excluded. */
    commands: null,
    exclude: AUTO_BATCH_EXCLUDED,
    /** `(info) => void`, called after every flush that actually batched. */
    onBatch: null,
});

// --- building --------------------------------------------------------------

function encodeItems(items) {
    const list = Array.from(items || []);
    if (list.length === 0) throw cheetahError('cheetah BATCH requires at least one item');
    if (list.length > BATCH_MAX_ITEMS) {
        throw cheetahError(
            `cheetah BATCH accepts at most ${BATCH_MAX_ITEMS} items (got ${list.length})`
        );
    }
    return Buffer.from(JSON.stringify(list), 'utf8').toString('base64');
}

function assertBatchable(command) {
    const name = String(command || '').trim().toUpperCase();
    if (!name) throw cheetahError('cheetah BATCH requires a target command');
    const base = name.split(':')[0];
    if (NON_BATCHABLE.includes(base)) {
        throw cheetahError(`cheetah BATCH cannot target ${base}`);
    }
    return name;
}

/**
 * Build one `BATCH` line.
 *
 * `shared` are modifiers every *object* item inherits — `{type: 'knows'}` writes
 * the edge type once instead of ten thousand times. Raw-string and array items
 * carry their own arguments and ignore it, which is the only rule that lets one
 * command serve both dialects.
 */
function buildBatch(command, items, options = {}) {
    const { continueOnError = true, results = true, async: asJob = false, shared = null } = options;
    const parts = [`BATCH ${assertBatchable(command)}`, `items=${encodeItems(items)}`];
    // The server defaults to stop-on-error, while this binder deliberately
    // defaults to continuing. Always spell the flag so either caller choice
    // survives the wire unchanged.
    parts.push(`continue_on_error=${continueOnError ? '1' : '0'}`);
    if (!results) parts.push('results=0');
    if (asJob) parts.push('async=1');
    for (const [key, value] of Object.entries(shared || {})) {
        if (value === undefined || value === null || value === '') continue;
        const rendered = typeof value === 'boolean' ? (value ? '1' : '0') : String(value);
        if (/\s/.test(rendered)) {
            throw cheetahError(`cheetah BATCH shared modifier ${key}= must not contain whitespace`);
        }
        parts.push(`${key}=${rendered}`);
    }
    return parts.join(' ');
}

// --- reading ---------------------------------------------------------------

/**
 * Decode the `payload=` of a `BATCH` (or `JOB results`) response into response
 * lines.
 *
 * The lines are JSON strings, so they must be valid UTF-8. When a command
 * answers with bytes that are not — `READ` of a binary payload is the only real
 * case — the server switches the whole array to base64 and says so in
 * `results_encoding=`; decoding it as latin1 then gives back the exact bytes
 * that were on the wire, which is the encoding the socket itself uses.
 */
function decodeResultLines(fields) {
    const payload = fields && fields.payload;
    if (!payload) return [];
    const decoded = JSON.parse(Buffer.from(payload, 'base64').toString('utf8'));
    if (!Array.isArray(decoded)) return [];
    const base64Lines = (fields.results_encoding || '') === 'base64';
    return decoded.map((line) => {
        if (line === null || line === undefined) return null;
        return base64Lines ? Buffer.from(String(line), 'base64').toString('latin1') : String(line);
    });
}

/**
 * `{requested, applied, failed, firstError, lines, results}` from a `BATCH`
 * response. `results` holds one parsed response per item, `null` where the item
 * never ran (an early abort with `continueOnError: false`).
 */
function parseBatchResponse(response, { target = null } = {}) {
    const lines = decodeResultLines(response.fields);
    return {
        target: response.fields.target || target,
        requested: Number(response.fields.requested || 0),
        applied: Number(response.fields.applied || 0),
        failed: Number(response.fields.failed || 0),
        firstError: response.fields.first_error || null,
        lines,
        results: lines.map((line) => (line === null ? null : parseResponse(line))),
        response,
    };
}

// --- inline ----------------------------------------------------------------

/** Send one `BATCH` and return its parsed aggregate. */
async function runBatch(conn, command, items, options = {}) {
    const line = buildBatch(command, items, options);
    const response = await conn.send(line);
    if (!response.ok) {
        throw cheetahError(`cheetah BATCH ${command} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return parseBatchResponse(response, { target: command });
}

/**
 * `runBatch` split into `chunkSize` requests, with the counts merged.
 *
 * The server caps one request at `BATCH_MAX_ITEMS`; above that the split is
 * mandatory. Below it, it is a choice about failure granularity — the command
 * is not a transaction, so two chunks can leave a half-applied list either way.
 */
async function runBatchChunked(conn, command, items, options = {}) {
    const { chunkSize = 1000, ...rest } = options;
    const list = Array.from(items || []);
    const size = Math.max(1, Math.min(Number(chunkSize) || 1, BATCH_MAX_ITEMS));
    const totals = { target: command, requested: 0, applied: 0, failed: 0, firstError: null, lines: [], results: [] };
    for (let at = 0; at < list.length; at += size) {
        const page = await runBatch(conn, command, list.slice(at, at + size), rest);
        totals.requested += page.requested;
        totals.applied += page.applied;
        totals.failed += page.failed;
        totals.firstError = totals.firstError || page.firstError;
        totals.lines.push(...page.lines);
        totals.results.push(...page.results);
    }
    return totals;
}

// --- detached --------------------------------------------------------------

/**
 * Submit a `BATCH` as a job and follow it to the end.
 *
 * `onResult(parsed, index)` — when given — receives every item's response as
 * soon as the server has it, not at the end: that is the whole point of the
 * detached form, and it is why the poll loop reads `JOB results` before it
 * decides whether the job is done.
 */
async function runBatchAsync(conn, command, items, options = {}) {
    const {
        onResult = null,
        onProgress = null,
        pollIntervalMs = 250,
        timeoutMs = null,
        pageSize = JOB_RESULTS_MAX_PAGE,
        ...batchOptions
    } = options;
    const jobs = require('./jobs');

    const line = buildBatch(command, items, { ...batchOptions, async: true });
    const submitted = await conn.send(line);
    if (!submitted.ok) {
        throw cheetahError(`cheetah BATCH ${command} submit failed: ${submitted.error || submitted.raw}`, {
            command: line,
            submitted,
        });
    }
    const jobId = submitted.fields.job;
    if (!jobId) throw cheetahError(`cheetah BATCH returned no job id: ${submitted.raw}`, { response: submitted });

    const deadline = timeoutMs === null ? null : Date.now() + timeoutMs;
    let consumed = 0;
    for (;;) {
        if (onResult) {
            for (;;) {
                const page = await jobs.results(conn, jobId, { from: consumed, limit: pageSize });
                if (page.lines.length === 0) break;
                page.lines.forEach((text, offset) => {
                    onResult(parseResponse(text), consumed + offset);
                });
                consumed = page.next;
            }
        }
        const snapshot = await jobs.status(conn, jobId);
        if (onProgress) onProgress(snapshot);
        if (snapshot.finished) {
            // One last sweep: the items produced between the page read above
            // and the job's own completion would otherwise never be delivered.
            if (onResult) {
                for (;;) {
                    const page = await jobs.results(conn, jobId, { from: consumed, limit: pageSize });
                    if (page.lines.length === 0) break;
                    page.lines.forEach((text, offset) => onResult(parseResponse(text), consumed + offset));
                    consumed = page.next;
                }
            }
            const result = await jobs.fetch(conn, jobId);
            if (result !== null) return parseBatchResponse(result, { target: command });
        }
        if (deadline !== null && Date.now() >= deadline) {
            throw cheetahError(`cheetah BATCH job ${jobId} did not finish within ${timeoutMs}ms`);
        }
        await new Promise((resolve) => setTimeout(resolve, Math.max(10, pollIntervalMs)));
    }
}

/**
 * `runBatch` or `runBatchAsync`, chosen by size.
 *
 * `async: 'auto'` (the default) detaches above `asyncThreshold` items, because
 * past that the batch is long enough that holding the socket open for it costs
 * more than the extra round trips of polling.
 */
async function batch(conn, command, items, options = {}) {
    const { async: mode = 'auto', asyncThreshold = 2000, ...rest } = options;
    const list = Array.from(items || []);
    const detach = mode === true || (mode === 'auto' && list.length >= asyncThreshold);
    if (detach) return runBatchAsync(conn, command, list, rest);
    if (list.length > BATCH_MAX_ITEMS) return runBatchChunked(conn, command, list, rest);
    return runBatch(conn, command, list, rest);
}

// --- automatic -------------------------------------------------------------

/** Split a raw command line into `[COMMAND, argumentLine]`. */
function splitCommandLine(line) {
    const text = String(line);
    const space = text.indexOf(' ');
    if (space === -1) return [text.toUpperCase(), ''];
    return [text.slice(0, space).toUpperCase(), text.slice(space + 1)];
}

/**
 * Watches a stream of command lines and folds bursts of the same command into
 * `BATCH`.
 *
 * The policy has two halves, and they answer two different questions:
 *
 *   - *Is this command hot?* — `threshold` calls inside `windowMs`. Below that
 *     nothing changes at all, which is what "automatic" has to mean for a
 *     client that was not written with batching in mind.
 *   - *Is there anything to gain right now?* — the queue is held only to the end
 *     of the tick and is sent as plain commands when fewer than `minSize` items
 *     accumulated. A caller that awaits every command therefore never batches,
 *     and never pays for the machinery; a caller that fires ten in parallel
 *     batches all ten.
 *
 * Order is preserved end to end: a pending queue is flushed before anything
 * that cannot join it, and the server applies a batch's items in order.
 */
class CommandBatcher {
    constructor(send, options = {}) {
        this.send = send;
        this.options = { ...AUTO_BATCH_DEFAULTS, ...(options || {}) };
        this.options.exclude = (this.options.exclude || []).map((name) => String(name).toUpperCase());
        this.options.commands = this.options.commands
            ? this.options.commands.map((name) => String(name).toUpperCase())
            : null;
        this.windows = new Map();
        this.queue = [];
        this.queueCommand = null;
        this.queueBytes = 0;
        this.timer = null;
        this.stats = { batched: 0, batches: 0, direct: 0 };
    }

    get enabled() {
        return Boolean(this.options.enabled);
    }

    /** Accept one command line; resolves with its own parsed response. */
    submit(line) {
        if (!this.enabled) return this.send(line);
        const [command, args] = splitCommandLine(line);
        if (!this.#batchable(command)) {
            this.#flush();
            return this.send(line);
        }
        if (!this.#markHot(command)) {
            this.#flush();
            return this.send(line);
        }
        if (this.queueCommand !== null && this.queueCommand !== command) this.#flush();
        return new Promise((resolve, reject) => {
            this.queueCommand = command;
            this.queue.push({ args, resolve, reject });
            this.queueBytes += args.length + 8;
            if (this.queue.length >= this.options.maxSize || this.queueBytes >= this.options.maxBytes) {
                this.#flush();
                return;
            }
            this.#schedule();
        });
    }

    /** Send whatever is queued right now. Safe to call when nothing is. */
    flush() {
        this.#flush();
    }

    #batchable(command) {
        const base = command.split(':')[0];
        if (this.options.exclude.includes(base)) return false;
        if (this.options.commands && !this.options.commands.includes(base)) return false;
        return true;
    }

    /**
     * Per-command sliding window. Returns whether the command is hot *now*.
     *
     * The window is a counter and a start stamp rather than a list of
     * timestamps: this runs on every single command, and an exact sliding
     * window would allocate per call to answer a question whose only use is a
     * yes/no.
     */
    #markHot(command) {
        const now = Date.now();
        let window = this.windows.get(command);
        if (!window || now - window.last > this.options.idleMs) {
            window = { start: now, count: 0, last: now, hot: false };
            this.windows.set(command, window);
        }
        if (now - window.start > this.options.windowMs) {
            window.hot = window.count >= this.options.threshold;
            window.start = now;
            window.count = 0;
        }
        window.count += 1;
        window.last = now;
        if (window.count >= this.options.threshold) window.hot = true;
        return window.hot;
    }

    #schedule() {
        if (this.timer !== null) return;
        const { flushMs } = this.options;
        if (flushMs > 0) {
            this.timer = setTimeout(() => this.#flush(), flushMs);
        } else {
            // End of tick, not end of a timer: everything a caller started in
            // this turn of the loop is already queued by then.
            this.timer = setImmediate(() => this.#flush());
        }
        // Deliberately *not* unref'd. A queued flush is a command someone is
        // awaiting, so it has to keep the loop alive; unref'ing it lets a busy
        // poll phase starve the callback and the caller waits on nothing.
    }

    #clearTimer() {
        if (this.timer === null) return;
        if (this.options.flushMs > 0) clearTimeout(this.timer);
        else clearImmediate(this.timer);
        this.timer = null;
    }

    #flush() {
        this.#clearTimer();
        const pending = this.queue;
        const command = this.queueCommand;
        this.queue = [];
        this.queueCommand = null;
        this.queueBytes = 0;
        if (pending.length === 0) return;

        if (pending.length < this.options.minSize) {
            // Nothing to gain: one item in a BATCH is one command plus a
            // wrapper. Send it as itself.
            for (const entry of pending) {
                this.stats.direct += 1;
                const text = entry.args ? `${command} ${entry.args}` : command;
                this.send(text).then(entry.resolve, entry.reject);
            }
            return;
        }

        this.stats.batches += 1;
        this.stats.batched += pending.length;
        let line;
        try {
            line = buildBatch(command, pending.map((entry) => entry.args), { continueOnError: true });
        } catch (error) {
            for (const entry of pending) entry.reject(error);
            return;
        }
        this.send(line).then(
            (response) => {
                if (!response.ok) {
                    const error = cheetahError(
                        `cheetah BATCH ${command} failed: ${response.error || response.raw}`,
                        { command: line, response }
                    );
                    for (const entry of pending) entry.reject(error);
                    return;
                }
                const parsed = parseBatchResponse(response, { target: command });
                pending.forEach((entry, index) => {
                    const item = parsed.results[index];
                    if (item) entry.resolve(item);
                    else {
                        entry.reject(cheetahError(
                            `cheetah BATCH ${command} returned no result for item ${index}`,
                            { command: line, response }
                        ));
                    }
                });
                if (this.options.onBatch) {
                    this.options.onBatch({ command, size: pending.length, response: parsed });
                }
            },
            (error) => {
                for (const entry of pending) entry.reject(error);
            }
        );
    }
}

function createAutoBatcher(send, options = {}) {
    return new CommandBatcher(send, options);
}

module.exports = {
    AUTO_BATCH_DEFAULTS,
    AUTO_BATCH_EXCLUDED,
    BATCH_MAX_ITEMS,
    CommandBatcher,
    JOB_RESULTS_MAX_PAGE,
    NON_BATCHABLE,
    batch,
    buildBatch,
    createAutoBatcher,
    decodeResultLines,
    parseBatchResponse,
    runBatch,
    runBatchAsync,
    runBatchChunked,
    splitCommandLine,
};

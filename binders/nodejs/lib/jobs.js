// The `JOB` micro-command: submit a bounded command, poll it, fetch it once.
//
// A sweep that takes minutes cannot hold a connection hostage, so Cheetah
// detaches it: `JOB submit` answers a `job=<id>` immediately, `JOB status`
// reports progress, and `JOB fetch` returns the submitted command's own
// response — once, because fetching consumes the job.
//
// Three things a caller has to know:
//
//   - **Only registered commands are submittable** (today `PAIR_REDUCE`,
//     `PREDICT_INHERIT_BATCH` and `BATCH`); anything else answers
//     `ERROR,command_not_submittable`.
//   - **The command line travels base64** in `command=`. The micro dialect
//     splits on whitespace and a command line is full of it.
//   - **Jobs are in memory.** A server restart loses them, so a poll loop must
//     treat `job_not_found` as terminal rather than retrying forever.
//
// The historical names (`PAIR_REDUCE_ASYNC`/`_STATUS`/`_FETCH`,
// `PREDICT_INHERIT_ASYNC`/…) are aliases over exactly this and still work; use
// them only against a server too old to know `JOB`.

const { CheetahError } = require('./client');
const { buildKeyValueCommand, numericField } = require('./protocol');

function buildSubmit(commandLine) {
    const text = String(commandLine || '').trim();
    if (!text) throw new CheetahError('cheetah JOB submit requires a command line');
    return `JOB submit command=${Buffer.from(text, 'utf8').toString('base64')}`;
}

function buildStatus(jobId) {
    return buildKeyValueCommand('JOB status', { id: jobId });
}

function buildFetch(jobId) {
    return buildKeyValueCommand('JOB fetch', { id: jobId });
}

/** Submit a bounded command; resolves to the job id. */
async function submit(conn, commandLine) {
    const line = buildSubmit(commandLine);
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah JOB submit failed: ${response.error || response.raw}`, {
            command: commandLine,
            response,
        });
    }
    const jobId = response.fields.job;
    if (!jobId) {
        throw new CheetahError(`cheetah JOB submit returned no job id: ${response.raw}`, { response });
    }
    return jobId;
}

function snapshotOf(response, jobId) {
    const state = (response.fields.state || '').trim();
    return {
        jobId: response.fields.job || jobId,
        state,
        progress: numericField(response.fields, 'progress', 0),
        completed: numericField(response.fields, 'completed', 0),
        total: numericField(response.fields, 'total', 0),
        error: response.fields.error || null,
        // A *failed* job still answers SUCCESS to `JOB status`; the state is
        // where the failure lives, which is why these two are separate.
        finished: ['completed', 'failed', 'cancelled'].includes(state),
        failed: state === 'failed' || Boolean(response.fields.error),
        response,
    };
}

async function status(conn, jobId) {
    const line = buildStatus(jobId);
    const response = await conn.send(line);
    if (!(response.ok || response.pending)) {
        throw new CheetahError(`cheetah JOB status ${jobId} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return snapshotOf(response, jobId);
}

/**
 * The submitted command's own response, or null while it still runs. Consumes
 * the job when it returns one: there is no second fetch.
 */
async function fetch(conn, jobId) {
    const line = buildFetch(jobId);
    const response = await conn.send(line);
    if (response.pending) return null;
    if (!response.ok) {
        throw new CheetahError(`cheetah JOB fetch ${jobId} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    return response;
}

/**
 * Read the results a job has produced **so far**, without consuming it.
 *
 * The counterpart to `fetch`: fetch is terminal and answers with the command's
 * aggregate, `results` is repeatable and answers with the individual response
 * lines, so a caller can consume a long `BATCH` page by page while it is still
 * running. `from` is the index to resume at — feed it the previous call's
 * `next` — and a job whose command produces no per-item results simply always
 * answers `count=0`.
 */
async function results(conn, jobId, { from = 0, limit = 0 } = {}) {
    const line = buildKeyValueCommand('JOB results', { id: jobId, from, limit: limit || undefined });
    const response = await conn.send(line);
    if (!response.ok) {
        throw new CheetahError(`cheetah JOB results ${jobId} failed: ${response.error || response.raw}`, {
            command: line,
            response,
        });
    }
    // Required lazily: batch.js reaches for this module to stream a detached
    // BATCH, and a top-level require in both directions would leave one of them
    // holding a half-built module object.
    const { decodeResultLines } = require('./batch');
    return {
        jobId: response.fields.job || jobId,
        state: (response.fields.state || '').trim(),
        from: numericField(response.fields, 'from', from),
        count: numericField(response.fields, 'count', 0),
        next: numericField(response.fields, 'next', from),
        available: numericField(response.fields, 'available', 0),
        total: numericField(response.fields, 'total', 0),
        lines: decodeResultLines(response.fields),
        response,
    };
}

/**
 * Poll until the job finishes and resolve to its result.
 *
 * The interval is deliberately coarse: polling is a round trip that competes
 * with the job it is watching, and a sweep long enough to detach is never
 * milliseconds away from done.
 */
async function awaitJob(conn, jobId, { pollIntervalMs = 5000, timeoutMs = null, onProgress = null } = {}) {
    const deadline = timeoutMs === null ? null : Date.now() + timeoutMs;
    const interval = Math.max(100, Number(pollIntervalMs) || 0);
    for (;;) {
        const snapshot = await status(conn, jobId);
        if (onProgress) onProgress(snapshot);
        if (snapshot.failed) {
            throw new CheetahError(
                `cheetah job ${jobId} failed: ${snapshot.error || snapshot.state}`,
                { response: snapshot.response }
            );
        }
        if (snapshot.finished) {
            const result = await fetch(conn, jobId);
            if (result !== null) return result;
        }
        if (deadline !== null && Date.now() >= deadline) {
            throw new CheetahError(`cheetah job ${jobId} did not finish within ${timeoutMs}ms`);
        }
        await new Promise((resolve) => setTimeout(resolve, interval));
    }
}

/**
 * Whether this server knows `JOB` at all. Probed rather than assumed: the
 * micro-commands landed after the aliases, and a client that pins the answer
 * once per connection pays for the probe only on the first detached command.
 */
async function supportsJobApi(conn) {
    const response = await conn.send('JOB');
    return !(response.error || '').startsWith('unknown_command');
}

module.exports = {
    awaitJob,
    buildFetch,
    buildStatus,
    buildSubmit,
    fetch,
    results,
    status,
    submit,
    supportsJobApi,
};

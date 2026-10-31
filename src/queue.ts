// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { createLimiterError } from './errors.js';
import { createLimiterWith, releaseJob } from './limiter.js';
import { readOptions } from './options.js';
import type {
    AttemptInfo,
    Cost,
    JobContext,
    JobOutcome,
    JobResult,
    Limiter,
    LimiterError,
    LimiterOptions,
    PushOptions,
    Queue,
    QueueOptions,
    QueueStatus,
    Result,
    RunSummary,
    StopOptions,
    StopReason,
} from './types.js';
import { isObject } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Pushed job tracked until outcome is resolved. */
type Entry<J, D> = {
    job: J;
    cost: Cost | undefined;
    signal: AbortSignal | undefined;
    priority: number;
    resolve: (outcome: JobOutcome<D>) => void;
    isSettled: boolean;
    onAbort: (() => void) | undefined;
};

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

/** Default limiter buffer: unbounded, so retries sleeping in it never block free concurrency slots. */
const UNLIMITED_BUFFER = Number.POSITIVE_INFINITY;

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

/**
 * **Memory-backed queue wrapper around a limiter.**
 *
 * - `push` enqueues jobs by `priority` (higher first), then push order; resolves with outcome
 * - runs automatically when jobs are available; stops when empty
 * - releases pending jobs on stop or abort
 * - `maxBufferSize` defaults to `Infinity`: jobs wait in memory, so no source to protect from draining
 * - never throws; `push` never rejects
 */
export function createQueue<J, D>(options: QueueOptions<J, D>): Queue<J, D> {
    const waiting: Entry<J, D>[] = [];
    let limiter: Limiter;
    try {
        limiter = createLimiterWith(wrapOptions(options), {
            isSourceDone: () => waiting.length === 0,
            onStop: releaseWaiting,
            unwrapJob: (entry) => entry.job,
        });
    } catch (error) {
        // Only reading options can throw (getter, revoked Proxy).
        return createInvalidQueue(createLimiterError('INVALID_CONFIG', error));
    }

    /** Wraps queue options into limiter options with job entry tracking. */
    function wrapOptions(queueOptions: QueueOptions<J, D>): LimiterOptions<Entry<J, D>, D> {
        // Read options to preserve prototype inheritance.
        const read = readOptions(queueOptions) as Partial<LimiterOptions<Entry<J, D>, D>>;
        if (!isObject(read)) {
            // Retain non-object to trigger INVALID_CONFIG in limiter.
            return read as LimiterOptions<Entry<J, D>, D>;
        }
        const { processJob, onAttempt } = read as Partial<QueueOptions<J, D>>;
        const wrapped: LimiterOptions<Entry<J, D>, D> = {
            ...read,
            maxBufferSize: read.maxBufferSize === undefined ? UNLIMITED_BUFFER : read.maxBufferSize,
            fetchJob: () => {
                const entry = waiting.shift();
                return entry && (entry.cost === undefined ? { job: entry } : { job: entry, cost: entry.cost });
            },
            // Retain missing processJob to trigger INVALID_CONFIG in limiter.
            processJob: (typeof processJob === 'function'
                ? (entry: Entry<J, D>, context: JobContext) =>
                      processJob(
                          entry.job,
                          entry.signal
                              ? { ...context, signal: AbortSignal.any([context.signal, entry.signal]) }
                              : context,
                      )
                : processJob) as LimiterOptions<Entry<J, D>, D>['processJob'],
            onJobDone: (entry, outcome) => settle(entry, outcome),
        };
        if (onAttempt !== undefined) {
            // Retain non-function onAttempt to trigger INVALID_CONFIG in limiter.
            wrapped.onAttempt =
                typeof onAttempt === 'function'
                    ? (entry: Entry<J, D>, result: JobResult<D>, info: AttemptInfo) =>
                          onAttempt(entry.job, result, info)
                    : (onAttempt as LimiterOptions<Entry<J, D>, D>['onAttempt'] & {});
        }
        return wrapped;
    }

    function push(job: J, pushOptions?: PushOptions): Promise<JobOutcome<D>> {
        return new Promise((resolve) => {
            let cost: Cost | undefined;
            let signal: AbortSignal | undefined;
            let priority: number | undefined;
            try {
                ({ cost, signal, priority } = isObject(pushOptions) ? pushOptions : {});
            } catch (error) {
                resolve({ ok: false, status: 'failed', error: createLimiterError('INVALID_CONFIG', error) });
                return;
            }
            if (signal !== undefined && !(signal instanceof AbortSignal)) {
                const error = createLimiterError('INVALID_CONFIG', 'signal must be an AbortSignal');
                resolve({ ok: false, status: 'failed', error });
                return;
            }
            if (priority !== undefined && !Number.isFinite(priority)) {
                const error = createLimiterError('INVALID_CONFIG', 'priority must be a finite number');
                resolve({ ok: false, status: 'failed', error });
                return;
            }
            if (signal?.aborted) {
                resolve(abortedOutcome(signal));
                return;
            }
            const entry: Entry<J, D> = {
                job,
                cost,
                signal,
                priority: priority ?? 0,
                resolve,
                isSettled: false,
                onAbort: undefined,
            };
            if (signal) {
                entry.onAbort = () => abort(entry);
                signal.addEventListener('abort', entry.onAbort);
            }
            insertByPriority(waiting, entry);
            if (limiter.isIdle()) {
                startRun();
            } else {
                limiter.wake();
            }
        });
    }

    /** Starts run; restarts if jobs were queued while stopping. */
    function startRun(): void {
        void limiter.start().then((result) => {
            // Push may have already started next run.
            if (waiting.length === 0 || !limiter.isIdle()) {
                return;
            }
            if (!result.ok) {
                // Run failed; reject remaining waiting jobs.
                for (const entry of waiting.splice(0)) {
                    settle(entry, { ok: false, status: 'released', error: result.error });
                }
                return;
            }
            startRun();
        });
    }

    /** Releases waiting jobs not yet dispatched to limiter on stop. */
    function releaseWaiting(reason: StopReason, error: LimiterError | undefined): void {
        const releaseError =
            error ?? createLimiterError(reason === 'max_wait_exceeded' ? 'MAX_WAIT_EXCEEDED' : 'STOPPED');
        for (const entry of waiting.splice(0)) {
            settle(entry, { ok: false, status: 'released', error: releaseError });
        }
    }

    /** Cancels waiting job or requests active job release on abort signal. */
    function abort(entry: Entry<J, D>): void {
        if (entry.isSettled) {
            return;
        }
        // Settle first so abort reason takes precedence over late limiter outcome.
        settle(entry, abortedOutcome(entry.signal));
        const index = waiting.indexOf(entry);
        if (index !== -1) {
            waiting.splice(index, 1);
            // Re-checks the source so a run waiting on a bucket or lock timer ends at once when nothing is left.
            if (!limiter.isIdle()) {
                limiter.wake();
            }
        } else {
            releaseJob(limiter, entry);
        }
    }

    /** Settles job promise once; ignores subsequent outcomes. */
    function settle(entry: Entry<J, D>, outcome: JobOutcome<D>): void {
        if (entry.isSettled) {
            return;
        }
        entry.isSettled = true;
        if (entry.onAbort) {
            entry.signal?.removeEventListener('abort', entry.onAbort);
        }
        entry.resolve(outcome);
    }

    function getStatus(): QueueStatus {
        return { ...limiter.getStatus(), waiting: waiting.length };
    }

    function stop(stopOptions?: StopOptions): Promise<Result<RunSummary, LimiterError>> {
        // Release waiting jobs even if run is already stopping so none triggers a new run.
        releaseWaiting('stop', undefined);
        return limiter.stop(stopOptions);
    }

    return { push, stop, getStatus };
}

// -------------------------------------------------------------------
// --- 4. Private Functions
// -------------------------------------------------------------------

/** Inserts entry after all entries of equal or higher priority; appends directly when it goes last. */
function insertByPriority<E extends { priority: number }>(entries: E[], entry: E): void {
    if (entries.length === 0 || entries[entries.length - 1].priority >= entry.priority) {
        entries.push(entry);
        return;
    }
    // Binary search for first entry with lower priority.
    let low = 0;
    let high = entries.length - 1;
    while (low < high) {
        const middle = (low + high) >>> 1;
        if (entries[middle].priority < entry.priority) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    entries.splice(low, 0, entry);
}

/** Builds released outcome for aborted signal. */
function abortedOutcome<D>(signal: AbortSignal | undefined): JobOutcome<D> {
    return { ok: false, status: 'released', error: createLimiterError('ABORTED', signal?.reason) };
}

/** Creates stub queue rejecting calls when initial options are invalid. */
function createInvalidQueue<J, D>(error: LimiterError): Queue<J, D> {
    return {
        push: () => Promise.resolve({ ok: false, status: 'released', error }),
        stop: () => Promise.resolve({ ok: false, error }),
        getStatus: () => ({ state: 'idle', running: 0, buffered: 0, lockUntil: 0, lastStoppedBy: 'error', waiting: 0 }),
    };
}

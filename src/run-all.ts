// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { createLimiterError } from './errors.js';
import { createLimiterWith } from './limiter.js';
import { createLogger } from './logger.js';
import { readOptions } from './options.js';
import type {
    AttemptInfo,
    FetchedJob,
    JobContext,
    JobOutcome,
    JobResult,
    LimiterOptions,
    RunAllOptions,
    RunAllResult,
} from './types.js';
import { callSafelyAsync, isObject, validateCost } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Indexed job item preserving original input array position. */
type Entry<J> = { index: number; job: J };

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/**
 * **Processes fixed batch of jobs with rate limiting.**
 *
 * - preserves input order in resolved outcomes array
 * - marks unfinished items as released if stopped or failed
 * - invokes optional `onJobDone` hook once per item, also for released items never started
 * - never rejects
 */
export async function runAll<J, D>(
    items: readonly FetchedJob<J>[],
    options: RunAllOptions<J, D>,
): Promise<RunAllResult<D>> {
    try {
        return await runAllUnguarded(items, options);
    } catch (error) {
        // Only reading items or options can throw (getter, revoked Proxy).
        return { ok: false, error: createLimiterError('INVALID_CONFIG', error) };
    }
}

// -------------------------------------------------------------------
// --- 3. Private Functions
// -------------------------------------------------------------------

/** Internal batch execution helper wrapped with try-catch. */
async function runAllUnguarded<J, D>(
    items: readonly FetchedJob<J>[],
    options: RunAllOptions<J, D>,
): Promise<RunAllResult<D>> {
    if (!Array.isArray(items) || !isObject(options)) {
        return { ok: false, error: createLimiterError('INVALID_CONFIG', 'runAll needs an array of items and options') };
    }
    const { processJob, onAttempt, onJobDone, signal } = options;
    // Read options to preserve prototype inheritance.
    const limiterOptions = readOptions(options) as Partial<LimiterOptions<Entry<J>, D>>;
    if (signal !== undefined && !(signal instanceof AbortSignal)) {
        return { ok: false, error: createLimiterError('INVALID_CONFIG', 'signal must be an AbortSignal') };
    }
    let nextIndex = 0;
    // Preserve original cost; Array.from visits sparse array holes as undefined.
    const open = Array.from(items, (item, index) => ({ ...item, job: { index, job: item?.job } }));
    const results: (JobOutcome<D> | undefined)[] = Array.from(items, () => undefined);
    const wrapped: LimiterOptions<Entry<J>, D> = {
        ...limiterOptions,
        fetchJob: () => open[nextIndex++],
        // Retain missing processJob to trigger INVALID_CONFIG in limiter.
        processJob: (typeof processJob === 'function'
            ? (entry: Entry<J>, context: JobContext) => processJob(entry.job, context)
            : processJob) as LimiterOptions<Entry<J>, D>['processJob'],
        onJobDone: (entry, result) => {
            results[entry.index] = result;
        },
    };
    if (onJobDone !== undefined) {
        // Retain non-function onJobDone to trigger INVALID_CONFIG in limiter.
        wrapped.onJobDone =
            typeof onJobDone === 'function'
                ? (entry: Entry<J>, result: JobOutcome<D>, info: AttemptInfo) => {
                      results[entry.index] = result;
                      return onJobDone(entry.job, result, info);
                  }
                : (onJobDone as LimiterOptions<Entry<J>, D>['onJobDone'] & {});
    }
    if (onAttempt !== undefined) {
        // Retain non-function onAttempt to trigger INVALID_CONFIG in limiter.
        wrapped.onAttempt =
            typeof onAttempt === 'function'
                ? (entry: Entry<J>, result: JobResult<D>, info: AttemptInfo) => onAttempt(entry.job, result, info)
                : (onAttempt as LimiterOptions<Entry<J>, D>['onAttempt'] & {});
    }
    // Run concludes once all items finish without waiting for bucket capacity.
    const limiter = createLimiterWith(wrapped, {
        isSourceDone: () => nextIndex >= open.length,
        unwrapJob: (entry) => entry.job,
    });
    const started = limiter.start();
    /** Aborts active limiter run with abort flag. */
    function abort(): void {
        void limiter.stop({ abort: true });
    }
    if (signal?.aborted) {
        abort();
    }
    signal?.addEventListener('abort', abort);
    const run = await started;
    signal?.removeEventListener('abort', abort);
    if (!run.ok && run.error.code === 'INVALID_CONFIG') {
        return run;
    }
    const releaseCode = run.ok && run.data.stoppedBy === 'max_wait_exceeded' ? 'MAX_WAIT_EXCEEDED' : 'STOPPED';
    const data: JobOutcome<D>[] = results.map(
        (result) => result ?? { ok: false, status: 'released', error: createLimiterError(releaseCode) },
    );
    if (typeof onJobDone === 'function') {
        // Items the limiter never fetched reach onJobDone like the released ones it flushed.
        const logger = createLogger(limiterOptions.log);
        for (const [index, result] of results.entries()) {
            if (result !== undefined) {
                continue;
            }
            const { job, cost } = open[index];
            const outcome = data[index];
            const info: AttemptInfo = {
                attempt: 0,
                cost: { requests: 1, ...(cost === undefined ? {} : validateCost(cost)) },
                durationMs: 0,
            };
            const called = await callSafelyAsync(() => onJobDone(job.job, outcome, info));
            if (!called.ok) {
                logger.error('HOOK_FAILED', 'onJobDone threw.', { job: job.job, outcome, info, error: called.error });
            }
        }
    }
    return run.ok ? { ok: true, data } : { ok: false, error: run.error, data };
}

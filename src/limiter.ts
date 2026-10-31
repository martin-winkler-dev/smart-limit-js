// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { checkBuckets, tryReserveBuckets } from './bucket.js';
import { createLimiterError, ERROR_CODES } from './errors.js';
import { callProcessJob } from './job-result.js';
import { createLogger, type Logger } from './logger.js';
import { type ResolvedLimiterOptions, resolveOptions } from './options.js';
import { createRunState, type LimiterLockState, type QueuedJob, type RunState } from './run-state.js';
import type {
    AttemptInfo,
    Cost,
    JobError,
    JobOutcome,
    JobResult,
    Limiter,
    LimiterError,
    LimiterOptions,
    LimiterState,
    LimiterStatus,
    Result,
    RunSummary,
    StartOptions,
    StopOptions,
    StopReason,
} from './types.js';
import { callSafely, callSafelyAsync, isObject, MAX_TIMER_MS, validateCost } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Outcome decision from attempt: terminal outcome or wait duration exceeding maximum. */
type Decision<D> = { outcome: JobOutcome<D> | undefined; exceededWaitMs?: number };

/** Internal limiter options used by `runAll` and `createQueue`. */
export type InternalLimiterOptions<J = unknown> = {
    isSourceDone?: () => boolean;
    onStop?: (reason: StopReason, error: LimiterError | undefined) => void;
    /** Maps internal wrapper job to caller's job in log entries. */
    unwrapJob?: (job: J) => unknown;
};

/** Fetched job entry with validated cost. */
type FetchedEntry<J> = { job: J; cost: unknown; validCost: Record<string, number> | undefined };

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

/** Shortest computed `backoff_global` lock; provider `retryAfterMs` used as given. */
const MIN_GLOBAL_BACKOFF_MS = 1_000;

/** Lock states keyed by limiter instance for white-box testing. */
const lockStates = new WeakMap<Limiter, LimiterLockState>();

/** Job releasers keyed by limiter instance. */
const jobReleasers = new WeakMap<Limiter, (job: unknown) => void>();

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

/**
 * Returns copy of limiter lock state for white-box testing.
 * @internal - exported for testing only
 */
export function readLockState(limiter: Limiter): LimiterLockState | undefined {
    const lock = lockStates.get(limiter);
    return lock && { ...lock };
}

/** Releases fetched job with `ABORTED` status; cancels buffered entry or next retry. */
export function releaseJob(limiter: Limiter, job: unknown): void {
    jobReleasers.get(limiter)?.(job);
}

/**
 * **Creates concurrency and rate-limited job processor**: pulls jobs and coordinates execution across buckets.
 *
 * - starts ready jobs within concurrency and rate bucket limits
 * - manages exponential backoff and global outage locking
 * - never throws: routes all failures to job outcomes, hooks, or logger
 */
export function createLimiter<J, D>(options: LimiterOptions<J, D>): Limiter {
    return createLimiterWith(options, {});
}

/** Internal limiter factory accepting package-private options. */
export function createLimiterWith<J, D>(options: LimiterOptions<J, D>, internal: InternalLimiterOptions<J>): Limiter {
    const log = callSafely(() => options?.log);
    const logger = createLogger(log.ok ? log.data : undefined);
    const resolved = resolveOptions(options);
    if (resolved.ok) {
        return createConfiguredLimiter(resolved.data, logger, internal);
    }
    const { error } = resolved;
    return {
        start() {
            logger.error(error.code, error.message, error.cause);
            return Promise.resolve({ ok: false, error });
        },
        stop() {
            return Promise.resolve({ ok: false, error });
        },
        wake() {
            return getInvalidStatus();
        },
        isIdle() {
            return true;
        },
        getStatus: getInvalidStatus,
    };
}

// -------------------------------------------------------------------
// --- 4. Private Functions
// -------------------------------------------------------------------

/** Returns default status for unconfigured or errored limiter. */
function getInvalidStatus(): LimiterStatus {
    return { state: 'idle', running: 0, buffered: 0, lockUntil: 0, lastStoppedBy: 'error' };
}

/** Instantiates configured limiter runner and state machine. */
function createConfiguredLimiter<J, D>(
    options: ResolvedLimiterOptions<J, D>,
    logger: Logger,
    internal: InternalLimiterOptions<J>,
): Limiter {
    const { buckets, clock } = options;
    let currentRun: RunState<J> | undefined;
    const lock: LimiterLockState = {
        status: 'unlocked',
        epoch: 0,
        lockedSince: 0,
        lockUntil: 0,
        consecutiveFailures: 0,
        isStoppedByUser: false,
        lastStoppedBy: undefined,
    };

    function start(startOptions?: StartOptions): Promise<Result<RunSummary, LimiterError>> {
        if (currentRun) {
            return Promise.resolve({ ok: false, error: createLimiterError('ALREADY_RUNNING') });
        }
        lock.isStoppedByUser = false;
        if (isObject(startOptions) && startOptions.force === true) {
            lock.status = 'unlocked';
            lock.lockedSince = 0;
            lock.lockUntil = 0;
            lock.consecutiveFailures = 0;
        } else if (lock.status !== 'unlocked') {
            lock.status = 'waiting';
            lock.lockedSince = clock.now();
        }
        lock.epoch++;

        return new Promise((resolve) => {
            const run = createRunState<J>(resolve);
            if (lock.status !== 'unlocked') {
                run.lockStartedAt = lock.lockedSince;
            }
            currentRun = run;
            logger.log('Limiter started.');
            for (const bucket of buckets) {
                if (bucket.subscribe) {
                    const subscribed = callSafely(() => bucket.subscribe?.(() => wakeSoon(run)));
                    if (!subscribed.ok || typeof subscribed.data !== 'function') {
                        failRun(
                            run,
                            createLimiterError('BUCKET_FAILED', subscribed.ok ? subscribed.data : subscribed.error),
                        );
                        return;
                    }
                    run.unsubscribers.push(subscribed.data);
                }
            }
            startReadyJobs();
        });
    }

    function stop(stopOptions?: StopOptions): Promise<Result<RunSummary, LimiterError>> {
        lock.isStoppedByUser = true;
        const run = currentRun;
        if (!run) {
            return Promise.resolve({
                ok: true,
                data: { done: 0, failed: 0, released: 0, stoppedBy: 'stop' },
            });
        }
        return new Promise((resolve) => {
            run.stopResolvers.push(resolve);
            requestStop(run, 'stop');
            if (isObject(stopOptions) && stopOptions.abort === true) {
                run.stopAbortReason ??= new DOMException('The limiter was stopped.', 'AbortError');
                for (const controller of run.controllers) {
                    controller.abort(run.stopAbortReason);
                }
            }
        });
    }

    function wake(): LimiterStatus {
        const run = currentRun;
        if (run) {
            if (!run.isStopping) {
                run.wakeCount++;
                run.isSourceEmpty = false;
                wakeSoon(run);
            }
        } else if (canStartOnWake()) {
            void start();
        }
        return getStatus();
    }

    /** Checks if limiter may auto-start on wake. */
    function canStartOnWake(): boolean {
        if (lock.isStoppedByUser || (lock.lastStoppedBy !== undefined && lock.lastStoppedBy !== 'empty')) {
            return false;
        }
        return !(lock.status === 'waiting' && lock.lockUntil - clock.now() > options.maxWaitMs);
    }

    function isIdle(): boolean {
        return currentRun === undefined;
    }

    function getStatus(): LimiterStatus {
        const run = currentRun;
        return {
            state: getState(run),
            running: run?.controllers.size ?? 0,
            buffered: run?.queue.length ?? 0,
            lockUntil: lock.status === 'waiting' ? lock.lockUntil : 0,
            lastStoppedBy: lock.lastStoppedBy,
        };
    }

    function getState(run: RunState<J> | undefined): LimiterState {
        if (!run) {
            return 'idle';
        }
        if (run.isStopping) {
            return 'stopping';
        }
        if (lock.status === 'retrying') {
            return 'probing';
        }
        return lock.status === 'waiting' ? 'locked' : 'running';
    }

    /** Batches queue checks into a single microtask. */
    function wakeSoon(run: RunState<J>): void {
        if (run.isWakeQueued) {
            return;
        }
        run.isWakeQueued = true;
        queueMicrotask(() => {
            run.isWakeQueued = false;
            if (currentRun === run) {
                startReadyJobs();
            }
        });
    }

    /** Computes exponential backoff delay capped at `maxRetryDelayMs`. */
    function getBackoffMs(failures: number): number {
        // Clamp exponent to prevent arithmetic overflow.
        const exponent = Math.min(Math.max(failures - 1, 0), 30);
        return Math.min(options.retryDelayMs * 2 ** exponent, options.maxRetryDelayMs);
    }

    /**
     * **Dispatches queued jobs within limits**: schedules timers or waits when blocked.
     *
     * - `isTimedWake`: starts one job per macrotask to interleave limiters sharing buckets
     */
    function startReadyJobs(isTimedWake = false): void {
        const run = currentRun;
        if (!run) {
            return;
        }
        clearTimer(run);
        if (run.isStopping) {
            resolveIfFinished(run);
            return;
        }
        if (lock.status === 'waiting') {
            // Stop run when lock outlasts maxWaitMs without succeeding probe.
            markSourceDone(run);
            const isNothingLeft = run.queue.length === 0 && !run.isFetching && run.isSourceEmpty;
            const lockedMs = clock.now() - lock.lockedSince;
            if (lockedMs > options.maxWaitMs && !isNothingLeft) {
                stopForMaxWait(run, lockedMs);
                return;
            }
        }
        if (lock.status === 'waiting') {
            const now = clock.now();
            if (now < lock.lockUntil) {
                // Exit early if source is exhausted instead of waiting out lock.
                markSourceDone(run);
                resolveIfFinished(run);
                if (currentRun !== run) {
                    return;
                }
                const waitMs = lock.lockUntil - now;
                if (waitMs > options.maxWaitMs) {
                    stopForMaxWait(run, waitMs);
                    return;
                }
                run.timer = clock.setTimeout(startReadyJobs, Math.min(waitMs, MAX_TIMER_MS));
                return;
            }
        }
        while (!run.isStopping && run.controllers.size < options.concurrency && lock.status !== 'retrying') {
            const now = clock.now();
            const index = run.queue.findIndex((queued) => queued.retryAt <= now && !queued.isHookPending);
            if (index !== -1) {
                const queued = run.queue[index];
                const reservation = tryReserveBuckets(buckets, queued.cost, now);
                if (!reservation.ok) {
                    if (reservation.error.code === 'COST_EXCEEDS_LIMIT') {
                        run.queue.splice(index, 1);
                        void finishJob(
                            run,
                            queued,
                            { ok: false, status: 'failed', error: reservation.error },
                            queued.cost,
                            0,
                        );
                        continue;
                    }
                    failRun(run, reservation.error);
                    break;
                }
                if (!reservation.data.reserved) {
                    const { waitMs, waitForChange } = reservation.data;
                    if (waitMs > options.maxWaitMs) {
                        stopForMaxWait(run, waitMs);
                        break;
                    }
                    scheduleWakeUp(run, now, waitMs, waitForChange);
                    break;
                }
                run.queue.splice(index, 1);
                if (lock.status === 'waiting') {
                    lock.status = 'retrying';
                    lock.epoch++;
                }
                void runAttempt(run, queued, reservation.data.reservations, now, lock.epoch);
                if (isTimedWake) {
                    run.timer = clock.setTimeout(() => startReadyJobs(true), 0);
                    break;
                }
                continue;
            }
            // Reserve concurrency slots for retries awaiting onAttempt hook resolution.
            const hookPending = run.queue.filter((queued) => queued.retryAt <= now && queued.isHookPending).length;
            const isSlotFree =
                lock.status === 'waiting'
                    ? hookPending === 0
                    : run.controllers.size + hookPending < options.concurrency;
            if (!isSlotFree) {
                scheduleWakeUp(run, now, Number.POSITIVE_INFINITY);
                break;
            }
            markSourceDone(run);
            const canFetch = !run.isFetching && !run.isSourceEmpty && run.queue.length < options.maxBufferSize;
            if (!canFetch) {
                scheduleWakeUp(run, now, Number.POSITIVE_INFINITY);
                break;
            }
            const preCheck = checkBuckets(buckets, { requests: 1 }, now);
            if (!preCheck.ok) {
                failRun(run, preCheck.error);
                break;
            }
            const { waitMs, waitForChange } = preCheck.data;
            if (waitMs > options.maxWaitMs) {
                stopForMaxWait(run, waitMs);
                break;
            }
            if (waitMs > 0 || waitForChange) {
                scheduleWakeUp(run, now, waitMs, waitForChange);
                break;
            }
            scheduleWakeUp(run, now, Number.POSITIVE_INFINITY);
            void fetchNextJob(run, isTimedWake);
            break;
        }
        resolveIfFinished(run);
    }

    /** Marks source exhausted if internal source reports completion. */
    function markSourceDone(run: RunState<J>): void {
        if (!run.isFetching && internal.isSourceDone?.() === true) {
            run.isSourceEmpty = true;
        }
    }

    /** Schedules wake-up timer for next bucket window or retry deadline. */
    function scheduleWakeUp(run: RunState<J>, now: number, ms: number, isWaitingForChange = false): void {
        let delayMs = isWaitingForChange ? Number.POSITIVE_INFINITY : ms;
        for (const queued of run.queue) {
            if (queued.retryAt > now) {
                delayMs = Math.min(delayMs, queued.retryAt - now);
            }
        }
        const lockedMsAtWake = now + delayMs - lock.lockedSince;
        if (
            lock.status === 'waiting' &&
            !isWaitingForChange &&
            lockedMsAtWake > options.maxWaitMs &&
            isNextStartKnown(run, delayMs)
        ) {
            stopForMaxWait(run, lockedMsAtWake);
            return;
        }
        if (lock.status === 'waiting' && isWaitingForChange && Number.isFinite(options.maxWaitMs)) {
            // Arm deadline check when waiting for dynamic capacity changes.
            const deadlineMs = options.maxWaitMs - (now - lock.lockedSince) + 1;
            run.lockTimer = clock.setTimeout(startReadyJobs, Math.min(deadlineMs, MAX_TIMER_MS));
        }
        if (Number.isFinite(delayMs)) {
            run.timer = clock.setTimeout(() => startReadyJobs(true), Math.min(delayMs, MAX_TIMER_MS));
        }
    }

    /** Checks if any job can start before scheduled wake-up. */
    function isNextStartKnown(run: RunState<J>, delayMs: number): boolean {
        const canFetch = !run.isSourceEmpty && run.queue.length < options.maxBufferSize;
        return (
            Number.isFinite(delayMs) &&
            run.controllers.size === 0 &&
            !run.isFetching &&
            !canFetch &&
            !run.queue.some((queued) => queued.isHookPending)
        );
    }

    /** Pulls and enqueues next job from source. */
    async function fetchNextJob(run: RunState<J>, isTimedWake: boolean): Promise<void> {
        run.isFetching = true;
        const wakeCountAtStart = run.wakeCount;
        const fetched = await callSafelyAsync(() => options.fetchJob());
        run.isFetching = false;
        // Protect against throwing getters or revoked proxies.
        const read = fetched.ok ? callSafely(() => readEntry(fetched.data)) : fetched;
        const entry = read.ok ? read.data : read;
        if (!entry.ok) {
            failRun(run, createLimiterError('FETCH_FAILED', entry.error));
            return;
        }
        if (entry.data === null) {
            if (run.wakeCount === wakeCountAtStart) {
                run.isSourceEmpty = true;
            }
            startReadyJobs(isTimedWake);
            return;
        }
        const { job, cost, validCost } = entry.data;
        const queued: QueuedJob<J> = {
            job,
            cost: { requests: 1, ...validCost },
            attempts: 0,
            failures: 0,
            backoffFailures: 0,
            lastAttempt: undefined,
            isHookPending: false,
            retryAt: 0,
            firstFailedAt: undefined,
            lockedMsAtFirstFailure: 0,
        };
        if (run.isStopping) {
            flushJobs(run, [queued], run.stoppedBy);
            return;
        }
        if (run.releaseRequested.has(job)) {
            // Released while being fetched.
            void finishJob(run, queued, abortedOutcome(), queued.cost, 0);
            startReadyJobs(isTimedWake);
            return;
        }
        if (!validCost) {
            void finishJob(
                run,
                queued,
                { ok: false, status: 'failed', error: createLimiterError('INVALID_COST', cost) },
                queued.cost,
                0,
            );
        } else {
            run.queue.push(queued);
        }
        startReadyJobs(isTimedWake);
    }

    /** Validates and parses fetched entry structure. */
    function readEntry(value: unknown): Result<FetchedEntry<J> | null, unknown> {
        if (value === null || value === undefined) {
            return { ok: true, data: null };
        }
        if (!isObject(value) || !('job' in value)) {
            return { ok: false, error: value };
        }
        const { job, cost } = value as { job: J; cost?: unknown };
        return { ok: true, data: { job, cost, validCost: cost === undefined ? {} : validateCost(cost) } };
    }

    /** Executes single attempt: runs job, settles buckets, and triggers hooks. */
    async function runAttempt(
        run: RunState<J>,
        queued: QueuedJob<J>,
        reservations: unknown[],
        startedAt: number,
        attemptEpoch: number,
    ): Promise<void> {
        run.pending++;
        queued.attempts++;
        const controller = new AbortController();
        run.controllers.add(controller);
        // Capture abort reason at settlement to ignore post-result stops.
        let abortReasonAtResult: unknown;
        const hangTimer = startHangWarning(queued);
        const result = await callProcessJob(options, queued.job, queued.attempts, controller, () => {
            abortReasonAtResult = controller.signal.reason;
        });
        if (hangTimer !== undefined) {
            callSafely(() => clock.clearTimeout(hangTimer));
        }
        run.controllers.delete(controller);
        const isAbortedByStop =
            !result.ok && run.stopAbortReason !== undefined && abortReasonAtResult === run.stopAbortReason;
        const finishedAt = clock.now();
        // Use reported usage or fall back to booked cost.
        const usage = result.ok ? result.usage : result.error.usage;
        const actualCost: Cost = { ...queued.cost, ...usage };
        for (const [index, bucket] of buckets.entries()) {
            callBucketSafely('settle', () => bucket.settle(reservations[index], actualCost, finishedAt));
        }
        const durationMs = finishedAt - startedAt;
        queued.lastAttempt = { cost: actualCost, durationMs };
        const { outcome, exceededWaitMs } = decideOutcome(
            run,
            queued,
            result,
            attemptEpoch,
            finishedAt,
            isAbortedByStop,
        );
        // Prevent job dispatch until onAttempt settles.
        queued.isHookPending = outcome === undefined;
        if (exceededWaitMs !== undefined) {
            stopForMaxWait(run, exceededWaitMs);
        }
        // Defer dispatch to yield to limiters sharing capacity.
        wakeSoon(run);
        const info: AttemptInfo = { attempt: queued.attempts, cost: actualCost, durationMs };
        await callHookSafely('onAttempt', () => options.onAttempt(queued.job, result, info), {
            job: logJob(queued.job),
            result,
            info,
        });
        if (outcome) {
            await finishJob(run, queued, outcome, actualCost, durationMs);
        } else {
            queued.isHookPending = false;
            if (run.isStopping || run.releaseRequested.has(queued.job)) {
                // Release job deferred during onAttempt hook.
                run.queue.splice(run.queue.indexOf(queued), 1);
                wakeSoon(run);
                const outcome = run.isStopping ? releasedOutcome(run.stoppedBy) : abortedOutcome();
                await finishJob(run, queued, outcome, actualCost, durationMs);
            } else {
                wakeSoon(run);
            }
        }
        run.pending--;
        resolveIfFinished(run);
    }

    /** Schedules warning if `processJob` hangs past timeout duration. */
    function startHangWarning(queued: QueuedJob<J>): unknown {
        const { timeoutMs } = options;
        if (timeoutMs === undefined) {
            return undefined;
        }
        const warnAfterMs = Math.min(2 * timeoutMs, MAX_TIMER_MS);
        const started = callSafely(() =>
            clock.setTimeout(() => {
                logger.warn(
                    `processJob is still running ${warnAfterMs} ms after it started, although timeoutMs (${timeoutMs} ms) aborted its signal; it holds its slot until it settles.`,
                    { job: logJob(queued.job), attempt: queued.attempts },
                );
            }, warnAfterMs),
        );
        return started.ok ? started.data : undefined;
    }

    /** Evaluates attempt outcome, lock state transitions, and retry decisions. */
    function decideOutcome(
        run: RunState<J>,
        queued: QueuedJob<J>,
        result: JobResult<D>,
        attemptEpoch: number,
        now: number,
        isAbortedByStop: boolean,
    ): Decision<D> {
        const isCurrentGeneration = attemptEpoch === lock.epoch;

        // 1. Aborted by stop({ abort: true }).
        if (isAbortedByStop) {
            return { outcome: releasedOutcome('stop') };
        }

        // 2. Aborted probe yields to next queued job.
        if (!result.ok && result.error.type !== 'backoff_global' && run.releaseRequested.has(queued.job)) {
            if (lock.status === 'retrying' && isCurrentGeneration) {
                lock.status = 'waiting';
                lock.epoch++;
            }
            return { outcome: abortedOutcome() };
        }

        // 3. Successful probe unlocks; other failures return to waiting.
        if (result.ok || result.error.type !== 'backoff_global') {
            let lockedMs = 0;
            if (lock.status !== 'unlocked' && isCurrentGeneration) {
                if (result.ok) {
                    endLockTime(run, now);
                    lock.status = 'unlocked';
                    lock.lockedSince = 0;
                    lock.lockUntil = 0;
                    lock.consecutiveFailures = 0;
                } else {
                    lock.status = 'waiting';
                    lockedMs = now - lock.lockedSince;
                }
                lock.epoch++;
            }
            const outcome = decideJobOutcome(run, queued, result, now);
            // Stop run if waiting lock exceeded maxWaitMs.
            return lockedMs > options.maxWaitMs && !run.isStopping
                ? { outcome, exceededWaitMs: lockedMs }
                : { outcome };
        }

        // 4. Global backoff.
        return decideGlobalBackoff(run, queued, result.error, isCurrentGeneration, now);
    }

    /** Returns cumulative time spent in global locks up to `now`. */
    function getLockedMs(run: RunState<J>, now: number): number {
        return run.lockedMs + (run.lockStartedAt === undefined ? 0 : now - run.lockStartedAt);
    }

    /** Records start of global lock duration. */
    function startLockTime(run: RunState<J>, now: number): void {
        run.lockStartedAt = now;
    }

    /** Accumulates completed global lock duration. */
    function endLockTime(run: RunState<J>, now: number): void {
        run.lockedMs = getLockedMs(run, now);
        run.lockStartedAt = undefined;
    }

    /** Determines job-specific terminal outcome or schedules individual retry. */
    function decideJobOutcome(
        run: RunState<J>,
        queued: QueuedJob<J>,
        result: JobResult<D>,
        now: number,
    ): JobOutcome<D> | undefined {
        if (result.ok) {
            return { ok: true, status: 'done', data: result.data };
        }
        const { error } = result;
        if (error.type === 'fatal') {
            return { ok: false, status: 'failed', error: createLimiterError('FATAL', error) };
        }
        queued.failures++;
        if (queued.failures > options.maxRetries) {
            return { ok: false, status: 'failed', error: createLimiterError('RETRIES_EXHAUSTED', error) };
        }
        let waitMs = 0;
        if (error.type === 'retry_backoff') {
            queued.backoffFailures++;
            if (queued.firstFailedAt === undefined) {
                queued.firstFailedAt = now;
                queued.lockedMsAtFirstFailure = getLockedMs(run, now);
            }
            waitMs = error.retryAfterMs ?? getBackoffMs(queued.backoffFailures);
            // Global lock duration does not count toward individual maxWaitMs.
            const waitedMs = now - queued.firstFailedAt - (getLockedMs(run, now) - queued.lockedMsAtFirstFailure);
            if (waitMs > options.maxWaitMs || waitedMs + waitMs > options.maxWaitMs) {
                return { ok: false, status: 'failed', error: createLimiterError('MAX_WAIT_EXCEEDED', error) };
            }
        }
        if (run.isStopping) {
            return releasedOutcome(run.stoppedBy);
        }
        if (error.type === 'retry_immediate') {
            logger.warn(`Attempt ${queued.attempts} failed (${error.message}), retrying immediately.`, error);
            queued.retryAt = 0;
        } else {
            logger.warn(`Attempt ${queued.attempts} failed (${error.message}), retrying in ${waitMs} ms.`, error);
            queued.retryAt = now + waitMs;
        }
        run.queue.push(queued);
        return undefined;
    }

    /** Handles `backoff_global` failures, adjusting lock duration and concurrency. */
    function decideGlobalBackoff(
        run: RunState<J>,
        queued: QueuedJob<J>,
        error: JobError,
        isCurrentGeneration: boolean,
        now: number,
    ): Decision<D> {
        queued.retryAt = 0;

        // Case 4a: Stale failure from previous lock generation.
        if (lock.status === 'unlocked' && !isCurrentGeneration) {
            if (run.isStopping) {
                return { outcome: releasedOutcome(run.stoppedBy) };
            }
            run.queue.push(queued);
            return { outcome: undefined };
        }

        // Case 4b: Initiate or advance global backoff lock.
        if (isCurrentGeneration) {
            if (lock.status === 'unlocked') {
                lock.lockedSince = now;
                lock.consecutiveFailures = 1;
                startLockTime(run, now);
            } else {
                lock.consecutiveFailures++;
            }
            // Enforce minimum lock duration to prevent tight probe loops.
            const waitMs =
                error.retryAfterMs ?? Math.max(getBackoffMs(lock.consecutiveFailures), MIN_GLOBAL_BACKOFF_MS);
            lock.status = 'waiting';
            lock.epoch++;
            lock.lockUntil = Math.max(lock.lockUntil, now + waitMs);
            if (run.isStopping) {
                return { outcome: releasedOutcome(run.stoppedBy) };
            }
            if (waitMs > options.maxWaitMs || now - lock.lockedSince + waitMs > options.maxWaitMs) {
                return { outcome: releasedOutcome('max_wait_exceeded', error), exceededWaitMs: waitMs };
            }
            logger.warn(`Global backoff triggered (${error.message}), pausing for ${waitMs} ms.`, error);
            // Place probe at front of queue.
            run.queue.unshift(queued);
            return { outcome: undefined };
        }

        // Case 4c: Concurrent failure during active lock; provider retryAfterMs extends lock.
        const { retryAfterMs } = error;
        if (retryAfterMs !== undefined) {
            lock.lockUntil = Math.max(lock.lockUntil, now + retryAfterMs);
        }
        if (run.isStopping) {
            return { outcome: releasedOutcome(run.stoppedBy) };
        }
        if (
            retryAfterMs !== undefined &&
            (retryAfterMs > options.maxWaitMs || now - lock.lockedSince + retryAfterMs > options.maxWaitMs)
        ) {
            return { outcome: releasedOutcome('max_wait_exceeded', error), exceededWaitMs: retryAfterMs };
        }
        run.queue.push(queued);
        return { outcome: undefined };
    }

    /** Finalizes job outcome, updates counters, and invokes `onJobDone` hook. */
    async function finishJob(
        run: RunState<J>,
        queued: QueuedJob<J>,
        outcome: JobOutcome<D>,
        actualCost: Cost,
        durationMs: number,
    ): Promise<void> {
        run.pending++;
        run.releaseRequested.delete(queued.job);
        if (outcome.status === 'done') {
            run.done++;
        } else if (outcome.status === 'failed') {
            run.failed++;
            logger.error(outcome.error.code, `Job failed: ${outcome.error.message}`, {
                job: logJob(queued.job),
                error: outcome.error,
            });
        } else {
            run.released++;
        }
        const info: AttemptInfo = { attempt: queued.attempts, cost: actualCost, durationMs };
        await callHookSafely('onJobDone', () => options.onJobDone(queued.job, outcome, info), {
            job: logJob(queued.job),
            outcome,
            info,
        });
        run.pending--;
        resolveIfFinished(run);
    }

    /** Initiates limiter shutdown; releases buffered jobs and awaits active attempts. */
    function requestStop(run: RunState<J>, reason: StopReason): void {
        if (run.isStopping) {
            resolveIfFinished(run);
            return;
        }
        if (lock.status === 'retrying') {
            lock.status = 'waiting';
        }
        run.isStopping = true;
        run.stoppedBy = reason;
        clearTimer(run);
        logger.log('Stopping: no new jobs are started.');
        callSafely(() => internal.onStop?.(reason, run.error));
        // Jobs with active onAttempt hooks are deferred until completion.
        const flushed = run.queue.filter((queued) => !queued.isHookPending);
        run.queue = run.queue.filter((queued) => queued.isHookPending);
        flushJobs(run, flushed, reason);
        resolveIfFinished(run);
    }

    /** Flushes queued jobs with released status. */
    function flushJobs(run: RunState<J>, items: readonly QueuedJob<J>[], reason: StopReason): void {
        for (const queued of items) {
            const { cost, durationMs } = queued.lastAttempt ?? { cost: queued.cost, durationMs: 0 };
            void finishJob(run, queued, releasedOutcome(reason), cost, durationMs);
        }
    }

    /** Creates released outcome for stopped or timed-out limiter. */
    function releasedOutcome(reason: StopReason, cause?: JobError): JobOutcome<D> {
        const code = reason === 'max_wait_exceeded' ? 'MAX_WAIT_EXCEEDED' : 'STOPPED';
        return { ok: false, status: 'released', error: createLimiterError(code, cause) };
    }

    /** Creates released outcome for aborted job. */
    function abortedOutcome(): JobOutcome<D> {
        return { ok: false, status: 'released', error: createLimiterError('ABORTED') };
    }

    /** Internal handler to cancel and release a job by identity. */
    function releaseQueuedJob(job: unknown): void {
        const run = currentRun;
        if (!run || run.isStopping) {
            return;
        }
        const index = run.queue.findIndex((queued) => queued.job === job && !queued.isHookPending);
        if (index === -1) {
            // Defer release if running, fetching, or awaiting onAttempt hook.
            run.releaseRequested.add(job);
            return;
        }
        const [queued] = run.queue.splice(index, 1);
        const { cost, durationMs } = queued.lastAttempt ?? { cost: queued.cost, durationMs: 0 };
        void finishJob(run, queued, abortedOutcome(), cost, durationMs);
        wakeSoon(run);
    }

    /** Stops limiter when required wait duration exceeds `maxWaitMs`. */
    function stopForMaxWait(run: RunState<J>, waitMs: number): void {
        logger.error('MAX_WAIT_EXCEEDED', ERROR_CODES.MAX_WAIT_EXCEEDED, { waitMs, maxWaitMs: options.maxWaitMs });
        requestStop(run, 'max_wait_exceeded');
    }

    /** Aborts run on fatal error and releases buffered jobs. */
    function failRun(run: RunState<J>, error: LimiterError): void {
        if (!run.isStopping) {
            run.error = error;
        }
        logger.error(error.code, error.message, error.cause);
        requestStop(run, 'stop');
    }

    /** Checks completion conditions and resolves active run promises. */
    function resolveIfFinished(run: RunState<J>): void {
        if (currentRun !== run) {
            return;
        }
        const isIdle = run.pending === 0 && !run.isFetching && run.queue.length === 0;
        if (!isIdle || !(run.isSourceEmpty || run.isStopping)) {
            return;
        }
        clearTimer(run);
        for (const unsubscribe of run.unsubscribers.splice(0)) {
            callBucketSafely('unsubscribe', unsubscribe);
        }
        currentRun = undefined;
        lock.lastStoppedBy = run.error ? 'error' : run.stoppedBy;
        const result = getRunResult(run);
        if (result.ok) {
            const summary = result.data;
            logger.log(
                `Limiter finished: ${summary.done} done, ${summary.failed} failed, ${summary.released} released.`,
                summary,
            );
        }
        run.resolve(result);
        for (const resolveStop of run.stopResolvers.splice(0)) {
            resolveStop(result);
        }
    }

    /** Builds final run summary result or error. */
    function getRunResult(run: RunState<J>): Result<RunSummary, LimiterError> {
        if (run.error) {
            return { ok: false, error: run.error };
        }
        return {
            ok: true,
            data: { done: run.done, failed: run.failed, released: run.released, stoppedBy: run.stoppedBy },
        };
    }

    /** Clears active wake-up and lock deadline timers on run state. */
    function clearTimer(run: RunState<J>): void {
        if (run.timer !== undefined) {
            clock.clearTimeout(run.timer);
            run.timer = undefined;
        }
        if (run.lockTimer !== undefined) {
            clock.clearTimeout(run.lockTimer);
            run.lockTimer = undefined;
        }
    }

    /** Safely executes bucket method, logging failures without disrupting run. */
    function callBucketSafely(name: string, fn: () => unknown): void {
        function logFailure(error: unknown): void {
            logger.error('BUCKET_FAILED', `A bucket's ${name} threw or rejected; the run continues.`, error);
        }
        const result = callSafely(fn, logFailure);
        if (!result.ok) {
            logFailure(result.error);
        }
    }

    /** Returns caller's job for log entries. */
    function logJob(job: J): unknown {
        return internal.unwrapJob ? internal.unwrapJob(job) : job;
    }

    /** Safely executes hook function, logging uncaught exceptions. */
    async function callHookSafely(name: string, fn: () => unknown, context: Record<string, unknown>): Promise<void> {
        const result = await callSafelyAsync(fn);
        if (!result.ok) {
            logger.error('HOOK_FAILED', `${name} threw.`, { ...context, error: result.error });
        }
    }

    const limiter: Limiter = { start, stop, wake, isIdle, getStatus };
    lockStates.set(limiter, lock);
    jobReleasers.set(limiter, releaseQueuedJob);
    return limiter;
}

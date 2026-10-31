// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { ERROR_CODES } from './errors.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Value or promise resolving to value. */
export type Awaitable<T> = T | Promise<T>;

/** Result container with data on success or error on failure. */
export type Result<T, E> = { ok: true; data: T } | { ok: false; error: E };

/** Resource consumption map per unit dimension, e.g. { requests: 1, tokens: 1200 }. */
export type Cost = Readonly<Record<string, number>>;

/** Stable limiter error code. */
export type LimiterErrorCode = keyof typeof ERROR_CODES;

/** Structured limiter error object. */
export type LimiterError = { code: LimiterErrorCode; message: string; cause?: unknown };

/** Passive rate limit interface for capacity checks and reservations. */
export type Bucket = {
    /** Set when bucket was created with invalid options. */
    readonly configError?: LimiterError;
    /** Milliseconds until cost fits (0 = now, Infinity = never, WAIT_FOR_CHANGE = until event). */
    getWaitMs(cost: Cost, now: number): number;
    /** Reserve cost; returns reservation handle passed to settle. */
    reserve(cost: Cost, now: number): unknown;
    /** Replace reserved cost with actual cost used. */
    settle(reservation: unknown, actualCost: Cost, now: number): void;
    /** Subscribe to capacity change events for event-driven buckets. */
    subscribe?(onChange: () => void): () => void;
};

/** Lifecycle hooks for createBucket; omitted hooks default to no-op. */
export type BucketHooks<R> = {
    getWaitMs?: (cost: Cost, now: number) => number;
    reserve?: (cost: Cost, now: number) => R;
    settle?: (reservation: R, actualCost: Cost, now: number) => void;
    subscribe?: (onChange: () => void) => () => void;
};

/** Prior usage record with timestamp and optional amount. */
export type PriorUsageEntry = { readonly at: number; readonly amount?: number };

/** Initial usage: plain number (immediate) or array of timestamped entries. */
export type PriorUsage = number | readonly PriorUsageEntry[];

/** Sliding window options; requires exactly one unit limit next to perMs. */
export type SlidingWindowOptions = {
    /** Window length in milliseconds. */
    readonly perMs: number;
    /** Usage from before start (startup offset). */
    readonly used?: PriorUsage;
    readonly [unit: string]: number | PriorUsage | undefined;
};

/** Options for sliding window helper shortcuts. */
export type SlidingWindowShortcutOptions = { readonly used?: PriorUsage };

/** Time source abstraction for timers and timestamps. */
export type Clock = {
    now(): number;
    setTimeout(callback: () => void, ms: number): unknown;
    clearTimeout(timer: unknown): void;
};

/** Job fetched from source with optional bucket cost; requests: 1 is added automatically. */
export type FetchedJob<J> = { job: J; cost?: Cost };

/**
 * **Failure category controlling retry and backoff behavior.**
 *
 * - `backoff_global`: locks limiter for outages/global 429s; probes with single job after backoff
 * - `retry_backoff`: per-item delayed retry; other jobs continue running
 * - `retry_immediate`: instant retry without delay or lock; counts toward retries
 * - `fatal`: non-retryable error reported immediately
 */
export type JobErrorType = 'backoff_global' | 'retry_backoff' | 'retry_immediate' | 'fatal';

/** Structured error returned when job processing fails. */
export type JobError = {
    type: JobErrorType;
    message: string;
    code?: string;
    /** Delay before retry or probe in milliseconds; overrides exponential backoff. */
    retryAfterMs?: number;
    usage?: Cost;
    cause?: unknown;
};

/** Outcome of single job processing attempt with optional usage adjustment. */
export type JobResult<D> = { ok: true; data: D; usage?: Cost } | { ok: false; error: JobError };

/** Execution context passed to processJob. */
export type JobContext = { attempt: number; signal: AbortSignal };

/** Final resolution status of a job. */
export type JobOutcomeStatus = 'done' | 'failed' | 'released';

/**
 * **Final outcome of a completed, failed, or released job.**
 *
 * - `status: 'done'`: job succeeded; contains result data
 * - `status: 'failed'`: permanent failure (fatal, retries exhausted, or wait exceeded)
 * - `status: 'released'`: unfinished job flushed on stop, abort, or global timeout
 */
export type JobOutcome<D> =
    | { ok: true; status: 'done'; data: D }
    | { ok: false; status: 'failed' | 'released'; error: LimiterError };

/** Metrics for a single job attempt including settled cost and duration. */
export type AttemptInfo = { attempt: number; cost: Cost; durationMs: number };

/** Limiter log severity level. */
export type LogLevel = 'log' | 'warn' | 'error';

/** Structured limiter log entry. */
export type LogEntry = { level: LogLevel; message: string; code?: LimiterErrorCode; data?: unknown };

/** Trigger that concluded the limiter run. */
export type StopReason = 'empty' | 'stop' | 'max_wait_exceeded';

/** Options for starting a limiter run. */
export type StartOptions = {
    /** Clears persisted global backoff lock to send requests immediately. */
    force?: boolean;
};

/** Aggregate outcome counts for a concluded limiter run. */
export type RunSummary = { done: number; failed: number; released: number; stoppedBy: StopReason };

/** Limiter lifecycle state. */
export type LimiterState = 'idle' | 'running' | 'locked' | 'probing' | 'stopping';

/** Real-time operational status snapshot. */
export type LimiterStatus = {
    /** Current lifecycle state. */
    state: LimiterState;
    /** ProcessJob attempts currently in flight. */
    running: number;
    /** Jobs waiting in internal retry buffer. */
    buffered: number;
    /** Timestamp when global backoff lock expires; 0 when unlocked. */
    lockUntil: number;
    /** Termination reason of most recently completed run. */
    lastStoppedBy: StopReason | 'error' | undefined;
};

/** Configuration options for createLimiter. */
export type LimiterOptions<J, D> = {
    /** Fetch next job or return null/undefined when empty. */
    fetchJob: () => Awaitable<FetchedJob<J> | null | undefined>;
    /** Execute single attempt of job. */
    processJob: (job: J, context: JobContext) => Awaitable<JobResult<D>>;
    /** Rate limit buckets applied to each attempt. */
    buckets?: readonly Bucket[];
    /** Maximum concurrent jobs in flight; default 5. */
    concurrency?: number;
    /** Maximum queued retry jobs before pausing fetchJob; defaults to concurrency (Infinity for createQueue). */
    maxBufferSize?: number;
    /** Maximum retries per job for retryable errors; default 4. */
    maxRetries?: number;
    /** Base exponential backoff delay in milliseconds; default 2000. */
    retryDelayMs?: number;
    /** Maximum backoff delay cap in milliseconds; default 300000 (5 min). */
    maxRetryDelayMs?: number;
    /** Maximum cumulative wait before aborting run or job; default 900000 (15 min). */
    maxWaitMs?: number;
    /** Per-attempt timeout in milliseconds; 0 or false disables. */
    timeoutMs?: number | false;
    /** Callback invoked after every attempt; next attempt and onJobDone wait for it to settle. */
    onAttempt?: (job: J, result: JobResult<D>, info: AttemptInfo) => unknown;
    /** Callback invoked once per job with final outcome. */
    onJobDone?: (job: J, result: JobOutcome<D>, info: AttemptInfo) => unknown;
    /** Logger callback for limiter events. */
    log?: (entry: LogEntry) => void;
    /** Custom clock for testing or time mocking. */
    clock?: Clock;
};

/** Configuration options for runAll batch execution. */
export type RunAllOptions<J, D> = Omit<LimiterOptions<J, D>, 'fetchJob'> & {
    /** Abort signal to stop batch execution. */
    signal?: AbortSignal;
};

/** Batch execution result in input order; failed runs retain finished and released items in data. */
export type RunAllResult<D> =
    | { ok: true; data: JobOutcome<D>[] }
    | { ok: false; error: LimiterError; data?: JobOutcome<D>[] };

/** Options for stopping an active limiter run. */
export type StopOptions = {
    /** Also abort running attempts through signal; unfinished attempts are released. */
    abort?: boolean;
};

/** Core rate limiter handle controlling execution lifecycle. */
export type Limiter = {
    /**
     * **Run limiter until queue empties or stop is requested.**
     *
     * - processes jobs from `fetchJob` within configured limits
     * - resets manual stop flag
     * - resolves immediately if global lock exceeds `maxWaitMs`
     * - never rejects
     */
    start(options?: StartOptions): Promise<Result<RunSummary, LimiterError>>;
    /**
     * **Stop active limiter run and release queued jobs.**
     *
     * - flushes buffered jobs as released
     * - awaits active attempts or aborts them if `abort: true`
     * - blocks `wake()` until next explicit `start()`
     * - do not await inside limiter callbacks (causes deadlock); call `void limiter.stop()`
     */
    stop(options?: StopOptions): Promise<Result<RunSummary, LimiterError>>;
    /** Trigger queue check or restart idle limiter if previous run emptied naturally. */
    wake(): LimiterStatus;
    /** Return true if no jobs are fetching, running, or waiting. */
    isIdle(): boolean;
    /** Return current status snapshot. */
    getStatus(): LimiterStatus;
};

/** Configuration options for createQueue. */
export type QueueOptions<J, D> = Omit<LimiterOptions<J, D>, 'fetchJob' | 'onJobDone'>;

/** Per-job options for queue push. */
export type PushOptions = {
    /** Bucket cost booked for each job attempt. */
    cost?: Cost;
    /** Abort signal releasing job immediately with ABORTED. */
    signal?: AbortSignal;
    /**
     * **Order among waiting jobs**: higher first, default `0`, push order among equal values.
     *
     * - finite number, negatives allowed; else push fails with `INVALID_CONFIG`
     * - applies only until limiter fetches job; fetched jobs and retries keep their place
     */
    priority?: number;
};

/** Limiter status extended with count of unconsumed waiting jobs. */
export type QueueStatus = LimiterStatus & {
    /** Pushed jobs waiting to enter limiter. */
    waiting: number;
};

/** FIFO job queue wrapping rate limiter. */
export type Queue<J, D> = {
    /** Enqueue job in FIFO order, starting run if idle; resolves with outcome and never rejects. */
    push(job: J, options?: PushOptions): Promise<JobOutcome<D>>;
    /** Release all waiting jobs and stop underlying limiter. */
    stop(options?: StopOptions): Promise<Result<RunSummary, LimiterError>>;
    /** Return current queue and limiter status snapshot. */
    getStatus(): QueueStatus;
};

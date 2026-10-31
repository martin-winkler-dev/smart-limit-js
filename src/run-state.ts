// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { Cost, LimiterError, Result, RunSummary, StopReason } from './types.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Fetched job with limiter tracking data across retry attempts. */
export type QueuedJob<J> = {
    /** Fetched job payload. */
    job: J;
    /** Booked bucket cost per attempt. */
    cost: Cost;
    /** Total attempt count. */
    attempts: number;
    /** Retryable failures checked against maxRetries. */
    failures: number;
    /** Backoff failures determining retry delay. */
    backoffFailures: number;
    /** Settled metrics of previous attempt. */
    lastAttempt: { cost: Cost; durationMs: number } | undefined;
    /** True while previous attempt onAttempt hook executes. */
    isHookPending: boolean;
    /** Retry expiration timestamp; 0 for immediate retry. */
    retryAt: number;
    /** First failure timestamp for cumulative maxWaitMs. */
    firstFailedAt: number | undefined;
    /** Cumulative lock duration at time of first failure. */
    lockedMsAtFirstFailure: number;
};

/** Global backoff lock phase: unlocked, waiting, or retrying. */
export type LockStatus = 'unlocked' | 'waiting' | 'retrying';

/**
 * **Persistent lock state held on the limiter instance across runs.**
 *
 * - coordinates global outage backoff and single-probe retries
 * - tracks state transition epoch to ignore stale attempt completions
 */
export type LimiterLockState = {
    /** Current lock phase. */
    status: LockStatus;
    /** Transition epoch filtering stale attempt outcomes. */
    epoch: number;
    /** Timestamp when current global lock episode began. */
    lockedSince: number;
    /** Timestamp when current global backoff expires. */
    lockUntil: number;
    /** Consecutive global backoff failures in current episode. */
    consecutiveFailures: number;
    /** Flag set by stop() requiring explicit start() to resume. */
    isStoppedByUser: boolean;
    /** Termination reason of previous run. */
    lastStoppedBy: StopReason | 'error' | undefined;
};

/** Mutable execution state for a single active limiter run. */
export type RunState<J> = {
    /** Fetched jobs and jobs waiting for retry. */
    queue: QueuedJob<J>[];
    /** In-flight attempts and unsettled hooks awaiting completion. */
    pending: number;
    /** Cumulative duration spent in resolved global locks. */
    lockedMs: number;
    /** Start timestamp of active lock, or undefined if unlocked. */
    lockStartedAt: number | undefined;
    /** True while fetchJob is actively running. */
    isFetching: boolean;
    /** True when fetchJob returned empty without subsequent wake. */
    isSourceEmpty: boolean;
    /** Monotonic counter tracking wake calls during active fetch. */
    wakeCount: number;
    /** True when limiter shutdown has been initiated. */
    isStopping: boolean;
    /** Reason triggering shutdown. */
    stoppedBy: StopReason;
    /** Active timer handle for sleep or backoff delays. */
    timer: unknown;
    /** Timer stopping run when global lock outlasts maxWaitMs. */
    lockTimer: unknown;
    /** True when queue check microtask is scheduled. */
    isWakeQueued: boolean;
    /** Active attempt abort controllers for cancellation on stop. */
    controllers: Set<AbortController>;
    /** Abort reason passed to stop({ abort: true }). */
    stopAbortReason: unknown;
    /** Reactive bucket unsubscribe callbacks. */
    unsubscribers: (() => void)[];
    /** Jobs flagged for immediate release. */
    releaseRequested: Set<unknown>;
    /** Completed job count. */
    done: number;
    /** Permanently failed job count. */
    failed: number;
    /** Flushed or aborted job count. */
    released: number;
    /** Fatal error terminating the run. */
    error: LimiterError | undefined;
    /** Run promise resolution handler. */
    resolve: (result: Result<RunSummary, LimiterError>) => void;
    /** Pending stop promise resolution handlers. */
    stopResolvers: ((result: Result<RunSummary, LimiterError>) => void)[];
};

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/** Initializes default run state for new limiter execution. */
export function createRunState<J>(resolve: RunState<J>['resolve']): RunState<J> {
    return {
        queue: [],
        pending: 0,
        lockedMs: 0,
        lockStartedAt: undefined,
        isFetching: false,
        isSourceEmpty: false,
        wakeCount: 0,
        isStopping: false,
        stoppedBy: 'empty',
        timer: undefined,
        lockTimer: undefined,
        isWakeQueued: false,
        controllers: new Set(),
        stopAbortReason: undefined,
        unsubscribers: [],
        releaseRequested: new Set(),
        done: 0,
        failed: 0,
        released: 0,
        error: undefined,
        resolve,
        stopResolvers: [],
    };
}

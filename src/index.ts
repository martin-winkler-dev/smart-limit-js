// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

export type {
    AttemptInfo,
    Awaitable,
    Bucket,
    BucketHooks,
    Clock,
    Cost,
    FetchedJob,
    JobContext,
    JobError,
    JobErrorType,
    JobOutcome,
    JobOutcomeStatus,
    JobResult,
    Limiter,
    LimiterError,
    LimiterErrorCode,
    LimiterOptions,
    LimiterState,
    LimiterStatus,
    LogEntry,
    LogLevel,
    PriorUsage,
    PriorUsageEntry,
    PushOptions,
    Queue,
    QueueOptions,
    QueueStatus,
    Result,
    RunAllOptions,
    RunAllResult,
    RunSummary,
    SlidingWindowOptions,
    SlidingWindowShortcutOptions,
    StartOptions,
    StopOptions,
    StopReason,
} from './types.js';

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

export { WAIT_FOR_CHANGE } from './bucket.js';
export { ERROR_CODES } from './errors.js';

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

export {
    createBucket,
    createConcurrencyBucket,
    createRPD,
    createRPM,
    createSlidingWindow,
    createTPD,
    createTPM,
} from './bucket.js';
export { createLimiterError } from './errors.js';
export { createLimiter } from './limiter.js';
export { createQueue } from './queue.js';
export { runAll } from './run-all.js';

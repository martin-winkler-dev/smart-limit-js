// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { LimiterError, LimiterErrorCode } from './types.js';

// -------------------------------------------------------------------
// --- 1. Constants
// -------------------------------------------------------------------

/** Error codes mapped to human-readable explanations; match on code, not message. */
export const ERROR_CODES = {
    INVALID_CONFIG: 'Limiter or bucket options are invalid.',
    ALREADY_RUNNING: 'start() was called while the limiter is already running.',
    FETCH_FAILED: 'fetchJob() threw or did not return { job, cost? } | null | undefined; the run was stopped.',
    INVALID_COST: 'A fetched job has a cost that is not an object of non-negative numbers.',
    COST_EXCEEDS_LIMIT: 'The job needs more than a bucket allows in total; it can never run.',
    BUCKET_FAILED: 'A bucket threw or returned an invalid value.',
    PROCESS_JOB_THREW: 'processJob() threw instead of returning a result.',
    INVALID_RESULT: 'processJob() returned something other than { ok: true, data } or { ok: false, error }.',
    TIMEOUT: 'processJob() took longer than timeoutMs.',
    FATAL: 'processJob() reported an error that must not be retried.',
    RETRIES_EXHAUSTED: 'The job exceeded maxRetries.',
    MAX_WAIT_EXCEEDED: 'A required wait or backoff duration exceeded maxWaitMs.',
    STOPPED: 'The limiter was stopped before the job finished.',
    ABORTED: "The job's signal was aborted before the job finished.",
    HOOK_FAILED: 'A hook (onAttempt, onJobDone) threw.',
} as const;

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/** Creates normalized limiter error with optional cause. */
export function createLimiterError(code: LimiterErrorCode, cause?: unknown): LimiterError {
    return cause === undefined ? { code, message: ERROR_CODES[code] } : { code, message: ERROR_CODES[code], cause };
}

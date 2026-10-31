// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { ERROR_CODES } from './errors.js';
import type { ResolvedLimiterOptions } from './options.js';
import type { Cost, JobError, JobErrorType, JobResult, LimiterErrorCode } from './types.js';
import { callSafely, callSafelyAsync, isFiniteNonNegative, isObject, validateCost } from './util.js';

// -------------------------------------------------------------------
// --- 1. Constants
// -------------------------------------------------------------------

/** Known job error types. */
const JOB_ERROR_TYPES: ReadonlySet<unknown> = new Set<JobErrorType>([
    'backoff_global',
    'retry_backoff',
    'retry_immediate',
    'fatal',
]);

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/**
 * **Runs single `processJob` attempt**: coordinates timeout, signal cancellation, and result normalization.
 *
 * - always resolves to well-formed `JobResult`; throws or malformed results map to `fatal`
 * - on timeout, aborts signal with `TimeoutError` and waits for execution to settle
 * - late `ok: true` or non-fatal wrapper errors preserved on timeout; throws/fatals become `TIMEOUT`
 * - `onSettled` called immediately when promise settles to distinguish pre/post-result aborts
 */
export async function callProcessJob<J, D>(
    options: Pick<ResolvedLimiterOptions<J, D>, 'processJob' | 'timeoutMs' | 'clock'>,
    job: J,
    attempt: number,
    controller: AbortController = new AbortController(),
    onSettled?: () => void,
): Promise<JobResult<D>> {
    const processing = callSafelyAsync(() => {
        const returned = options.processJob(job, { attempt, signal: controller.signal });
        if (onSettled) {
            void Promise.resolve(returned).then(onSettled, onSettled);
        }
        return returned;
    });
    const { clock, timeoutMs } = options;
    let timer: unknown;
    const timeout = new Promise<'timeout'>((resolve) => {
        if (timeoutMs !== undefined) {
            const started = callSafely(() => clock.setTimeout(() => resolve('timeout'), timeoutMs));
            timer = started.ok ? started.data : undefined;
        }
    });
    const outcome = await Promise.race([processing, timeout]);
    if (timer !== undefined) {
        callSafely(() => clock.clearTimeout(timer));
    }
    if (outcome === 'timeout') {
        controller.abort(new DOMException(ERROR_CODES.TIMEOUT, 'TimeoutError'));
        const settled = await processing;
        if (settled.ok) {
            const validated = validateJobResult<D>(settled.data);
            if (validated.ok || validated.error.type !== 'fatal') {
                return validated;
            }
            // Reported usage replaces booking on failed attempt.
            return createJobFailure(
                'retry_backoff',
                'TIMEOUT',
                validated.error.cause ?? validated.error,
                validated.error.usage,
            );
        }
        return createJobFailure('retry_backoff', 'TIMEOUT', settled.error);
    }
    if (!outcome.ok) {
        return createJobFailure('fatal', 'PROCESS_JOB_THREW', outcome.error);
    }
    return validateJobResult(outcome.data);
}

// -------------------------------------------------------------------
// --- 3. Private Functions
// -------------------------------------------------------------------

/** Safely validates job result; maps reader throws to fatal `INVALID_RESULT`. */
function validateJobResult<D>(value: unknown): JobResult<D> {
    const validated = callSafely(() => checkJobResult<D>(value));
    return validated.ok ? validated.data : createJobFailure('fatal', 'INVALID_RESULT', value);
}

/** Validates result shape; strips invalid `usage` and non-positive `retryAfterMs`. */
function checkJobResult<D>(value: unknown): JobResult<D> {
    if (!isObject(value) || (value.ok !== true && value.ok !== false)) {
        return createJobFailure('fatal', 'INVALID_RESULT', value);
    }
    if (value.ok === true) {
        const usage = validateCost(value.usage);
        return usage ? { ok: true, data: value.data as D, usage } : { ok: true, data: value.data as D };
    }
    const { error } = value;
    if (!isObject(error) || !JOB_ERROR_TYPES.has(error.type)) {
        return createJobFailure('fatal', 'INVALID_RESULT', value);
    }
    const validError: JobError = { ...(error as JobError), message: String(error.message ?? '') };
    const usage = validateCost(error.usage);
    if (usage) {
        validError.usage = usage;
    } else {
        delete validError.usage;
    }
    if (!(isFiniteNonNegative(error.retryAfterMs) && error.retryAfterMs > 0)) {
        delete validError.retryAfterMs;
    }
    return { ok: false, error: validError };
}

/** Builds normalized failed `JobResult`. */
function createJobFailure<D>(type: JobErrorType, code: LimiterErrorCode, cause?: unknown, usage?: Cost): JobResult<D> {
    const error: JobError = { type, code, message: ERROR_CODES[code] };
    if (usage !== undefined) {
        error.usage = usage;
    }
    if (cause !== undefined) {
        error.cause = cause;
    }
    return { ok: false, error };
}

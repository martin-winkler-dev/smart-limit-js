// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { createLimiterError } from './errors.js';
import type { Bucket, Clock, LimiterError, LimiterOptions, Result } from './types.js';
import { callSafely, isFiniteNonNegative, isObject, MAX_TIMER_MS } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Resolved options with defaults applied; optional `timeoutMs`, `log` handled separately. */
export type ResolvedLimiterOptions<J, D> = Required<Omit<LimiterOptions<J, D>, 'timeoutMs' | 'log'>> & {
    timeoutMs?: number;
};

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

/** Default system clock wrapping global timer functions. */
const SYSTEM_CLOCK: Clock = {
    now() {
        return Date.now();
    },
    setTimeout(callback, ms) {
        return setTimeout(callback, ms);
    },
    clearTimeout(timer) {
        clearTimeout(timer as ReturnType<typeof setTimeout>);
    },
};

/**
 * Default options applied to limiter instances.
 * @internal - exported for testing only
 */
export const DEFAULT_OPTIONS = {
    buckets: [],
    concurrency: 5,
    maxRetries: 4,
    retryDelayMs: 2_000,
    maxRetryDelayMs: 5 * 60_000,
    maxWaitMs: 15 * 60_000,
    onAttempt: doNothing,
    onJobDone: doNothing,
    clock: SYSTEM_CLOCK,
} satisfies Omit<ResolvedLimiterOptions<unknown, unknown>, 'fetchJob' | 'processJob' | 'maxBufferSize'>;

/** Known limiter option keys for validation. */
const OPTION_KEYS: Record<keyof LimiterOptions<unknown, unknown>, true> = {
    fetchJob: true,
    processJob: true,
    buckets: true,
    concurrency: true,
    maxBufferSize: true,
    maxRetries: true,
    retryDelayMs: true,
    maxRetryDelayMs: true,
    maxWaitMs: true,
    timeoutMs: true,
    onAttempt: true,
    onJobDone: true,
    log: true,
    clock: true,
};

const REQUIRED_FUNCTIONS = ['fetchJob', 'processJob'];
const OPTIONAL_FUNCTIONS = ['onAttempt', 'onJobDone', 'log'];
const MINIMUM_INTEGERS = { maxRetries: 0 };
const NON_NEGATIVE_DURATIONS = ['retryDelayMs', 'maxRetryDelayMs'];
const CLOCK_METHODS = ['now', 'setTimeout', 'clearTimeout'];
const BUCKET_METHODS = ['getWaitMs', 'reserve', 'settle'];

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

/** Validates limiter options and applies defaults. */
export function resolveOptions<J, D>(
    options: LimiterOptions<J, D>,
): Result<ResolvedLimiterOptions<J, D>, LimiterError> {
    // Snapshot options once to isolate getter side effects.
    const read = callSafely(() => readOptions(options));
    if (!read.ok) {
        return { ok: false, error: createLimiterError('INVALID_CONFIG', read.error) };
    }
    const problem = findInvalidOption(read.data);
    if (problem !== undefined) {
        return { ok: false, error: createLimiterError('INVALID_CONFIG', problem) };
    }
    const definedOptions = Object.fromEntries(
        Object.entries(read.data as Record<string, unknown>).filter(
            ([key, value]) => key !== 'timeoutMs' || !isDisabledTimeout(value),
        ),
    );
    const data = {
        ...DEFAULT_OPTIONS,
        ...(definedOptions as Omit<LimiterOptions<J, D>, 'timeoutMs'> & { timeoutMs?: number }),
    };
    // Copy array to prevent caller mutation.
    return {
        ok: true,
        data: { ...data, buckets: [...data.buckets], maxBufferSize: data.maxBufferSize ?? data.concurrency },
    };
}

/**
 * Copies defined known options, ignoring unknown keys.
 * @throws When property accessors throw.
 */
export function readOptions(options: unknown): unknown {
    if (!isObject(options)) {
        return options;
    }
    const copy: Record<string, unknown> = {};
    for (const key of Object.keys(OPTION_KEYS)) {
        const value = options[key];
        if (value !== undefined) {
            copy[key] = value;
        }
    }
    return copy;
}

// -------------------------------------------------------------------
// --- 4. Private Functions
// -------------------------------------------------------------------

/** Validates option values; returns first error description or undefined. */
function findInvalidOption(options: unknown): string | undefined {
    if (!isObject(options)) {
        return 'options must be an object';
    }
    for (const key of REQUIRED_FUNCTIONS) {
        if (typeof options[key] !== 'function') {
            return `${key} must be a function`;
        }
    }
    for (const key of OPTIONAL_FUNCTIONS) {
        if (options[key] !== undefined && typeof options[key] !== 'function') {
            return `${key} must be a function`;
        }
    }
    if (options.concurrency !== undefined && !isCount(options.concurrency)) {
        return 'concurrency must be an integer >= 1 or Infinity';
    }
    for (const [key, minimum] of Object.entries(MINIMUM_INTEGERS)) {
        const value = options[key];
        if (value !== undefined && !(Number.isInteger(value) && (value as number) >= minimum)) {
            return `${key} must be an integer >= ${minimum}`;
        }
    }
    for (const key of NON_NEGATIVE_DURATIONS) {
        if (options[key] !== undefined && !isFiniteNonNegative(options[key])) {
            return `${key} must be a finite number >= 0`;
        }
    }
    const { maxBufferSize, maxWaitMs, timeoutMs, clock, buckets } = options;
    if (maxBufferSize !== undefined && !isCount(maxBufferSize)) {
        return 'maxBufferSize must be an integer >= 1 or Infinity';
    }
    if (maxWaitMs !== undefined && !(typeof maxWaitMs === 'number' && maxWaitMs >= 0)) {
        return 'maxWaitMs must be a number >= 0 (Infinity allowed)';
    }
    if (!isDisabledTimeout(timeoutMs) && !(isFiniteNonNegative(timeoutMs) && timeoutMs <= MAX_TIMER_MS)) {
        return `timeoutMs must be a positive number <= ${MAX_TIMER_MS}, or 0, false or Infinity to disable it`;
    }
    if (clock !== undefined && !(isObject(clock) && CLOCK_METHODS.every((name) => typeof clock[name] === 'function'))) {
        return `clock must have ${CLOCK_METHODS.join(', ')}`;
    }
    if (buckets !== undefined && !Array.isArray(buckets)) {
        return 'buckets must be an array';
    }
    for (const [index, bucket] of ((buckets ?? []) as unknown[]).entries()) {
        if (!(isObject(bucket) && BUCKET_METHODS.every((name) => typeof bucket[name] === 'function'))) {
            return `buckets[${index}] must have ${BUCKET_METHODS.join(', ')}`;
        }
        const { subscribe } = bucket as Record<string, unknown>;
        if (subscribe !== undefined && typeof subscribe !== 'function') {
            return `buckets[${index}].subscribe must be a function`;
        }
        const { configError } = bucket as Bucket;
        if (configError) {
            return `buckets[${index}] is invalid: ${String(configError.cause)}`;
        }
    }
    return undefined;
}

/** Checks if value is integer >= 1 or positive infinity. */
function isCount(value: unknown): boolean {
    return value === Number.POSITIVE_INFINITY || (Number.isInteger(value) && (value as number) >= 1);
}

/** Checks if timeout is disabled (`undefined`, `false`, `0`, or `Infinity`). */
function isDisabledTimeout(value: unknown): boolean {
    return value === undefined || value === false || value === 0 || value === Number.POSITIVE_INFINITY;
}

/** No-op fallback for optional hooks. */
function doNothing(): void {}

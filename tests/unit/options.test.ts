// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createBucket, createRPM, createSlidingWindow } from '../../src/bucket.js';
import { ERROR_CODES } from '../../src/errors.js';
import { DEFAULT_OPTIONS, resolveOptions } from '../../src/options.js';
import { createFakeClock } from '../../src/testing.js';
import type { LimiterOptions } from '../../src/types.js';

function fetchJob() {
    return null;
}

function processJob() {
    return { ok: true as const, data: undefined };
}

const BASE: LimiterOptions<unknown, unknown> = { fetchJob, processJob };

function causeOf(options: unknown): unknown {
    const result = resolveOptions(options as LimiterOptions<unknown, unknown>);
    if (result.ok) {
        throw new Error('expected resolveOptions to fail');
    }
    expect(result.error.code).toBe('INVALID_CONFIG');
    expect(result.error.message).toBe(ERROR_CODES.INVALID_CONFIG);
    return result.error.cause;
}

describe('resolveOptions', () => {
    it('fills in DEFAULT_OPTIONS', () => {
        const result = resolveOptions(BASE);
        expect(result).toEqual({
            ok: true,
            data: { ...DEFAULT_OPTIONS, maxBufferSize: DEFAULT_OPTIONS.concurrency, fetchJob, processJob },
        });
    });

    it('defaults maxBufferSize to concurrency and keeps a given value', () => {
        const byDefault = resolveOptions({ ...BASE, concurrency: 3 });
        expect(byDefault.ok && byDefault.data.maxBufferSize).toBe(3);
        const unset = resolveOptions({ ...BASE, concurrency: 3, maxBufferSize: undefined } as unknown as typeof BASE);
        expect(unset.ok && unset.data.maxBufferSize).toBe(3);
        for (const maxBufferSize of [1, 10, Number.POSITIVE_INFINITY]) {
            const result = resolveOptions({ ...BASE, concurrency: 3, maxBufferSize });
            expect(result.ok && result.data.maxBufferSize).toBe(maxBufferSize);
        }
    });

    it.each([0, -1, 1.5, Number.NaN, Number.NEGATIVE_INFINITY, '5'])('rejects maxBufferSize = %p', (maxBufferSize) => {
        expect(causeOf({ ...BASE, maxBufferSize })).toBe('maxBufferSize must be an integer >= 1 or Infinity');
    });

    it('has the documented defaults', () => {
        expect(DEFAULT_OPTIONS).toMatchObject({
            buckets: [],
            concurrency: 5,
            maxRetries: 4,
            retryDelayMs: 2_000,
            maxRetryDelayMs: 300_000,
            maxWaitMs: 900_000,
        });
        expect(typeof DEFAULT_OPTIONS.clock.now()).toBe('number');
    });

    it('keeps given values and does not add timeoutMs', () => {
        const clock = createFakeClock();
        const buckets = [createBucket()];
        const result = resolveOptions({ ...BASE, concurrency: 2, maxRetries: 0, clock, buckets, timeoutMs: 500 });
        expect(result.ok).toBe(true);
        if (result.ok) {
            expect(result.data.concurrency).toBe(2);
            expect(result.data.maxRetries).toBe(0);
            expect(result.data.clock).toBe(clock);
            // Defensive copy prevents external mutation.
            expect(result.data.buckets).toEqual(buckets);
            expect(result.data.buckets).not.toBe(buckets);
            expect(result.data.timeoutMs).toBe(500);
            expect(result.data.maxRetryDelayMs).toBe(DEFAULT_OPTIONS.maxRetryDelayMs);
        }
        const withoutTimeout = resolveOptions(BASE);
        expect(withoutTimeout.ok && 'timeoutMs' in withoutTimeout.data).toBe(false);
    });

    it('falls back to defaults for undefined values', () => {
        const result = resolveOptions({
            ...BASE,
            concurrency: undefined,
            clock: undefined,
            buckets: undefined,
            onJobDone: undefined,
            timeoutMs: undefined,
        } as unknown as LimiterOptions<unknown, unknown>);
        expect(result.ok).toBe(true);
        if (result.ok) {
            expect(result.data.concurrency).toBe(DEFAULT_OPTIONS.concurrency);
            expect(result.data.clock).toBe(DEFAULT_OPTIONS.clock);
            expect(result.data.buckets).toEqual([]);
            expect(result.data.buckets).not.toBe(DEFAULT_OPTIONS.buckets);
            expect(result.data.onJobDone).toBe(DEFAULT_OPTIONS.onJobDone);
            expect('timeoutMs' in result.data).toBe(false);
        }
    });

    it('rejects non-object options', () => {
        expect(causeOf(undefined)).toBe('options must be an object');
        expect(causeOf(null)).toBe('options must be an object');
        expect(causeOf('options')).toBe('options must be an object');
    });

    it('requires fetchJob and processJob', () => {
        expect(causeOf({ processJob })).toBe('fetchJob must be a function');
        expect(causeOf({ fetchJob })).toBe('processJob must be a function');
        expect(causeOf({ fetchJob: [], processJob })).toBe('fetchJob must be a function');
    });

    it.each(['onAttempt', 'onJobDone', 'log'])('rejects a non-function %s', (key) => {
        expect(causeOf({ ...BASE, [key]: 'nope' })).toBe(`${key} must be a function`);
    });

    it.each([
        ['concurrency', 0, 'concurrency must be an integer >= 1 or Infinity'],
        ['concurrency', 1.5, 'concurrency must be an integer >= 1 or Infinity'],
        ['maxRetries', -1, 'maxRetries must be an integer >= 0'],
        ['maxRetries', '3', 'maxRetries must be an integer >= 0'],
    ])('rejects %s = %p', (key, value, cause) => {
        expect(causeOf({ ...BASE, [key]: value })).toBe(cause);
    });

    it('accepts concurrency Infinity for no cap; maxBufferSize then defaults to Infinity, a given one stays', () => {
        const unbounded = resolveOptions({ ...BASE, concurrency: Number.POSITIVE_INFINITY });
        expect(unbounded.ok && unbounded.data.concurrency).toBe(Number.POSITIVE_INFINITY);
        expect(unbounded.ok && unbounded.data.maxBufferSize).toBe(Number.POSITIVE_INFINITY);
        const buffered = resolveOptions({ ...BASE, concurrency: Number.POSITIVE_INFINITY, maxBufferSize: 10 });
        expect(buffered.ok && buffered.data.maxBufferSize).toBe(10);
    });

    it.each([-1, Number.NaN, Number.NEGATIVE_INFINITY, '5', 'Infinity', null, true])(
        'rejects concurrency = %p',
        (concurrency) => {
            expect(causeOf({ ...BASE, concurrency })).toBe('concurrency must be an integer >= 1 or Infinity');
        },
    );

    it('accepts the minimum integers', () => {
        expect(resolveOptions({ ...BASE, concurrency: 1, maxRetries: 0 }).ok).toBe(true);
    });

    it.each([
        ['retryDelayMs', Number.NaN],
        ['retryDelayMs', -1],
        ['maxRetryDelayMs', Number.POSITIVE_INFINITY],
        ['maxRetryDelayMs', -1],
    ])('rejects %s = %p', (key, value) => {
        expect(causeOf({ ...BASE, [key]: value })).toBe(`${key} must be a finite number >= 0`);
    });

    it('rejects a negative maxWaitMs but allows Infinity and 0', () => {
        expect(causeOf({ ...BASE, maxWaitMs: -1 })).toBe('maxWaitMs must be a number >= 0 (Infinity allowed)');
        expect(causeOf({ ...BASE, maxWaitMs: Number.NaN })).toBe('maxWaitMs must be a number >= 0 (Infinity allowed)');
        expect(resolveOptions({ ...BASE, maxWaitMs: Number.POSITIVE_INFINITY }).ok).toBe(true);
        expect(resolveOptions({ ...BASE, maxWaitMs: 0 }).ok).toBe(true);
    });

    it('requires a positive timeoutMs that fits a timer', () => {
        // Values above 2^31 - 1 overflow 32-bit signed timer integer.
        for (const timeoutMs of [-5, Number.NaN, Number.NEGATIVE_INFINITY, 2_147_483_648, 3_000_000_000, true, '500']) {
            expect(causeOf({ ...BASE, timeoutMs })).toBe(
                'timeoutMs must be a positive number <= 2147483647, or 0, false or Infinity to disable it',
            );
        }
        expect(resolveOptions({ ...BASE, timeoutMs: 0.5 }).ok).toBe(true);
        expect(resolveOptions({ ...BASE, timeoutMs: 2_147_483_647 }).ok).toBe(true);
    });

    it.each([0, false, Number.POSITIVE_INFINITY] as const)('disables the timeout for timeoutMs = %p', (timeoutMs) => {
        const result = resolveOptions({ ...BASE, timeoutMs });
        expect(result.ok).toBe(true);
        expect(result.ok && 'timeoutMs' in result.data).toBe(false);
    });

    it('requires subscribe to be a function when a bucket has one', () => {
        expect(causeOf({ ...BASE, buckets: [{ ...createBucket(), subscribe: true }] })).toBe(
            'buckets[0].subscribe must be a function',
        );
    });

    it('requires every clock method', () => {
        const { now, setTimeout } = createFakeClock();
        expect(causeOf({ ...BASE, clock: { now, setTimeout } })).toBe('clock must have now, setTimeout, clearTimeout');
        expect(causeOf({ ...BASE, clock: 'system' })).toBe('clock must have now, setTimeout, clearTimeout');
    });

    it('requires buckets to be an array', () => {
        expect(causeOf({ ...BASE, buckets: createBucket() })).toBe('buckets must be an array');
    });

    it('requires every bucket method', () => {
        const { getWaitMs, reserve } = createBucket();
        expect(causeOf({ ...BASE, buckets: [createBucket(), { getWaitMs, reserve }] })).toBe(
            'buckets[1] must have getWaitMs, reserve, settle',
        );
        expect(causeOf({ ...BASE, buckets: [null] })).toBe('buckets[0] must have getWaitMs, reserve, settle');
    });

    it('rejects a bucket with a configError', () => {
        const bucket = createSlidingWindow({ perMs: 0, requests: 1 });
        expect(causeOf({ ...BASE, buckets: [bucket] })).toBe(
            'buckets[0] is invalid: perMs must be a positive number of milliseconds',
        );
    });

    it('rejects a requests limit below 1, which no job could ever fit, but not other units below 1', () => {
        for (const bucket of [createSlidingWindow({ perMs: 1_000, requests: 0.5 }), createRPM(0.5)]) {
            expect(causeOf({ ...BASE, buckets: [bucket] })).toBe('buckets[0] is invalid: requests must be at least 1');
        }
        expect(resolveOptions({ ...BASE, buckets: [createSlidingWindow({ perMs: 1_000, tokens: 0.5 })] }).ok).toBe(
            true,
        );
    });

    it('reports the first invalid option', () => {
        expect(causeOf({ ...BASE, concurrency: 0, maxRetries: -1 })).toBe(
            'concurrency must be an integer >= 1 or Infinity',
        );
        expect(causeOf({ processJob: 1, fetchJob: 1 })).toBe('fetchJob must be a function');
    });

    it('validates and uses the same values: options inherited from a prototype count, unknown keys are left out', () => {
        const fromPrototype = Object.assign(Object.create({ fetchJob, processJob, concurrency: 3 }), { extra: 1 });
        expect(resolveOptions(fromPrototype)).toEqual({
            ok: true,
            data: { ...DEFAULT_OPTIONS, concurrency: 3, maxBufferSize: 3, fetchJob, processJob },
        });
    });

    it('reads each option once, so a getter cannot pass validation and then return something else', () => {
        let reads = 0;
        const options = {
            ...BASE,
            get concurrency() {
                reads++;
                return reads === 1 ? 2 : 0;
            },
        };
        const result = resolveOptions(options);
        expect(result.ok && result.data.concurrency).toBe(2);
        expect(reads).toBe(1);
    });

    it('reports an option that throws while it is read as INVALID_CONFIG', () => {
        const error = new Error('getter failed');
        const options = {
            ...BASE,
            get maxRetries(): never {
                throw error;
            },
        };
        expect(causeOf(options)).toBe(error);
    });
});

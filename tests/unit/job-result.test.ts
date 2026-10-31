// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { ERROR_CODES } from '../../src/errors.js';
import { callProcessJob } from '../../src/job-result.js';
import { createFakeClock } from '../../src/testing.js';
import type { JobContext, JobError, JobResult } from '../../src/types.js';

type Processor = (job: string, context: JobContext) => unknown;

function run(
    processJob: Processor,
    options: { timeoutMs?: number; attempt?: number; controller?: AbortController } = {},
) {
    const clock = createFakeClock();
    const promise = callProcessJob<string, unknown>(
        {
            processJob: processJob as never,
            clock,
            ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
        },
        'job-1',
        options.attempt ?? 1,
        options.controller,
    );
    return { clock, promise };
}

function failure(type: string, code: keyof typeof ERROR_CODES, cause?: unknown): JobResult<unknown> {
    const error = { type, code, message: ERROR_CODES[code] };
    return { ok: false, error: (cause === undefined ? error : { ...error, cause }) as never };
}

describe('callProcessJob', () => {
    it('passes data through and keeps valid usage', async () => {
        const { promise } = run(() => ({ ok: true, data: { id: 1 }, usage: { tokens: 50 } }));
        expect(await promise).toEqual({ ok: true, data: { id: 1 }, usage: { tokens: 50 } });
    });

    it('accepts an async processJob without usage', async () => {
        const { promise } = run(async () => ({ ok: true, data: 'x' }));
        const result = await promise;
        expect(result).toEqual({ ok: true, data: 'x' });
        expect('usage' in result).toBe(false);
    });

    it('drops invalid usage on success', async () => {
        const { promise } = run(() => ({ ok: true, data: 1, usage: { tokens: -1 } }));
        const result = await promise;
        expect(result).toEqual({ ok: true, data: 1 });
        expect('usage' in result).toBe(false);
    });

    it.each(['backoff_global', 'retry_backoff', 'retry_immediate', 'fatal'] as const)(
        'keeps a valid %s error and its valid usage and retryAfterMs',
        async (type) => {
            const error: JobError = {
                type,
                message: 'slow',
                code: 'X',
                retryAfterMs: 500,
                usage: { requests: 1 },
            };
            const { promise } = run(() => ({ ok: false, error }));
            expect(await promise).toEqual({ ok: false, error });
        },
    );

    it('drops invalid usage on errors', async () => {
        const { promise } = run(() => ({ ok: false, error: { type: 'retry_backoff', message: 'm', usage: 'lots' } }));
        const result = await promise;
        expect(result).toEqual({ ok: false, error: { type: 'retry_backoff', message: 'm' } });
        expect(!result.ok && 'usage' in result.error).toBe(false);
    });

    it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, '100'])('drops retryAfterMs = %p', async (retryAfterMs) => {
        const { promise } = run(() => ({ ok: false, error: { type: 'backoff_global', message: 'm', retryAfterMs } }));
        const result = await promise;
        expect(result).toEqual({ ok: false, error: { type: 'backoff_global', message: 'm' } });
        expect(!result.ok && 'retryAfterMs' in result.error).toBe(false);
    });

    it('coerces the message to a string', async () => {
        const numeric = run(() => ({ ok: false, error: { type: 'fatal', message: 404 } }));
        expect(await numeric.promise).toEqual({ ok: false, error: { type: 'fatal', message: '404' } });
        const missing = run(() => ({ ok: false, error: { type: 'fatal' } }));
        expect(await missing.promise).toEqual({ ok: false, error: { type: 'fatal', message: '' } });
    });

    it('turns a sync throw into a fatal PROCESS_JOB_THREW', async () => {
        const thrown = new Error('boom');
        const { promise } = run(() => {
            throw thrown;
        });
        expect(await promise).toEqual(failure('fatal', 'PROCESS_JOB_THREW', thrown));
    });

    it('turns a rejection into a fatal PROCESS_JOB_THREW', async () => {
        const { promise } = run(() => Promise.reject('nope'));
        expect(await promise).toEqual(failure('fatal', 'PROCESS_JOB_THREW', 'nope'));
    });

    it.each([
        ['undefined', undefined],
        ['a string', 'ok'],
        ['null', null],
        ['an object without ok', { data: 1 }],
        ['a non-boolean ok', { ok: 'yes', data: 1 }],
        ['an error that is not an object', { ok: false, error: 'bad' }],
        ['an error without type', { ok: false, error: { message: 'bad' } }],
        ['an error with an unknown type', { ok: false, error: { type: 'transient', message: 'bad' } }],
    ])('turns %s into a fatal INVALID_RESULT', async (_label, value) => {
        const { promise } = run(() => value);
        const expected = failure('fatal', 'INVALID_RESULT', value);
        expect(await promise).toEqual(expected);
    });

    it.each([
        ['a message without a prototype', { ok: false, error: { type: 'fatal', message: Object.create(null) } }],
        [
            'a message whose toString throws',
            {
                ok: false,
                error: {
                    type: 'retry_backoff',
                    message: {
                        toString() {
                            throw new Error('no string');
                        },
                    },
                },
            },
        ],
        [
            'a usage getter that throws',
            {
                ok: true,
                data: 1,
                get usage() {
                    throw new Error('no usage');
                },
            },
        ],
    ])('turns a result with %s into a fatal INVALID_RESULT instead of throwing', async (_label, value) => {
        expect(await run(() => value).promise).toEqual(failure('fatal', 'INVALID_RESULT', value));
    });

    it('turns a result that throws while read after a timeout into retry_backoff TIMEOUT', async () => {
        const value = { ok: false, error: { type: 'retry_backoff', message: Object.create(null) } };
        const { clock, promise } = run(
            (_job, context) =>
                new Promise((resolve) => {
                    context.signal.addEventListener('abort', () => resolve(value));
                }),
            { timeoutMs: 1_000 },
        );
        await clock.advance(1_000);
        expect(await promise).toEqual(failure('retry_backoff', 'TIMEOUT', value));
    });

    it('omits cause for an INVALID_RESULT of undefined', async () => {
        const result = await run(() => undefined).promise;
        expect(!result.ok && 'cause' in result.error).toBe(false);
    });

    it('passes the job, attempt and an unaborted signal', async () => {
        const calls: [string, number, boolean][] = [];
        const { promise } = run(
            (job, context) => {
                calls.push([job, context.attempt, context.signal.aborted]);
                return { ok: true, data: null };
            },
            { attempt: 3 },
        );
        await promise;
        expect(calls).toEqual([['job-1', 3, false]]);
    });

    it('aborts the signal with TimeoutError on timeout and turns a fatal/throw/invalid result into retry_backoff TIMEOUT', async () => {
        let signal: AbortSignal | undefined;
        const fatalError = { type: 'fatal' as const, message: 'aborted' };
        const { clock, promise } = run(
            (_job, context) => {
                signal = context.signal;
                return new Promise((resolve) => {
                    context.signal.addEventListener('abort', () => resolve({ ok: false, error: fatalError }));
                });
            },
            { timeoutMs: 1_000 },
        );
        let settled = false;
        void promise.then(() => {
            settled = true;
        });
        await clock.advance(999);
        expect(settled).toBe(false);
        expect(signal!.aborted).toBe(false);
        await clock.advance(1);
        expect(settled).toBe(true);
        expect(await promise).toEqual(failure('retry_backoff', 'TIMEOUT', fatalError));
        expect(signal!.aborted).toBe(true);
        expect((signal!.reason as DOMException).name).toBe('TimeoutError');
        expect((signal!.reason as DOMException).message).toBe(ERROR_CODES.TIMEOUT);
    });

    it('keeps the usage of a fatal result after timeout on the TIMEOUT failure', async () => {
        const fatalError = {
            type: 'fatal' as const,
            message: 'aborted',
            usage: { tokens: 9_000 },
            retryAfterMs: 5_000,
        };
        const { clock, promise } = run(
            (_job, context) =>
                new Promise((resolve) => {
                    context.signal.addEventListener('abort', () => resolve({ ok: false, error: fatalError }));
                }),
            { timeoutMs: 1_000 },
        );
        await clock.advance(1_000);
        const result = await promise;
        expect(result).toEqual({
            ok: false,
            error: {
                ...(failure('retry_backoff', 'TIMEOUT', fatalError) as { error: JobError }).error,
                usage: { tokens: 9_000 },
            },
        });
    });

    it('returns a late ok: true when processJob ignores the timeout abort and succeeds', async () => {
        const clock = createFakeClock();
        function processJob() {
            return new Promise<JobResult<string>>((resolve) => {
                clock.setTimeout(() => resolve({ ok: true, data: 'late', usage: { tokens: 42 } }), 5_000);
            });
        }
        const promise = callProcessJob<string, string>({ processJob, timeoutMs: 1_000, clock }, 'job-1', 1);
        let settled = false;
        void promise.then(() => {
            settled = true;
        });
        await clock.advance(1_000);
        expect(settled).toBe(false);
        await clock.advance(4_000);
        expect(await promise).toEqual({ ok: true, data: 'late', usage: { tokens: 42 } });
    });

    it('returns the wrapper error when processJob returns a non-fatal error after timeout', async () => {
        const wrapperError: JobError = {
            type: 'backoff_global',
            message: 'custom timeout backoff',
            retryAfterMs: 10_000,
        };
        const { clock, promise } = run(
            (_job, context) =>
                new Promise((resolve) => {
                    context.signal.addEventListener('abort', () => resolve({ ok: false, error: wrapperError }));
                }),
            { timeoutMs: 1_000 },
        );
        await clock.advance(1_000);
        expect(await promise).toEqual({ ok: false, error: wrapperError });
    });

    it('preserves a pre-existing stop abort reason when timeout fires later', async () => {
        const controller = new AbortController();
        const stopReason = new DOMException('The limiter was stopped.', 'AbortError');
        const { clock, promise } = run(
            (_job, { signal }) =>
                new Promise((resolve) => {
                    clock.setTimeout(() => {
                        expect(signal.reason).toBe(stopReason);
                        resolve({ ok: false, error: { type: 'fatal', message: 'aborted' } });
                    }, 2_000);
                }),
            { timeoutMs: 1_000, controller },
        );
        controller.abort(stopReason);
        await clock.advance(2_000);
        await promise;
        expect(controller.signal.reason).toBe(stopReason);
    });

    it('clears the timeout when processJob finishes first', async () => {
        const fakeClock = createFakeClock();
        const cleared: unknown[] = [];
        let started: unknown;
        const clock = {
            now: fakeClock.now,
            setTimeout(callback: () => void, ms: number) {
                started = fakeClock.setTimeout(callback, ms);
                return started;
            },
            clearTimeout(timer: unknown) {
                cleared.push(timer);
                fakeClock.clearTimeout(timer);
            },
        };
        let signal: AbortSignal | undefined;
        function processJob(_job: string, context: JobContext): JobResult<number> {
            signal = context.signal;
            return { ok: true, data: 1 };
        }
        const result = await callProcessJob({ processJob, timeoutMs: 1_000, clock }, 'job-1', 1);
        expect(result).toEqual({ ok: true, data: 1 });
        expect(started).toBeDefined();
        expect(cleared).toEqual([started]);
        await fakeClock.advance(5_000);
        expect(signal!.aborted).toBe(false);
    });
});

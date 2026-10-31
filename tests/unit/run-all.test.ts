// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createRPM, createTPM, type LogEntry, runAll } from '../../src/index.js';
import { createFakeClock } from '../../src/testing.js';

describe('runAll', () => {
    it('resolves with one JobOutcome per item in input order', async () => {
        const clock = createFakeClock();
        const attempts: string[] = [];
        const promise = runAll<string, string>(
            [{ job: 'slow' }, { job: 'bad' }, { job: 'fast', cost: { tokens: 1 } }],
            {
                clock,
                processJob: async (job) => {
                    await new Promise<void>((resolve) => clock.setTimeout(resolve, job === 'slow' ? 5_000 : 1));
                    return job === 'bad'
                        ? { ok: false, error: { type: 'fatal', message: 'no' } }
                        : { ok: true, data: job.toUpperCase() };
                },
                onAttempt: (job) => {
                    attempts.push(job);
                },
            },
        );
        const result = await clock.settle(promise);
        expect(result.ok && result.data.map((item) => [item.status, item.ok ? item.data : item.error.code])).toEqual([
            ['done', 'SLOW'],
            ['failed', 'FATAL'],
            ['done', 'FAST'],
        ]);
        expect(attempts.sort()).toEqual(['bad', 'fast', 'slow']);
    });

    it('runs every item of a sparse array, a hole as job undefined, and returns a result for each', async () => {
        const clock = createFakeClock();
        const seen: (string | undefined)[] = [];
        const items: { job: string }[] = [];
        items[0] = { job: 'a' };
        items[2] = { job: 'c' };
        const result = await clock.settle(
            runAll<string | undefined, string>(items, {
                clock,
                processJob: (job) => {
                    seen.push(job);
                    return { ok: true, data: String(job) };
                },
            }),
        );
        expect(seen).toEqual(['a', undefined, 'c']);
        expect(result.ok && result.data).toEqual([
            { ok: true, status: 'done', data: 'a' },
            { ok: true, status: 'done', data: 'undefined' },
            { ok: true, status: 'done', data: 'c' },
        ]);
        expect(result.ok && 1 in result.data).toBe(true);
    });

    it('gives items the run never finished status released with MAX_WAIT_EXCEEDED', async () => {
        const clock = createFakeClock();
        const promise = runAll<number, number>([{ job: 1 }, { job: 2 }], {
            clock,
            concurrency: 1,
            buckets: [createRPM(10)],
            processJob: () => ({
                ok: false,
                error: { type: 'backoff_global', message: '429', retryAfterMs: 3_600_000 },
            }),
        });
        const result = await clock.settle(promise);
        expect(result.ok && result.data.map((item) => [item.status, !item.ok && item.error.code])).toEqual([
            ['released', 'MAX_WAIT_EXCEEDED'],
            ['released', 'MAX_WAIT_EXCEEDED'],
        ]);
    });

    it('stops on signal: running attempts are aborted, finished items keep their outcome, the rest is released', async () => {
        const clock = createFakeClock();
        const controller = new AbortController();
        const aborted: number[] = [];
        const promise = runAll<number, number>([{ job: 1 }, { job: 2 }, { job: 3 }], {
            clock,
            concurrency: 1,
            signal: controller.signal,
            processJob: (job, { signal }) =>
                new Promise((resolve) => {
                    if (job === 1) {
                        resolve({ ok: true, data: 1 });
                        return;
                    }
                    signal.addEventListener('abort', () => {
                        aborted.push(job);
                        resolve({ ok: false, error: { type: 'retry_immediate', message: 'aborted' } });
                    });
                }),
        });
        await clock.advance(10);
        controller.abort();
        const result = await clock.settle(promise);
        expect(aborted).toEqual([2]);
        expect(result).toEqual({
            ok: true,
            data: [
                { ok: true, status: 'done', data: 1 },
                { ok: false, status: 'released', error: expect.objectContaining({ code: 'STOPPED' }) },
                { ok: false, status: 'released', error: expect.objectContaining({ code: 'STOPPED' }) },
            ],
        });
    });

    it('runs nothing for a signal that is already aborted and releases every item', async () => {
        const clock = createFakeClock();
        let calls = 0;
        const result = await clock.settle(
            runAll<number, number>([{ job: 1 }, { job: 2 }], {
                clock,
                signal: AbortSignal.abort(),
                processJob: (job) => {
                    calls++;
                    return { ok: true, data: job };
                },
            }),
        );
        expect(calls).toBe(0);
        expect(result.ok && result.data.map((item) => item.status)).toEqual(['released', 'released']);
    });

    it('rejects a signal that is not an AbortSignal with INVALID_CONFIG', async () => {
        const result = await runAll([{ job: 1 }], {
            processJob: () => ({ ok: true, data: 1 }),
            signal: {} as AbortSignal,
        });
        expect(result).toMatchObject({
            ok: false,
            error: { code: 'INVALID_CONFIG', cause: 'signal must be an AbortSignal' },
        });
        expect('data' in result).toBe(false);
    });

    it('passes INVALID_CONFIG on', async () => {
        const missing = await runAll([{ job: 1 }], {} as never);
        expect(!missing.ok && missing.error.code).toBe('INVALID_CONFIG');
        const notArray = await runAll('jobs' as never, { processJob: () => ({ ok: true, data: 1 }) });
        expect(!notArray.ok && notArray.error.code).toBe('INVALID_CONFIG');
    });

    it('resolves with INVALID_CONFIG instead of rejecting when an item or the options throw while read', async () => {
        function processJob() {
            return { ok: true as const, data: 1 };
        }
        const getterItem = {
            get job(): never {
                throw new Error('item getter failed');
            },
        };
        const revokedItem = Proxy.revocable({ job: 1 }, {});
        revokedItem.revoke();
        const revokedItems = Proxy.revocable([{ job: 1 }], {});
        revokedItems.revoke();
        const getterOptions = {
            processJob,
            get concurrency(): never {
                throw new Error('options getter failed');
            },
        };
        const results = await Promise.all([
            runAll([getterItem as never], { processJob }),
            runAll([revokedItem.proxy], { processJob }),
            runAll(revokedItems.proxy, { processJob }),
            runAll([{ job: 1 }], getterOptions),
        ]);
        for (const result of results) {
            expect(result).toMatchObject({ ok: false, error: { code: 'INVALID_CONFIG', cause: expect.any(Error) } });
        }
    });

    it('uses limiter options inherited from a prototype, like createLimiter does', async () => {
        const clock = createFakeClock();
        let running = 0;
        let maxRunning = 0;
        const options = Object.create({
            clock,
            concurrency: 1,
            processJob: async (job: number) => {
                running++;
                maxRunning = Math.max(maxRunning, running);
                await new Promise<void>((resolve) => clock.setTimeout(resolve, 10));
                running--;
                return { ok: true, data: job };
            },
        });
        const result = await clock.settle(runAll([{ job: 1 }, { job: 2 }, { job: 3 }], options));
        expect(result.ok && result.data.map((outcome) => outcome.status)).toEqual(['done', 'done', 'done']);
        expect(maxRunning).toBe(1);
    });

    it('resolves when the last item finishes, without waiting for bucket room it no longer needs', async () => {
        const clock = createFakeClock();
        // Usage exceeds bucket limit, but run completes without waiting for refill.
        const promise = runAll<number, number>([{ job: 1, cost: { tokens: 3 } }], {
            concurrency: 1,
            clock,
            buckets: [createTPM(10)],
            processJob: () => ({ ok: true, data: 1, usage: { tokens: 15 } }),
        });
        const result = await clock.settle(promise);
        expect(result.ok && result.data.map((item) => item.status)).toEqual(['done']);
        expect(clock.now()).toBe(0);
    });

    it('resolves an empty list at once, also with full buckets', async () => {
        const clock = createFakeClock();
        const promise = runAll<number, number>([], {
            clock,
            buckets: [createRPM(1, { used: 1 })],
            processJob: () => ({ ok: true, data: 1 }),
        });
        expect(await clock.settle(promise)).toEqual({ ok: true, data: [] });
        expect(clock.now()).toBe(0);
    });

    it('still waits for a retry of the last item after every item was handed out', async () => {
        const clock = createFakeClock();
        let attempts = 0;
        const promise = runAll<number, number>([{ job: 1 }], {
            clock,
            processJob: () =>
                ++attempts === 1
                    ? { ok: false, error: { type: 'retry_backoff', message: 'busy', retryAfterMs: 1_000 } }
                    : { ok: true, data: attempts },
        });
        const result = await clock.settle(promise);
        expect(result.ok && result.data[0]).toEqual({ ok: true, status: 'done', data: 2 });
        expect(clock.now()).toBe(1_000);
    });

    it('logs the caller job, not its internal wrapper, in Job failed and HOOK_FAILED', async () => {
        const logs: LogEntry[] = [];
        await runAll<string, string>([{ job: 'a' }], {
            processJob: () => ({ ok: false, error: { type: 'fatal', message: 'no' } }),
            onJobDone: () => {
                throw new Error('hook');
            },
            log: (entry) => logs.push(entry),
        });
        const jobs = logs
            .filter((entry) => entry.level === 'error')
            .map((entry) => [entry.code, (entry.data as { job: unknown }).job]);
        expect(jobs).toEqual([
            ['FATAL', 'a'],
            ['HOOK_FAILED', 'a'],
        ]);
    });

    it('calls onJobDone for every unfinished item, also the ones the run never fetched', async () => {
        const clock = createFakeClock();
        const controller = new AbortController();
        const calls: string[] = [];
        const logs: LogEntry[] = [];
        const promise = runAll<string, string>(
            ['a', 'b', 'c', 'd'].map((job) => ({ job, cost: { tokens: 60 } })),
            {
                clock,
                concurrency: 2,
                maxBufferSize: 1,
                buckets: [createTPM(100)],
                signal: controller.signal,
                processJob: async (job) => {
                    await new Promise<void>((resolve) => clock.setTimeout(resolve, 500));
                    return { ok: true, data: job };
                },
                onJobDone: (job, outcome, info) => {
                    calls.push(`${job} ${outcome.status} ${info.attempt} ${info.cost.tokens}`);
                    if (job === 'd') {
                        throw new Error('hook');
                    }
                },
                log: (entry) => logs.push(entry),
            },
        );
        await clock.advance(1_000);
        controller.abort();
        const result = await clock.settle(promise);
        expect(result.ok && result.data.map((item) => item.status)).toEqual([
            'done',
            'released',
            'released',
            'released',
        ]);
        expect(calls).toEqual(['a done 1 60', 'b released 0 60', 'c released 0 60', 'd released 0 60']);
        const hookFailed = logs.filter((entry) => entry.code === 'HOOK_FAILED');
        expect(hookFailed.map((entry) => (entry.data as { job: unknown }).job)).toEqual(['d']);
    });
});

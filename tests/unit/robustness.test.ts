// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    type Clock,
    createBucket,
    createConcurrencyBucket,
    createLimiter,
    createRPM,
    type FetchedJob,
    type JobResult,
    type LimiterOptions,
    runAll,
} from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

const MAX_TIMER_MS = 2_147_483_647;

/** In-memory source yielding jobs sequentially then null. */
function source(count: number) {
    let next = 1;
    let fetches = 0;
    return {
        get fetches() {
            return fetches;
        },
        fetchJob(): FetchedJob<number> | null {
            fetches++;
            return next <= count ? { job: next++ } : null;
        },
    };
}

function sleep(clock: Clock, ms: number) {
    return new Promise<void>((resolve) => clock.setTimeout(resolve, ms));
}

/** Fake clock tracking requested delay parameters. */
function recordingClock(): { clock: Clock; fake: FakeClock; delays: number[] } {
    const fake = createFakeClock();
    const delays: number[] = [];
    const clock: Clock = {
        now: () => fake.now(),
        setTimeout(callback, ms) {
            delays.push(ms);
            return fake.setTimeout(callback, ms);
        },
        clearTimeout: (timer) => fake.clearTimeout(timer),
    };
    return { clock, fake, delays };
}

describe('stop() calls', () => {
    it('void stop() from inside a callback stops the run without waiting for it', async () => {
        const clock = createFakeClock();
        const jobs = source(5);
        const statuses: [number, string][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            fetchJob: jobs.fetchJob,
            processJob: async (job) => {
                await sleep(clock, 1_000);
                return { ok: true, data: job };
            },
            onJobDone: (job, result) => {
                statuses.push([job, result.status]);
                if (job === 1) {
                    void limiter.stop();
                }
            },
        });
        const result = await clock.settle(limiter.start());
        // Slot frees before onJobDone: job 2 is in-flight when stop() is invoked.
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'stop' } });
        expect(statuses).toEqual([
            [1, 'done'],
            [2, 'done'],
        ]);
        expect(jobs.fetches).toBe(2);
        expect(limiter.isIdle()).toBe(true);
    });

    it('a callback of another limiter waits for the full run of the limiter it stops', async () => {
        const clock = createFakeClock();
        const target = createLimiter<number, number>({
            clock,
            fetchJob: source(3).fetchJob,
            processJob: async (job) => {
                await sleep(clock, 5_000);
                return { ok: true, data: job };
            },
        });
        let stopResult: unknown;
        const other = createLimiter<number, number>({
            clock,
            fetchJob: source(1).fetchJob,
            processJob: (job) => ({ ok: true, data: job }),
            onJobDone: async () => {
                stopResult = await target.stop();
            },
        });
        const targetRun = target.start();
        await clock.advance(1_000);
        await clock.settle(other.start());
        const targetResult = await clock.settle(targetRun);
        expect(targetResult).toEqual({
            ok: true,
            data: { done: 3, failed: 0, released: 0, stoppedBy: 'stop' },
        });
        expect(stopResult).toEqual(targetResult);
    });

    it('resolves every stop() call with the same result, and stop() without a run right away', async () => {
        const clock = createFakeClock();
        const limiter = createLimiter<number, number>({
            clock,
            fetchJob: source(3).fetchJob,
            processJob: async () => {
                await sleep(clock, 1_000);
                return { ok: true, data: 1 };
            },
        });
        expect(await limiter.stop()).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 0, stoppedBy: 'stop' },
        });
        const run = limiter.start();
        const first = limiter.stop();
        const second = limiter.stop({ abort: true });
        const result = await clock.settle(run);
        expect(await first).toEqual(result);
        expect(await second).toEqual(result);
    });
});

describe('timers longer than setTimeout allows', () => {
    it('re-arms a clamped timer until a retry_backoff wait above 2^31 - 1 ms has passed', async () => {
        const { clock, fake, delays } = recordingClock();
        const starts: number[] = [];
        const limiter = createLimiter<number, string>({
            clock,
            maxWaitMs: Number.POSITIVE_INFINITY,
            fetchJob: source(1).fetchJob,
            processJob: (): JobResult<string> => {
                starts.push(clock.now());
                return starts.length === 1
                    ? { ok: false, error: { type: 'retry_backoff', message: 'later', retryAfterMs: 3_000_000_000 } }
                    : { ok: true, data: 'ok' };
            },
        });
        const result = await fake.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(1);
        expect(starts).toEqual([0, 3_000_000_000]);
        expect(Math.max(...delays)).toBeLessThanOrEqual(MAX_TIMER_MS);
    });

    it('re-arms a clamped lock timer until a backoff_global wait above 2^31 - 1 ms has passed', async () => {
        const { clock, fake, delays } = recordingClock();
        const starts: number[] = [];
        const limiter = createLimiter<number, string>({
            clock,
            concurrency: 1,
            maxWaitMs: Number.POSITIVE_INFINITY,
            fetchJob: source(2).fetchJob,
            processJob: (job): JobResult<string> => {
                starts.push(clock.now());
                return starts.length === 1
                    ? { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 3_000_000_000 } }
                    : { ok: true, data: `r${job}` };
            },
        });
        const result = await fake.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(2);
        expect(starts).toEqual([0, 3_000_000_000, 3_000_000_000]);
        expect(Math.max(...delays)).toBeLessThanOrEqual(MAX_TIMER_MS);
    });

    it('rejects a timeoutMs that setTimeout would fire after 1 ms', async () => {
        const result = await createLimiter({
            timeoutMs: 3_000_000_000,
            fetchJob: () => null,
            processJob: () => ({ ok: true, data: 1 }),
        }).start();
        expect(!result.ok && result.error).toEqual({
            code: 'INVALID_CONFIG',
            message: expect.any(String),
            cause: 'timeoutMs must be a positive number <= 2147483647, or 0, false or Infinity to disable it',
        });
    });
});

describe('limiters sharing buckets', () => {
    it('keeps the global lock per limiter: another limiter on the same buckets keeps running', async () => {
        const clock = createFakeClock();
        const buckets = [createRPM(100), createConcurrencyBucket(2)];
        const startsA: [number, number][] = [];
        const startsB: [number, number][] = [];
        const a = createLimiter<number, number>({
            clock,
            buckets,
            concurrency: 1,
            fetchJob: source(2).fetchJob,
            processJob: (job): JobResult<number> => {
                startsA.push([job, clock.now()]);
                return startsA.length === 1
                    ? { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 10_000 } }
                    : { ok: true, data: job };
            },
        });
        const b = createLimiter<number, number>({
            clock,
            buckets,
            concurrency: 1,
            fetchJob: source(5).fetchJob,
            processJob: async (job) => {
                startsB.push([job, clock.now()]);
                await sleep(clock, 1_000);
                return { ok: true, data: job };
            },
        });
        const [resultA, resultB] = await clock.settle(Promise.all([a.start(), b.start()]));
        expect(resultA.ok && resultA.data.done).toBe(2);
        expect(resultB.ok && resultB.data.done).toBe(5);
        expect(startsA).toEqual([
            [1, 0],
            [1, 10_000],
            [2, 10_000],
        ]);
        expect(startsB.map(([, at]) => at)).toEqual([0, 1_000, 2_000, 3_000, 4_000]);
    });
});

describe('wake() around a run that stops by itself', () => {
    it('ignores wake() while the run is stopping and after it ended with max_wait_exceeded; start() resumes', async () => {
        const clock = createFakeClock();
        const jobs = source(3);
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            buckets: [createRPM(1)],
            maxWaitMs: 1_000,
            fetchJob: jobs.fetchJob,
            processJob: async (job) => {
                await sleep(clock, 5_000);
                return { ok: true, data: job };
            },
        });
        const run = limiter.start();
        // Job 2 wait (60s) exceeds maxWaitMs (1s), stopping run.
        await clock.advance(0);
        expect(jobs.fetches).toBe(1);
        limiter.wake();
        await clock.advance(0);
        expect(jobs.fetches).toBe(1);
        const result = await clock.settle(run);
        expect(result).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'max_wait_exceeded' },
        });
        expect(limiter.isIdle()).toBe(true);

        // wake() does not re-lease jobs after max_wait_exceeded.
        await clock.advance(60_000);
        expect(limiter.wake()).toEqual({
            state: 'idle',
            running: 0,
            buffered: 0,
            lockUntil: 0,
            lastStoppedBy: 'max_wait_exceeded',
        });
        expect(jobs.fetches).toBe(1);
        // Explicit start() resumes processing.
        const resumed = limiter.start();
        expect(jobs.fetches).toBe(2);
        expect(limiter.wake().state).toBe('running');
        await clock.settle(limiter.stop());
        await clock.settle(resumed);
    });
});

describe('runAll when the run fails', () => {
    it('returns the run error with the outcomes of the finished items and the others released', async () => {
        const clock = createFakeClock();
        let checks = 0;
        const failing = createBucket({
            getWaitMs: () => {
                checks++;
                if (checks > 2) {
                    throw new Error('bucket broke');
                }
                return 0;
            },
        });
        const finished: number[] = [];
        const options: Omit<LimiterOptions<number, number>, 'fetchJob' | 'onJobDone'> = {
            clock,
            concurrency: 1,
            buckets: [failing],
            processJob: (job) => ({ ok: true, data: job }),
            onAttempt: (job) => {
                finished.push(job);
            },
        };
        const result = await clock.settle(runAll([{ job: 1 }, { job: 2 }, { job: 3 }], options));
        expect(finished).toEqual([1]);
        expect(result).toMatchObject({
            ok: false,
            error: { code: 'BUCKET_FAILED' },
            data: [
                { ok: true, status: 'done', data: 1 },
                { ok: false, status: 'released', error: { code: 'STOPPED' } },
                { ok: false, status: 'released', error: { code: 'STOPPED' } },
            ],
        });
    });
});

describe('stale stragglers with a real-world clock', () => {
    it('requeues a backoff_global straggler after unlock without re-locking or stopping the run', async () => {
        // Non-zero base timestamp ensures straggler is not compared against lockedSince=0.
        const clock = createFakeClock(1_700_000_000_000);
        const t0 = clock.now();
        const starts: [number, number][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            fetchJob: source(3).fetchJob,
            processJob: async (job): Promise<JobResult<number>> => {
                starts.push([job, clock.now() - t0]);
                const attempt = starts.filter(([id]) => id === job).length;
                if (job === 1 && attempt === 1) {
                    await sleep(clock, 100);
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
                }
                if (job === 2 && attempt === 1) {
                    // Attempt started before lock fails after unlock: stale straggler.
                    await sleep(clock, 5_000);
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 60_000 } };
                }
                await sleep(clock, 100);
                return { ok: true, data: job };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(starts).toEqual([
            [1, 0],
            [2, 0],
            [1, 1_100],
            [3, 1_200],
            [2, 5_000],
        ]);
    });
});

describe('a clock whose clearTimeout does nothing', () => {
    it('ignores timers that fire after the run has ended', async () => {
        const fake = createFakeClock();
        const clock: Clock = { now: () => fake.now(), setTimeout: fake.setTimeout, clearTimeout: () => {} };
        const fetches: number[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            // Job 2 timer pending when run stops; no-op clearTimeout leaves timer intact.
            buckets: [createRPM(1)],
            fetchJob: () => {
                fetches.push(fake.now());
                return { job: fetches.length };
            },
            processJob: (job) => ({ ok: true, data: job }),
        });
        const run = limiter.start();
        await fake.advance(1_000);
        const result = await fake.settle(limiter.stop());
        expect(result.ok && result.data.done).toBe(1);
        await fake.advance(120_000);
        expect(fetches).toEqual([0]);
        expect(limiter.isIdle()).toBe(true);
        expect(await run).toEqual(result);
    });
});

describe('exact limits', () => {
    it.each([0, 1, 3])('maxRetries: %p allows exactly maxRetries + 1 attempts', async (maxRetries) => {
        const clock = createFakeClock();
        let attempts = 0;
        const codes: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            maxRetries,
            fetchJob: source(1).fetchJob,
            processJob: (): JobResult<number> => {
                attempts++;
                return { ok: false, error: { type: attempts % 2 ? 'retry_immediate' : 'retry_backoff', message: 'x' } };
            },
            onJobDone: (_job, result) => {
                codes.push(result.ok ? 'done' : result.error.code);
            },
        });
        await clock.settle(limiter.start());
        expect(attempts).toBe(maxRetries + 1);
        expect(codes).toEqual(['RETRIES_EXHAUSTED']);
    });

    it('a concurrent backoff_global with a longer retryAfterMs pushes the probe back', async () => {
        const clock = createFakeClock();
        const starts: [number, number][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            fetchJob: source(2).fetchJob,
            processJob: async (job): Promise<JobResult<number>> => {
                starts.push([job, clock.now()]);
                if (starts.length <= 2) {
                    // Job 1 locks for 1s; concurrent job 2 fails later requesting 5s.
                    await sleep(clock, job * 100);
                    const retryAfterMs = job === 1 ? 1_000 : 5_000;
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs } };
                }
                return { ok: true, data: job };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(2);
        expect(starts).toEqual([
            [1, 0],
            [2, 0],
            [1, 5_200],
            [2, 5_200],
        ]);
    });

    it('gives every run its own maxWaitMs budget for a lock that is still on', async () => {
        const clock = createFakeClock();
        let failUntil = 16_000;
        const starts: number[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            maxWaitMs: 10_000,
            fetchJob: () => ({ job: 1 }),
            processJob: (): JobResult<number> => {
                starts.push(clock.now());
                if (clock.now() < failUntil) {
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 4_000 } };
                }
                failUntil = Number.POSITIVE_INFINITY;
                void limiter.stop();
                return { ok: true, data: 1 };
            },
        });
        // Probes at t=4s and t=8s fail; next 4s probe exceeds 10s budget.
        const first = await clock.settle(limiter.start());
        expect(first.ok && first.data.stoppedBy).toBe('max_wait_exceeded');
        expect(starts).toEqual([0, 4_000, 8_000]);
        // New run receives fresh budget from start; probe at t=16s succeeds.
        const second = await clock.settle(limiter.start());
        expect(second.ok && second.data.done).toBe(1);
        expect(starts).toEqual([0, 4_000, 8_000, 12_000, 16_000]);
    });
});

describe('no stall while locked', () => {
    it('wakes at the earliest retryAt when the probe candidate can never fit and the buffer is full', async () => {
        const clock = createFakeClock();
        let isClosed = false;
        // Job 2 cost exceeds limit post-attempt, disqualifying it as probe candidate.
        const closing = createBucket({
            getWaitMs: (cost) => (isClosed && cost.special ? Number.POSITIVE_INFINITY : 0),
        });
        const starts: [number, number][] = [];
        const outcomes: [number, string][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            buckets: [closing],
            fetchJob: (() => {
                let next = 1;
                return () => (next <= 3 ? { job: next, cost: next++ === 2 ? { special: 1 } : {} } : null);
            })(),
            processJob: async (job): Promise<JobResult<number>> => {
                starts.push([job, clock.now()]);
                if (starts.filter(([id]) => id === job).length > 1) {
                    return { ok: true, data: job };
                }
                await sleep(clock, job === 1 ? 100 : 200);
                if (job === 2) {
                    isClosed = true;
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
                }
                return { ok: false, error: { type: 'retry_backoff', message: 'busy', retryAfterMs: 10_000 } };
            },
            onJobDone: (job, result) => {
                outcomes.push([job, result.ok ? 'done' : result.error.code]);
            },
        });
        // Jobs 1 and 3 sleep while job 2 locks until t=1200.
        // At t=1200 buffer is saturated with sleeping jobs; retry timer must wake scheduling loop.
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 1, released: 0, stoppedBy: 'empty' } });
        expect(outcomes).toEqual([
            [2, 'COST_EXCEEDS_LIMIT'],
            [1, 'done'],
            [3, 'done'],
        ]);
        expect(starts).toEqual([
            [1, 0],
            [2, 0],
            [3, 100],
            [1, 10_100],
            [3, 10_300],
        ]);
    });
});

/** Executes fn and collects unhandled rejections across macrotask turn. */
async function collectUnhandledRejections(fn: () => Promise<unknown>): Promise<unknown[]> {
    const seen: unknown[] = [];
    function onRejection(reason: unknown): void {
        seen.push(reason);
    }
    process.on('unhandledRejection', onRejection);
    try {
        await fn();
        await new Promise((resolve) => setTimeout(resolve, 10));
    } finally {
        process.off('unhandledRejection', onRejection);
    }
    return seen;
}

describe('async callbacks that reject', () => {
    it('an async log that rejects causes no unhandled rejection and does not affect the run', async () => {
        let calls = 0;
        const clock = createFakeClock();
        const limiter = createLimiter<number, string>({
            clock,
            fetchJob: source(2).fetchJob,
            processJob: (job) => ({ ok: true, data: `result ${job}` }),
            log: async () => {
                calls++;
                throw new Error('log sink down');
            },
        });
        let result: unknown;
        const unhandled = await collectUnhandledRejections(async () => {
            result = await clock.settle(limiter.start());
        });
        expect(unhandled).toEqual([]);
        expect(calls).toBeGreaterThan(0);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
    });

    it('an async settle or unsubscribe that rejects is logged as BUCKET_FAILED, not unhandled', async () => {
        const failures: [string, string][] = [];
        const settleRejects = createBucket({
            settle: async () => {
                throw new Error('settle: redis down');
            },
        });
        const unsubscribeRejects = createBucket({
            subscribe: () => async () => {
                throw new Error('unsubscribe: redis down');
            },
        });
        const clock = createFakeClock();
        const limiter = createLimiter<number, string>({
            clock,
            buckets: [settleRejects, unsubscribeRejects],
            fetchJob: source(2).fetchJob,
            processJob: (job) => ({ ok: true, data: `result ${job}` }),
            log: (entry) => {
                if (entry.code === 'BUCKET_FAILED') {
                    failures.push([(entry.data as Error).message, entry.message]);
                }
            },
        });
        let result: unknown;
        const unhandled = await collectUnhandledRejections(async () => {
            result = await clock.settle(limiter.start());
        });
        expect(unhandled).toEqual([]);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(failures).toEqual([
            ['settle: redis down', "A bucket's settle threw or rejected; the run continues."],
            ['settle: redis down', "A bucket's settle threw or rejected; the run continues."],
            ['unsubscribe: redis down', "A bucket's unsubscribe threw or rejected; the run continues."],
        ]);
    });

    it('an async subscribe that rejects fails the run with BUCKET_FAILED, not unhandled', async () => {
        const subscribeRejects = createBucket({
            subscribe: (async () => {
                throw new Error('subscribe: redis down');
            }) as never,
        });
        const clock = createFakeClock();
        const limiter = createLimiter<number, string>({
            clock,
            buckets: [subscribeRejects],
            fetchJob: source(2).fetchJob,
            processJob: (job) => ({ ok: true, data: `result ${job}` }),
        });
        let result: unknown;
        const unhandled = await collectUnhandledRejections(async () => {
            result = await clock.settle(limiter.start());
        });
        expect(unhandled).toEqual([]);
        expect(result).toMatchObject({ ok: false, error: { code: 'BUCKET_FAILED' } });
    });

    it('a listener of a concurrency bucket that rejects causes no unhandled rejection', async () => {
        const bucket = createConcurrencyBucket(1);
        bucket.subscribe?.(async () => {
            throw new Error('listener down');
        });
        const unhandled = await collectUnhandledRejections(async () => {
            bucket.settle(bucket.reserve({ requests: 1 }, 0), {}, 0);
        });
        expect(unhandled).toEqual([]);
    });
});

describe('fetched entries that throw while read', () => {
    function hostileEntries(): [string, unknown][] {
        const revoked = Proxy.revocable({ job: 1 }, {});
        revoked.revoke();
        const getters = {
            job: 1,
            get cost(): never {
                throw new Error('lazy getter failed');
            },
        };
        return [
            ['a cost getter that throws', getters],
            ['a revoked Proxy as entry', revoked.proxy],
        ];
    }

    for (const [name, entry] of hostileEntries()) {
        it(`fails the run with FETCH_FAILED for ${name}, without an unhandled rejection or a hang`, async () => {
            const clock = createFakeClock();
            let fetches = 0;
            const limiter = createLimiter<number, string>({
                clock,
                fetchJob: () => (fetches++ === 0 ? (entry as FetchedJob<number>) : null),
                processJob: (job) => ({ ok: true, data: `result ${job}` }),
            });
            let result: unknown;
            const unhandled = await collectUnhandledRejections(async () => {
                result = await clock.settle(limiter.start());
            });
            expect(unhandled).toEqual([]);
            expect(result).toMatchObject({ ok: false, error: { code: 'FETCH_FAILED' } });
            expect(limiter.isIdle()).toBe(true);
        });
    }
});

// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    type Clock,
    createBucket,
    createConcurrencyBucket,
    createLimiter,
    createQueue,
    createRPM,
    createSlidingWindow,
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

/** Source yielding one entry then null. */
function once(entry: FetchedJob<number>) {
    let isFetched = false;
    return (): FetchedJob<number> | null => {
        if (isFetched) {
            return null;
        }
        isFetched = true;
        return entry;
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

describe('attempts that run longer than a sliding window', () => {
    it.each([
        ['no wake()', undefined],
        ['wake() after the window passed', 1_500],
        ['wake() shortly before the attempts settle', 4_900],
    ])('starts the next job one window after the attempts settle: %s', async (_name, wakeAt) => {
        const clock = createFakeClock();
        const starts: [number, number][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 3,
            buckets: [createSlidingWindow({ requests: 2, perMs: 1_000 })],
            fetchJob: source(3).fetchJob,
            processJob: async (job) => {
                starts.push([job, clock.now()]);
                await sleep(clock, 5_000);
                return { ok: true, data: job };
            },
        });
        const run = limiter.start();
        if (wakeAt !== undefined) {
            await clock.advance(wakeAt);
            limiter.wake();
        }
        const result = await clock.settle(run);
        expect(result).toEqual({ ok: true, data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' } });
        // Both requests are booked at 5,000; a start before 6,000 would put three requests into one window.
        expect(starts).toEqual([
            [1, 0],
            [2, 0],
            [3, 6_000],
        ]);
    });

    it('starts a job waiting on a shared window when the long attempt of another limiter settles below its estimate', async () => {
        const clock = createFakeClock();
        const shared = createSlidingWindow({ tokens: 100, perMs: 1_000 });
        const starts: [string, number][] = [];
        const a = createLimiter<number, number>({
            clock,
            buckets: [shared],
            fetchJob: once({ job: 1, cost: { tokens: 100 } }),
            processJob: async (job) => {
                starts.push(['a', clock.now()]);
                await sleep(clock, 30_000);
                return { ok: true, data: job, usage: { tokens: 10 } };
            },
        });
        const b = createLimiter<number, number>({
            clock,
            buckets: [shared],
            fetchJob: once({ job: 2, cost: { tokens: 90 } }),
            processJob: (job) => {
                starts.push(['b', clock.now()]);
                return { ok: true, data: job };
            },
        });
        const runs = Promise.all([a.start(), b.start()]);
        await clock.advance(1_500);
        // The reservation of a is older than the window: a re-check must not let b start.
        b.wake();
        await clock.advance(28_499);
        expect(starts).toEqual([['a', 0]]);
        expect(b.getStatus()).toMatchObject({ state: 'running', buffered: 1 });
        await clock.settle(runs);
        // a held 100 until it settled with 10: the 90 of b fit at once.
        expect(starts).toEqual([
            ['a', 0],
            ['b', 30_000],
        ]);
    });

    it('starts a waiting job when a settled booking expires while a long attempt is still in flight', async () => {
        const clock = createFakeClock();
        const starts: [string, number][] = [];
        const entries = [
            { job: 'long', cost: { tokens: 60 } },
            { job: 'short', cost: { tokens: 40 } },
            { job: 'third', cost: { tokens: 40 } },
        ];
        const limiter = createLimiter<string, string>({
            clock,
            concurrency: 3,
            buckets: [createSlidingWindow({ tokens: 100, perMs: 1_000 })],
            fetchJob: () => entries.shift() ?? null,
            processJob: async (job) => {
                starts.push([job, clock.now()]);
                await sleep(clock, job === 'long' ? 10_000 : 100);
                return { ok: true, data: job };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' } });
        // 'short' is booked at 100 and leaves the window at 1,100; no settle happens until 10,000.
        expect(starts).toEqual([
            ['long', 0],
            ['short', 0],
            ['third', 1_100],
        ]);
    });

    it('does not let a hung attempt of another limiter block capacity that expired bookings free', async () => {
        const clock = createFakeClock();
        const shared = createSlidingWindow({ tokens: 100, perMs: 1_000 });
        const hung = createLimiter<number, number>({
            clock,
            buckets: [shared],
            fetchJob: once({ job: 0, cost: { tokens: 60 } }),
            processJob: () => new Promise<never>(() => {}),
        });
        const starts: [number, number][] = [];
        const jobs = source(2);
        const limiter = createLimiter<number, number>({
            clock,
            buckets: [shared],
            fetchJob: () => {
                const fetched = jobs.fetchJob();
                return fetched && { ...fetched, cost: { tokens: 40 } };
            },
            processJob: (job) => {
                starts.push([job, clock.now()]);
                return { ok: true, data: job };
            },
        });
        void hung.start();
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        // 60 stay reserved by the hung attempt; the 40 of job 1 leave the window at 1,000.
        expect(starts).toEqual([
            [1, 0],
            [2, 1_000],
        ]);
    });

    it('does not count the re-check delay toward maxWaitMs: an in-flight attempt may free capacity earlier', async () => {
        const clock = createFakeClock();
        const starts: [string, number][] = [];
        const entries = [
            { job: 'settled', cost: { tokens: 40 } },
            { job: 'in flight', cost: { tokens: 60 } },
            { job: 'waiting', cost: { tokens: 40 } },
        ];
        const limiter = createLimiter<string, string>({
            clock,
            concurrency: 3,
            maxWaitMs: 1_000,
            // Settled bookings expire after a day, far beyond maxWaitMs.
            buckets: [createSlidingWindow({ tokens: 100, perMs: 86_400_000 })],
            fetchJob: () => entries.shift() ?? null,
            processJob: async (job) => {
                starts.push([job, clock.now()]);
                if (job !== 'in flight') {
                    return { ok: true, data: job };
                }
                await sleep(clock, 5_000);
                return { ok: true, data: job, usage: { tokens: 10 } };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(starts).toEqual([
            ['settled', 0],
            ['in flight', 0],
            ['waiting', 5_000],
        ]);
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

describe('a clock that fails', () => {
    /** Fake clock whose methods can be broken one by one while a run is active. */
    function breakableClock() {
        const fake = createFakeClock();
        const broken: { now?: () => number; setTimeout?: boolean; clearTimeout?: boolean } = {};
        const clock: Clock = {
            now: () => (broken.now ? broken.now() : fake.now()),
            setTimeout(callback, ms) {
                if (broken.setTimeout) {
                    throw new Error('setTimeout broke');
                }
                return fake.setTimeout(callback, ms);
            },
            clearTimeout(timer) {
                if (broken.clearTimeout) {
                    throw new Error('clearTimeout broke');
                }
                fake.clearTimeout(timer);
            },
        };
        return { fake, broken, clock };
    }

    function throwingNow(): number {
        throw new Error('now broke');
    }

    it.each([
        ['throws', throwingNow, new Error('now broke')],
        ['returns NaN', () => Number.NaN, Number.NaN],
        ['returns Infinity', () => Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY],
        ['returns a non-number', () => '5' as never, '5'],
    ])('resolves start() with CLOCK_FAILED when now() %s, without fetching a job', async (_name, now, cause) => {
        const { fake, broken, clock } = breakableClock();
        broken.now = now;
        const outcomes: [number, string][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            fetchJob: source(3).fetchJob,
            processJob: (job) => ({ ok: true, data: job }),
            onJobDone: (job, outcome) => {
                outcomes.push([job, outcome.ok ? 'done' : outcome.error.code]);
            },
        });
        let result: unknown;
        const unhandled = await collectUnhandledRejections(async () => {
            result = await fake.settle(limiter.start());
        });
        expect(unhandled).toEqual([]);
        expect(result).toEqual({ ok: false, error: { code: 'CLOCK_FAILED', message: expect.any(String), cause } });
        // No job is leased with a clock that gives no time.
        expect(outcomes).toEqual([]);
        expect(limiter.getStatus()).toMatchObject({ state: 'idle', lastStoppedBy: 'error' });

        // The limiter works again once the clock does.
        broken.now = undefined;
        expect(await fake.settle(limiter.start())).toEqual({
            ok: true,
            data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' },
        });
    });

    it('reports CLOCK_FAILED, not max_wait_exceeded, when now() fails while the limiter is locked', async () => {
        const { fake, broken, clock } = breakableClock();
        let calls = 0;
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 1_000,
            concurrency: 1,
            fetchJob: source(2).fetchJob,
            processJob: (job): JobResult<number> =>
                calls++ === 0
                    ? { ok: false, error: { type: 'backoff_global', message: 'down', retryAfterMs: 5_000 } }
                    : { ok: true, data: job },
        });
        // Locks the limiter until 5,000; the lock outlasts the run, which hands back job 1.
        expect(await fake.settle(limiter.start())).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        await fake.advance(10_000);
        // The lock has passed, but the last valid time still lies inside it.
        broken.now = throwingNow;
        expect(await fake.settle(limiter.start())).toMatchObject({ ok: false, error: { code: 'CLOCK_FAILED' } });
        broken.now = undefined;
        expect(await fake.settle(limiter.start())).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'empty' },
        });
    });

    it('ends the run when now() starts to throw while attempts are in flight: attempts finish and free their bucket slots', async () => {
        const { fake, broken, clock } = breakableClock();
        const shared = createConcurrencyBucket(2);
        const window = createSlidingWindow({ tokens: 100, perMs: 1_000 });
        const outcomes: [number, string][] = [];
        const logs: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            buckets: [shared, window],
            fetchJob: () => {
                const fetched = jobs.fetchJob();
                return fetched && { ...fetched, cost: { tokens: 50 } };
            },
            processJob: async (job) => {
                await sleep(fake, 1_000);
                return { ok: true, data: job };
            },
            onJobDone: (job, outcome) => {
                outcomes.push([job, outcome.ok ? 'done' : outcome.error.code]);
            },
            log: (entry) => {
                if (entry.code === 'CLOCK_FAILED') {
                    logs.push(entry.code);
                }
            },
        });
        const jobs = source(4);
        let result: unknown;
        const unhandled = await collectUnhandledRejections(async () => {
            const run = limiter.start();
            await fake.advance(500);
            expect(limiter.getStatus()).toMatchObject({ running: 2 });
            broken.now = throwingNow;
            result = await fake.settle(run);
        });
        expect(unhandled).toEqual([]);
        expect(result).toMatchObject({ ok: false, error: { code: 'CLOCK_FAILED', cause: new Error('now broke') } });
        // Both running attempts finish; jobs 3 and 4 are never fetched.
        expect(outcomes).toEqual([
            [1, 'done'],
            [2, 'done'],
        ]);
        expect(jobs.fetches).toBe(2);
        // Reported once, although every later clock read fails as well.
        expect(logs).toEqual(['CLOCK_FAILED']);
        expect(limiter.isIdle()).toBe(true);
        // No leaked reservation: another limiter can use both slots and the settled window.
        expect(shared.getWaitMs({ requests: 1 }, 0)).toBe(0);
        const second = shared.reserve({ requests: 1 }, 0);
        expect(shared.getWaitMs({ requests: 1 }, 0)).toBe(0);
        shared.settle(second, {}, 0);
        expect(window.getWaitMs({ tokens: 1 }, 10_000)).toBe(0);
    });

    it('fails the run instead of hanging when setTimeout throws for a wake-up timer', async () => {
        const { fake, broken, clock } = breakableClock();
        broken.setTimeout = true;
        const started: number[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            // Job 2 needs a timer for the window wait.
            buckets: [createRPM(1)],
            fetchJob: source(2).fetchJob,
            processJob: (job) => {
                started.push(job);
                return { ok: true, data: job };
            },
        });
        const result = await fake.settle(limiter.start());
        expect(result).toMatchObject({
            ok: false,
            error: { code: 'CLOCK_FAILED', cause: new Error('setTimeout broke') },
        });
        expect(started).toEqual([1]);
        expect(limiter.isIdle()).toBe(true);
    });

    it('finishes the run when only clearTimeout throws', async () => {
        const { fake, broken, clock } = breakableClock();
        broken.clearTimeout = true;
        const limiter = createLimiter<number, number>({
            clock,
            buckets: [createSlidingWindow({ requests: 1, perMs: 1_000 })],
            timeoutMs: 5_000,
            fetchJob: source(3).fetchJob,
            processJob: async (job) => {
                await sleep(fake, 100);
                return { ok: true, data: job };
            },
        });
        expect(await fake.settle(limiter.start())).toEqual({
            ok: true,
            data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' },
        });
    });

    it('resolves every push of a queue with CLOCK_FAILED instead of rejecting or hanging', async () => {
        const { fake, broken, clock } = breakableClock();
        broken.now = throwingNow;
        const queue = createQueue<number, number>({
            clock,
            concurrency: 1,
            processJob: (job) => ({ ok: true, data: job }),
        });
        const outcomes = await fake.settle(Promise.all([queue.push(1), queue.push(2)]));
        expect(outcomes.map((outcome) => [outcome.status, !outcome.ok && outcome.error.code])).toEqual([
            ['released', 'CLOCK_FAILED'],
            ['released', 'CLOCK_FAILED'],
        ]);
        broken.now = undefined;
        expect(await fake.settle(queue.push(3))).toEqual({ ok: true, status: 'done', data: 3 });
    });
});

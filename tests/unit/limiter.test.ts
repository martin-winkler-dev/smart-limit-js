// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    type AttemptInfo,
    createBucket,
    createConcurrencyBucket,
    createLimiter,
    createRPM,
    createSlidingWindow,
    createTPD,
    createTPM,
    type FetchedJob,
    type JobOutcome,
    type JobResult,
    type LimiterOptions,
    type LimiterStatus,
    type LogEntry,
    WAIT_FOR_CHANGE,
} from '../../src/index.js';
import { readLockState } from '../../src/limiter.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

type Job = { id: number; tokens?: number };

/** In-memory job source yielding jobs sequentially. */
function source(count: number) {
    const open: Job[] = Array.from({ length: count }, (_, i) => ({ id: i + 1 }));
    let fetches = 0;
    return {
        open,
        get fetches() {
            return fetches;
        },
        fetchJob(): FetchedJob<Job> | null {
            fetches++;
            const job = open.shift();
            return job ? { job } : null;
        },
    };
}

/** Creates fetchJob yielding array entries sequentially then null. */
function fetchFrom(entries: FetchedJob<Job>[]) {
    const queue = [...entries];
    return () => queue.shift() ?? null;
}

function sleep(clock: FakeClock, ms: number) {
    return new Promise<void>((resolve) => clock.setTimeout(resolve, ms));
}

function setup(overrides: Partial<LimiterOptions<Job, string>> & { jobs?: number } = {}) {
    const clock = createFakeClock();
    const jobs = source(overrides.jobs ?? 5);
    const started: [number, number][] = [];
    const done: [number, unknown][] = [];
    const outcomes: [number, JobOutcome<string>][] = [];
    const released: number[] = [];
    const logs: LogEntry[] = [];
    const limiter = createLimiter<Job, string>({
        clock,
        fetchJob: jobs.fetchJob,
        processJob: async (job) => {
            started.push([job.id, clock.now()]);
            await sleep(clock, 1_000);
            return { ok: true, data: `result ${job.id}` };
        },
        onJobDone: (job, result) => {
            outcomes.push([job.id, result]);
            if (result.status === 'released') {
                released.push(job.id);
            } else {
                done.push([job.id, result.ok ? result.data : result.error.code]);
            }
        },
        log: (entry) => logs.push(entry),
        ...overrides,
    });
    return { clock, jobs, started, done, outcomes, released, logs, limiter };
}

describe('createLimiter', () => {
    it('runs all jobs and reports a summary', async () => {
        const { clock, limiter, done, logs } = setup();
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 5, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(done.map(([id]) => id).sort()).toEqual([1, 2, 3, 4, 5]);
        expect(logs.at(-1)?.message).toContain('5 done');
    });

    it('never runs more jobs at once than concurrency, fetching one by one only when slots and buckets allow', async () => {
        let running = 0;
        let peak = 0;
        const { clock, limiter, jobs } = setup({
            jobs: 10,
            concurrency: 3,
            processJob: async () => {
                running++;
                peak = Math.max(peak, running);
                await sleep(clock, 1_000);
                running--;
                return { ok: true, data: '' };
            },
        });
        await clock.settle(limiter.start());
        expect(peak).toBe(3);
        expect(jobs.fetches).toBe(11);
    });

    it('checks request-level buckets before calling fetchJob (pre-fetch check)', async () => {
        const { clock, limiter, started, jobs } = setup({ concurrency: 10, buckets: [createRPM(2)] });
        const promise = limiter.start();
        await clock.advance(0);
        // RPM(2) blocks third pre-fetch at t=0.
        expect(jobs.fetches).toBe(2);
        expect(jobs.open).toHaveLength(3);
        await clock.settle(promise);
        // 1s execution: request expires from 60s window at t=61s.
        expect(started.map(([, at]) => at)).toEqual([0, 0, 61_000, 61_000, 122_000]);
    });

    it('paces evenly with a small window', async () => {
        const { clock, limiter, started } = setup({
            jobs: 3,
            buckets: [createSlidingWindow({ requests: 1, perMs: 3000 })],
        });
        await clock.settle(limiter.start());
        expect(started.map(([, at]) => at)).toEqual([0, 4_000, 8_000]);
    });

    it('books the cost from fetchJob, settles to the actual usage, and passes actualCost in AttemptInfo', async () => {
        const attemptCosts: unknown[] = [];
        const doneCosts: unknown[] = [];
        const { clock, limiter, started } = setup({
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 100 } },
                { job: { id: 2 }, cost: { tokens: 85 } },
            ]),
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                return { ok: true, data: '', usage: { tokens: 15 } };
            },
            onAttempt: (_job, _result, info) => {
                attemptCosts.push(info.cost);
            },
            onJobDone: (_job, _result, info) => {
                doneCosts.push(info.cost);
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, 0],
        ]);
        expect(attemptCosts).toEqual([
            { requests: 1, tokens: 15 },
            { requests: 1, tokens: 15 },
        ]);
        expect(doneCosts).toEqual([
            { requests: 1, tokens: 15 },
            { requests: 1, tokens: 15 },
        ]);
    });

    it.each([
        // Booking settles at attempt completion (t=1000), releasing at t=61000.
        ['keeps the booking of an attempt cut by timeoutMs', true, undefined, 61_000],
        ['books the usage an attempt cut by timeoutMs reports', true, { tokens: 10 }, 1_000],
        ['keeps the booking of a failure without usage', false, undefined, 60_000],
        ['gives the tokens back when a failure reports usage 0', false, { tokens: 0 }, 0],
    ] as const)('%s', async (_label, waitsForTimeout, usage, secondStartsAt) => {
        const { clock, limiter, started } = setup({
            concurrency: 2,
            maxRetries: 0,
            timeoutMs: 1_000,
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 60 } },
                { job: { id: 2 }, cost: { tokens: 60 } },
            ]),
            processJob: async (job, { signal }) => {
                started.push([job.id, clock.now()]);
                if (job.id === 2) {
                    return { ok: true, data: 'result 2' };
                }
                if (waitsForTimeout) {
                    await new Promise((resolve) => signal.addEventListener('abort', resolve));
                }
                const error = { type: 'retry_backoff', message: 'cut' } as const;
                return { ok: false, error: usage ? { ...error, usage } : error };
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, secondStartsAt],
        ]);
    });

    it('keeps the booking of an attempt cut by stop({ abort: true })', async () => {
        const tpm = createTPM(100);
        const { clock, limiter } = setup({
            jobs: 1,
            buckets: [tpm],
            fetchJob: fetchFrom([{ job: { id: 1 }, cost: { tokens: 60 } }]),
            processJob: async (_job, { signal }) => {
                await new Promise((resolve) => signal.addEventListener('abort', resolve));
                return { ok: false, error: { type: 'retry_immediate', message: 'aborted' } };
            },
        });
        const run = limiter.start();
        await clock.advance(500);
        await clock.settle(limiter.stop({ abort: true }));
        await run;
        // Settled at t=500: capacity frees one window later at t=60500.
        expect(tpm.getWaitMs({ tokens: 60 }, clock.now())).toBe(60_000);
    });

    it.each([
        ['tokens count from the end of the attempt', createTPM(100), { tokens: 100 }, 110_000],
        ['requests count from the end of the attempt', createRPM(1), {}, 110_000],
    ] as const)('%s, also when the estimate was exact', async (_label, bucket, cost, secondStartsAt) => {
        const { clock, limiter, started } = setup({
            concurrency: 1,
            buckets: [bucket],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost },
                { job: { id: 2 }, cost },
            ]),
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                await sleep(clock, 50_000);
                return { ok: true, data: `result ${job.id}`, usage: cost };
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, secondStartsAt],
        ]);
    });

    it('counts the usage of an attempt longer than the window when it finishes', async () => {
        const { clock, limiter, started } = setup({
            concurrency: 1,
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 10 } },
                { job: { id: 2 }, cost: { tokens: 50 } },
            ]),
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                if (job.id === 1) {
                    await sleep(clock, 70_000);
                    return { ok: true, data: '', usage: { tokens: 90 } };
                }
                return { ok: true, data: '' };
            },
        });
        await clock.settle(limiter.start());
        // Job 1 booked 10 at t=0 and 80 at t=70s; job 2 waits until t=130s.
        expect(started).toEqual([
            [1, 0],
            [2, 130_000],
        ]);
    });

    it('shares a bucket between limiters', async () => {
        const shared = createRPM(2);
        const a = setup({ jobs: 2, buckets: [shared] });
        const b = setup({ jobs: 1, buckets: [shared] });
        const clock = a.clock;
        const limiterB = createLimiter<Job, string>({
            clock,
            buckets: [shared],
            fetchJob: b.jobs.fetchJob,
            processJob: async (job) => {
                b.started.push([job.id, clock.now()]);
                return { ok: true, data: '' };
            },
        });
        await clock.settle(Promise.all([a.limiter.start(), limiterB.start()]));
        const allStarts = [...a.started.map(([, at]) => at), ...b.started.map(([, at]) => at)].sort((x, y) => x - y);
        expect(allStarts).toEqual([0, 0, 60_000]);
    });

    it('wakes a limiter waiting on a shared window when another limiter settles below its estimate (Gemini4 20.1)', async () => {
        const clock = createFakeClock();
        const shared = createTPM(100);
        const startedB: number[] = [];
        const limiterA = createLimiter<Job, string>({
            clock,
            buckets: [shared],
            fetchJob: fetchFrom([{ job: { id: 1 }, cost: { tokens: 100 } }]),
            processJob: async () => {
                await sleep(clock, 1_000);
                return { ok: true, data: '', usage: { tokens: 10 } };
            },
        });
        const limiterB = createLimiter<Job, string>({
            clock,
            buckets: [shared],
            fetchJob: fetchFrom([{ job: { id: 2 }, cost: { tokens: 50 } }]),
            processJob: async () => {
                startedB.push(clock.now());
                return { ok: true, data: '' };
            },
        });
        const runA = limiterA.start();
        await clock.advance(0);
        await clock.settle(Promise.all([runA, limiterB.start()]));
        expect(startedB).toEqual([1_000]);
    });

    it('waits for an in-flight daily estimate to settle instead of stopping for maxWaitMs (Gemini4 20.2)', async () => {
        const { clock, limiter, started } = setup({
            concurrency: 2,
            buckets: [createTPD(1_000)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 900 } },
                { job: { id: 2 }, cost: { tokens: 500 } },
            ]),
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                await sleep(clock, 1_000);
                return { ok: true, data: '', usage: { tokens: 100 } };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(started).toEqual([
            [1, 0],
            [2, 1_000],
        ]);
    });

    it('retries retry_backoff errors with backoff, then succeeds', async () => {
        const attempts: number[] = [];
        let calls = 0;
        const { clock, limiter, done } = setup({
            jobs: 1,
            retryDelayMs: 1_000,
            processJob: async () => {
                calls++;
                return calls < 3
                    ? { ok: false, error: { type: 'retry_backoff', message: 'flaky' } }
                    : { ok: true, data: 'finally' };
            },
            onAttempt: (_job, _result, info) => {
                attempts.push(info.attempt);
            },
        });
        await clock.settle(limiter.start());
        expect(attempts).toEqual([1, 2, 3]);
        expect(done).toEqual([[1, 'finally']]);
        expect(clock.now()).toBeGreaterThanOrEqual(500 + 1_000);
    });

    it('retries retry_immediate errors immediately without delay', async () => {
        let calls = 0;
        const { clock, limiter, started, done } = setup({
            jobs: 1,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                calls++;
                return calls < 3
                    ? { ok: false, error: { type: 'retry_immediate', message: 'transient' } }
                    : { ok: true, data: 'ok' };
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [1, 0],
            [1, 0],
        ]);
        expect(done).toEqual([[1, 'ok']]);
    });

    it('fails a job when maxRetries are exhausted', async () => {
        const { clock, limiter, done } = setup({
            jobs: 1,
            maxRetries: 1,
            processJob: () => ({ ok: false, error: { type: 'retry_backoff', message: 'down' } }),
        });
        const result = await clock.settle(limiter.start());
        expect(done).toEqual([[1, 'RETRIES_EXHAUSTED']]);
        expect(result.ok && result.data.failed).toBe(1);
    });

    it('does not retry fatal errors and passes the cause on', async () => {
        let calls = 0;
        let cause: unknown;
        const { clock, limiter } = setup({
            jobs: 1,
            processJob: () => {
                calls++;
                return { ok: false, error: { type: 'fatal', message: 'bad input', code: 'HTTP_400' } };
            },
            onJobDone: (_job, result) => {
                cause = !result.ok && result.error.cause;
            },
        });
        await clock.settle(limiter.start());
        expect(calls).toBe(1);
        expect(cause).toMatchObject({ code: 'HTTP_400', message: 'bad input' });
    });

    it('locks globally on backoff_global, probes with the single job first, and unlocks on probe success', async () => {
        let limited = false;
        const { clock, limiter, started } = setup({
            jobs: 3,
            concurrency: 2,
            buckets: [createRPM(100)],
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                if (job.id === 1 && !limited) {
                    limited = true;
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 30_000 } };
                }
                await sleep(clock, 1_000);
                return { ok: true, data: '' };
            },
        });
        await clock.settle(limiter.start());
        // Job 1 locks until 30s; probe runs at 30s and unlocks job 3 at 31s.
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [1, 30_000],
            [3, 31_000],
        ]);
    });

    it('keeps the lock when the probe fails in any other way and probes with the next job until one passes', async () => {
        const plan: Record<number, JobResult<string>[]> = {
            1: [
                { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } },
                { ok: false, error: { type: 'fatal', message: 'bad input' } },
            ],
            2: [
                { ok: false, error: { type: 'retry_immediate', message: 'flaky' } },
                { ok: false, error: { type: 'retry_backoff', message: 'timed out', code: 'TIMEOUT' } },
                { ok: true, data: '2' },
            ],
            3: [
                { ok: false, error: { type: 'retry_immediate', message: 'flaky' } },
                { ok: true, data: '3' },
            ],
            4: [{ ok: true, data: '4' }],
            5: [{ ok: true, data: '5' }],
        };
        const { clock, limiter, started, done } = setup({
            concurrency: 3,
            retryDelayMs: 500,
            processJob: async (job, { attempt }) => {
                started.push([job.id, clock.now()]);
                await sleep(clock, job.id === 1 && attempt === 1 ? 0 : 100);
                return plan[job.id][attempt - 1];
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            // Lock from job 1 until 1000; in-flight jobs 2 and 3 are requeued.
            [1, 1_000], // probe: fatal -> still locked
            [2, 1_100], // next probe: TIMEOUT -> still locked
            [3, 1_200], // next probe: passes -> unlocked
            [4, 1_300],
            [5, 1_300],
            [2, 1_700], // its own retry_backoff wait (500 ms after 1200)
        ]);
        expect(done).toEqual([
            [1, 'FATAL'],
            [3, '3'],
            [4, '4'],
            [5, '5'],
            [2, '2'],
        ]);
    });

    it('keeps the lock when the probe hits timeoutMs while the provider still hangs', async () => {
        const lockAfterAttempt: string[] = [];
        const { clock, limiter, started, done } = setup({
            jobs: 1,
            timeoutMs: 500,
            retryDelayMs: 1_000,
            processJob: async (job, { attempt, signal }) => {
                started.push([job.id, clock.now()]);
                if (attempt === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
                }
                if (attempt === 2) {
                    // Hangs until timeout aborts signal; wrapper throws to simulate SDK behavior.
                    await new Promise((_resolve, reject) =>
                        signal.addEventListener('abort', () => reject(signal.reason)),
                    );
                }
                return { ok: true, data: 'recovered' };
            },
            onAttempt: (_job, result) => {
                lockAfterAttempt.push(
                    `${result.ok ? 'ok' : (result.error.code ?? result.error.type)}:${readLockState(limiter)?.status}`,
                );
            },
        });
        await clock.settle(limiter.start());
        expect(lockAfterAttempt).toEqual(['backoff_global:waiting', 'TIMEOUT:waiting', 'ok:unlocked']);
        expect(started.map(([, at]) => at)).toEqual([0, 1_000, 2_500]);
        expect(done).toEqual([[1, 'recovered']]);
    });

    it('stops with max_wait_exceeded once no probe passed within maxWaitMs since the lock started; the next start() probes first', async () => {
        let isDown = true;
        const { clock, limiter, started, outcomes, done } = setup({
            jobs: 3,
            concurrency: 1,
            timeoutMs: 30_000,
            maxWaitMs: 60_000,
            retryDelayMs: 1_000,
            processJob: async (job, { signal }) => {
                started.push([job.id, clock.now()]);
                if (!isDown) {
                    return { ok: true, data: `ok ${job.id}` };
                }
                if (started.length === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
                }
                // Hangs until timeout abort; each probe resolves as TIMEOUT.
                return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
            },
        });
        const result = await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [1, 1_000], // probe: TIMEOUT at 31 000, lock kept (31 s < 60 s); job 1 retries after 1 s
            [1, 32_000], // next probe: TIMEOUT at 62 000, the lock is 62 s old -> stop
        ]);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(outcomes).toEqual([
            [1, { ok: false, status: 'released', error: expect.objectContaining({ code: 'MAX_WAIT_EXCEEDED' }) }],
        ]);
        expect(clock.now()).toBe(62_000);
        expect(readLockState(limiter)?.status).toBe('waiting');

        // Recovered: subsequent start() executes job 2 as probe, unlocking limiter.
        isDown = false;
        const res2 = await clock.settle(limiter.start());
        expect(res2).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(done).toEqual([
            [2, 'ok 2'],
            [3, 'ok 3'],
        ]);
        expect(readLockState(limiter)?.status).toBe('unlocked');
    });

    it.each(['backoff_global', 'retry_backoff'] as const)(
        'treats %s with retryAfterMs: 0 as unset and backs off exponentially',
        async (type) => {
            let calls = 0;
            const { clock, limiter, done, started } = setup({
                jobs: 1,
                retryDelayMs: 2_000,
                processJob: (job) => {
                    started.push([job.id, clock.now()]);
                    calls++;
                    return calls <= 3
                        ? { ok: false, error: { type, message: 'retry now', retryAfterMs: 0 } }
                        : { ok: true, data: 'recovered' };
                },
            });
            await clock.settle(limiter.start());
            expect(started.map(([, at]) => at)).toEqual([0, 2_000, 6_000, 14_000]);
            expect(done).toEqual([[1, 'recovered']]);
        },
    );

    it('locks backoff_global for at least 1000 ms with retryDelayMs or maxRetryDelayMs 0, but uses a retryAfterMs as given', async () => {
        for (const options of [{ retryDelayMs: 0 }, { maxRetryDelayMs: 0 }]) {
            let calls = 0;
            const { clock, limiter, started } = setup({
                jobs: 1,
                ...options,
                processJob: (job) => {
                    started.push([job.id, clock.now()]);
                    calls++;
                    if (calls === 4) {
                        return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 10 } };
                    }
                    return calls < 5
                        ? { ok: false, error: { type: 'backoff_global', message: 'ECONNREFUSED' } }
                        : { ok: true, data: 'recovered' };
                },
            });
            await clock.settle(limiter.start());
            expect(started.map(([, at]) => at)).toEqual([0, 1_000, 2_000, 3_000, 3_010]);
        }
    });

    it('locks backoff_global with retryDelayMs: 0 through a timer, so other timers run between probes (real clock)', async () => {
        let calls = 0;
        let callsWhenTimerRan = -1;
        setTimeout(() => {
            callsWhenTimerRan = calls;
        }, 10);
        const limiter = createLimiter<number, string>({
            retryDelayMs: 0,
            maxWaitMs: 50,
            fetchJob: (() => {
                const jobs = [1];
                return () => (jobs.length > 0 ? { job: jobs.shift() as number } : null);
            })(),
            processJob: () => {
                calls++;
                return { ok: false, error: { type: 'backoff_global', message: 'ECONNREFUSED' } };
            },
        });
        const result = await limiter.start();
        expect(result).toMatchObject({ ok: true, data: { released: 1, stoppedBy: 'max_wait_exceeded' } });
        expect(calls).toBe(1);
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(callsWhenTimerRan).toBe(1);
    });

    it('does not count backoff_global against maxRetries and uses exponential backoff when no retryAfterMs is given', async () => {
        let calls = 0;
        const { clock, limiter, done, started } = setup({
            jobs: 1,
            maxRetries: 0,
            retryDelayMs: 2_000,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                calls++;
                return calls <= 3
                    ? { ok: false, error: { type: 'backoff_global', message: '503' } }
                    : { ok: true, data: 'recovered' };
            },
        });
        await clock.settle(limiter.start());
        expect(started.map(([, at]) => at)).toEqual([0, 2_000, 6_000, 14_000]);
        expect(done).toEqual([[1, 'recovered']]);
    });

    it('stops and releases jobs when a backoff_global wait exceeds maxWaitMs', async () => {
        const { clock, limiter, released } = setup({
            jobs: 3,
            concurrency: 1,
            maxWaitMs: 60_000,
            processJob: () => ({
                ok: false,
                error: { type: 'backoff_global', message: 'daily quota', retryAfterMs: 3_600_000 },
            }),
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(released).toEqual([1]);
    });

    it('a job whose own backoff_global wait exceeds maxWaitMs is released with that error as cause; the others without', async () => {
        const quota = { type: 'backoff_global', message: 'daily quota', retryAfterMs: 3_600_000 } as const;
        const { clock, limiter, outcomes } = setup({
            jobs: 3,
            concurrency: 2,
            maxWaitMs: 60_000,
            processJob: async (job) => {
                if (job.id === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
                }
                await sleep(clock, 10);
                // In-flight job 2 failure exceeds maxWaitMs after job 1 initiated lock.
                return { ok: false, error: quota };
            },
        });
        await clock.settle(limiter.start());
        // Job 1 buffered for probe is flushed on stop without error cause.
        expect(outcomes).toEqual([
            [1, { ok: false, status: 'released', error: { code: 'MAX_WAIT_EXCEEDED', message: expect.any(String) } }],
            [2, { ok: false, status: 'released', error: expect.objectContaining({ cause: quota }) }],
        ]);

        // Probe itself receives retryAfterMs exceeding maxWaitMs.
        const probe = setup({ jobs: 1, maxWaitMs: 60_000, processJob: () => ({ ok: false, error: quota }) });
        await probe.clock.settle(probe.limiter.start());
        expect(probe.outcomes).toEqual([
            [
                1,
                {
                    ok: false,
                    status: 'released',
                    error: expect.objectContaining({ code: 'MAX_WAIT_EXCEEDED', cause: quota }),
                },
            ],
        ]);
    });

    it('fails a job that can never fit and continues with the others', async () => {
        const { clock, limiter, done } = setup({
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 500 } },
                { job: { id: 2 }, cost: { tokens: 10 } },
            ]),
        });
        await clock.settle(limiter.start());
        expect(done).toContainEqual([1, 'COST_EXCEEDS_LIMIT']);
        expect(done).toContainEqual([2, 'result 2']);
    });

    it('fails a job with an invalid cost and stops on an invalid fetchJob entry', async () => {
        const { clock, limiter, done } = setup({
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: Number.NaN } },
                { job: { id: 2 }, cost: 'many' as never },
                { job: { id: 3 } },
            ]),
        });
        await clock.settle(limiter.start());
        expect(done).toContainEqual([1, 'INVALID_COST']);
        expect(done).toContainEqual([2, 'INVALID_COST']);
        expect(done).toContainEqual([3, 'result 3']);

        const broken = setup({ fetchJob: () => ({ notAJob: true }) as never });
        const result = await broken.clock.settle(broken.limiter.start());
        expect(!result.ok && result.error.code).toBe('FETCH_FAILED');
    });

    it('treats a throwing processJob() as fatal PROCESS_JOB_THREW', async () => {
        const results: JobResult<string>[] = [];
        const { clock, limiter, done } = setup({
            jobs: 1,
            processJob: () => {
                throw new Error('sdk exploded');
            },
            onAttempt: (_job, result) => {
                results.push(result);
            },
        });
        await clock.settle(limiter.start());
        expect(results[0]).toMatchObject({ ok: false, error: { type: 'fatal', code: 'PROCESS_JOB_THREW' } });
        expect(done).toEqual([[1, 'FATAL']]);
    });

    it('treats a malformed result as fatal', async () => {
        const { clock, limiter, done } = setup({
            jobs: 3,
            processJob: (job) =>
                [null, { ok: false, error: { type: 'nope' } }, { ok: 'yes' }][
                    job.id - 1
                ] as unknown as JobResult<string>,
        });
        await clock.settle(limiter.start());
        expect(done.map(([, code]) => code)).toEqual(['FATAL', 'FATAL', 'FATAL']);
    });

    it('aborts attempts that exceed timeoutMs', async () => {
        let aborted = false;
        const { clock, limiter, done } = setup({
            jobs: 1,
            maxRetries: 0,
            timeoutMs: 5_000,
            processJob: (_job, { signal }) =>
                new Promise((resolve) => {
                    signal.addEventListener('abort', () => {
                        aborted = true;
                        resolve({ ok: false, error: { type: 'fatal', message: 'aborted' } });
                    });
                }),
        });
        await clock.settle(limiter.start());
        expect(aborted).toBe(true);
        expect(done).toEqual([[1, 'RETRIES_EXHAUSTED']]);
    });

    it('stop() starts nothing new, releases buffered jobs, lets running jobs finish, and resolves with the summary', async () => {
        const { clock, limiter, done, released, jobs } = setup({
            jobs: 10,
            concurrency: 4,
            buckets: [createTPM(200)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 100 } },
                { job: { id: 2 }, cost: { tokens: 100 } },
                { job: { id: 3 }, cost: { tokens: 100 } },
            ]),
        });
        const promise = limiter.start();
        await clock.advance(0);
        // Job 3 remains buffered waiting for TPM capacity.
        const stopPromise = limiter.stop();
        const [result, stopResult] = await clock.settle(Promise.all([promise, stopPromise]));
        expect(done.map(([id]) => id)).toEqual([1, 2]);
        expect(released).toEqual([3]);
        expect(jobs.open).toHaveLength(10);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 1, stoppedBy: 'stop' } });
        expect(stopResult).toEqual(result);
    });

    it('stop() also releases jobs waiting for a retry and jobs arriving from an in-flight fetch', async () => {
        let resolveNext: (entry: FetchedJob<Job> | null) => void = () => {};
        let calls = 0;
        const { clock, limiter, released } = setup({
            concurrency: 2,
            fetchJob: () => {
                calls++;
                return calls === 1
                    ? { job: { id: 1 } }
                    : new Promise<FetchedJob<Job> | null>((resolve) => (resolveNext = resolve));
            },
            processJob: () => ({ ok: false, error: { type: 'retry_backoff', message: 'later' } }),
        });
        const promise = limiter.start();
        await clock.advance(0);
        const stopPromise = limiter.stop();
        resolveNext({ job: { id: 2 } });
        await clock.settle(Promise.all([promise, stopPromise]));
        expect(released.sort()).toEqual([1, 2]);
    });

    for (const type of ['retry_immediate', 'retry_backoff', 'backoff_global'] as const) {
        it(`stop() releases a running job that then fails with ${type}`, async () => {
            const { clock, limiter, released } = setup({
                jobs: 1,
                concurrency: 1,
                processJob: () =>
                    sleep(clock, 1000).then(() => ({ ok: false, error: { type, message: 'x', retryAfterMs: 10 } })),
            });
            const promise = limiter.start();
            await clock.advance(0);
            void limiter.stop();
            const result = await clock.settle(promise);
            expect(result).toMatchObject({ ok: true, data: { released: 1 } });
            expect(released).toEqual([1]);
        });
    }

    it('keeps the stop result and only logs FETCH_FAILED when fetchJob() fails after stop()', async () => {
        const { clock, limiter, logs } = setup({
            fetchJob: () => sleep(clock, 100).then(() => Promise.reject(new Error('db gone'))),
        });
        const promise = limiter.start();
        await clock.advance(0);
        const stopped = limiter.stop();
        const result = await clock.settle(promise);
        const summary = { ok: true, data: { done: 0, failed: 0, released: 0, stoppedBy: 'stop' } };
        expect(result).toEqual(summary);
        expect(await stopped).toEqual(summary);
        expect(limiter.getStatus().lastStoppedBy).toBe('stop');
        expect(logs.some((entry) => entry.code === 'FETCH_FAILED')).toBe(true);
    });

    it('resolves with FETCH_FAILED when fetchJob() throws', async () => {
        const { clock, limiter } = setup({
            fetchJob: () => {
                throw new Error('db gone');
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok).toBe(false);
        expect(!result.ok && result.error.code).toBe('FETCH_FAILED');
    });

    it('resolves with FETCH_FAILED when fetchJob() returns an invalid value', async () => {
        const { clock, limiter } = setup({ fetchJob: () => 'rows' as never });
        const result = await clock.settle(limiter.start());
        expect(!result.ok && result.error.code).toBe('FETCH_FAILED');
    });

    it('keeps running when hooks or the logger throw', async () => {
        const { clock, limiter, logs } = setup({
            jobs: 2,
            onAttempt: () => {
                throw new Error('log table locked');
            },
            onJobDone: async () => {
                throw new Error('insert failed');
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(2);
        expect(logs.filter((entry) => entry.code === 'HOOK_FAILED')).toHaveLength(4);

        const silent = createLimiter({
            fetchJob: () => null,
            processJob: () => ({ ok: true, data: 1 }),
            log: () => {
                throw new Error('logger broken');
            },
        });
        expect((await silent.start()).ok).toBe(true);
    });

    it.each([
        [
            'throws',
            () => {
                throw new Error('no subscription');
            },
        ],
        ['returns no unsubscribe function', () => undefined],
    ])('fails the run with BUCKET_FAILED when subscribe %s, instead of never waking up', async (_label, subscribe) => {
        const unsubscribed: string[] = [];
        const working = createBucket({
            subscribe: () => () => {
                unsubscribed.push('working');
            },
        });
        const broken = createBucket({ getWaitMs: () => -1, subscribe: subscribe as never });
        const { clock, limiter, started, jobs } = setup({ jobs: 1, buckets: [working, broken] });
        const result = await clock.settle(limiter.start());
        expect(result).toMatchObject({ ok: false, error: { code: 'BUCKET_FAILED' } });
        expect(started).toEqual([]);
        expect(jobs.fetches).toBe(0);
        expect(unsubscribed).toEqual(['working']);
        expect(limiter.getStatus().state).toBe('idle');
    });

    it('stops the run with BUCKET_FAILED when a bucket misbehaves during pre-fetch or post-fetch', async () => {
        const broken = createBucket({ getWaitMs: () => Number.NaN });
        const throwing = createBucket({
            getWaitMs: () => {
                throw new Error('bug');
            },
        });
        const settleThrows = createBucket({
            settle: () => {
                throw new Error('bug');
            },
        });
        for (const bucket of [broken, throwing]) {
            const { clock, limiter } = setup({ jobs: 1, buckets: [bucket] });
            const result = await clock.settle(limiter.start());
            expect(!result.ok && result.error.code).toBe('BUCKET_FAILED');
        }
        const { clock, limiter, done, logs } = setup({ jobs: 1, buckets: [settleThrows] });
        await clock.settle(limiter.start());
        expect(done).toEqual([[1, 'result 1']]);
        expect(logs.filter((entry) => entry.code === 'BUCKET_FAILED').map((entry) => entry.message)).toEqual([
            "A bucket's settle threw or rejected; the run continues.",
        ]);
    });

    it('refuses to start twice and can run again afterwards', async () => {
        const { clock, limiter, jobs } = setup({ jobs: 1 });
        const first = limiter.start();
        expect(await limiter.start()).toMatchObject({ ok: false, error: { code: 'ALREADY_RUNNING' } });
        await clock.settle(first);
        jobs.open.push({ id: 9 });
        expect(await clock.settle(limiter.start())).toMatchObject({ ok: true, data: { done: 1 } });
        await limiter.stop();
    });

    it.each([
        [null],
        [{ processJob: () => ({ ok: true, data: 1 }) }],
        [{ fetchJob: () => null, processJob: () => ({ ok: true, data: 1 }), buckets: [createRPM(0)] }],
    ])('returns INVALID_CONFIG from start() and stop() and logs it instead of throwing: %#', async (options) => {
        const logs: LogEntry[] = [];
        const limiter = createLimiter({ ...(options as object), log: (entry: LogEntry) => logs.push(entry) } as never);
        const result = await limiter.start();
        expect(!result.ok && result.error.code).toBe('INVALID_CONFIG');
        expect(logs[0]).toMatchObject({ level: 'error', code: 'INVALID_CONFIG' });
        const stopResult = await limiter.stop();
        expect(!stopResult.ok && stopResult.error.code).toBe('INVALID_CONFIG');
        limiter.wake();
        expect(limiter.isIdle()).toBe(true);
    });

    it('works with the real clock', async () => {
        const limiter = createLimiter({
            fetchJob: source(2).fetchJob,
            processJob: async (job: Job) => ({ ok: true, data: job.id }),
            buckets: [createSlidingWindow({ requests: 1, perMs: 20 })],
        });
        expect(await limiter.start()).toMatchObject({ ok: true, data: { done: 2 } });
    });
});

describe('createLimiter with a shared concurrency bucket', () => {
    function startRecorder(clock: FakeClock, bucket: ReturnType<typeof createConcurrencyBucket>, jobs: number) {
        const state = { starts: [] as number[] };
        const limiter = createLimiter<Job, string>({
            clock,
            concurrency: 5,
            buckets: [bucket],
            fetchJob: source(jobs).fetchJob,
            processJob: async () => {
                state.starts.push(clock.now());
                await sleep(clock, 1_000);
                return { ok: true, data: '' };
            },
        });
        return { limiter, state };
    }

    it('caps attempts in flight across limiters', async () => {
        const clock = createFakeClock();
        const shared = createConcurrencyBucket(2);
        const a = startRecorder(clock, shared, 4);
        const b = startRecorder(clock, shared, 4);
        const results = await clock.settle(Promise.all([a.limiter.start(), b.limiter.start()]));
        expect(results.map((result) => result.ok && result.data.done)).toEqual([4, 4]);
        const starts = [...a.state.starts, ...b.state.starts].sort((x, y) => x - y);
        expect(starts).toEqual([0, 0, 1_000, 1_000, 2_000, 2_000, 3_000, 3_000]);
    });

    it('hands a freed slot to the limiters in turn', async () => {
        const clock = createFakeClock();
        const shared = createConcurrencyBucket(1);
        const order: string[] = [];
        function make(name: string) {
            return createLimiter<Job, string>({
                clock,
                buckets: [shared],
                fetchJob: source(3).fetchJob,
                processJob: async () => {
                    order.push(name);
                    await sleep(clock, 1_000);
                    return { ok: true, data: '' };
                },
            });
        }
        await clock.settle(Promise.all([make('a').start(), make('b').start()]));
        expect(order).toEqual(['a', 'b', 'a', 'b', 'a', 'b']);
    });

    it('fails a job whose result cannot be read and frees its slot in a shared bucket', async () => {
        const shared = createConcurrencyBucket(1);
        const { clock, limiter, done } = setup({
            jobs: 2,
            buckets: [shared],
            processJob: (job) =>
                job.id === 1
                    ? { ok: false, error: { type: 'retry_backoff', message: Object.create(null) } }
                    : { ok: true, data: `result ${job.id}` },
        });
        const result = await clock.settle(limiter.start());
        expect(done).toEqual([
            [1, 'FATAL'],
            [2, 'result 2'],
        ]);
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 1, released: 0, stoppedBy: 'empty' } });
        expect(limiter.getStatus().running).toBe(0);
        expect(shared.getWaitMs({ requests: 1 }, clock.now())).toBe(0);
    });
});

describe('createLimiter options from the integration feedback', () => {
    it.each([
        ['ignores its signal', 1_000, 5_000, 1],
        ['honours its signal', 1_000, 0, 0],
        ['runs long without timeoutMs', undefined, 5_000, 0],
    ] as const)('warns once when processJob %s', async (_label, timeoutMs, ignoreForMs, warnings) => {
        const warned: [number, LogEntry][] = [];
        const { clock, limiter } = setup({
            jobs: 1,
            ...(timeoutMs === undefined ? {} : { timeoutMs }),
            processJob: (job, { signal }) =>
                new Promise((resolve) => {
                    function finish(): void {
                        resolve({ ok: true, data: `result ${job.id}` });
                    }
                    if (ignoreForMs > 0) {
                        clock.setTimeout(finish, ignoreForMs);
                    } else {
                        signal.addEventListener('abort', finish);
                    }
                }),
            log: (entry) => {
                if (entry.level === 'warn') {
                    warned.push([clock.now(), entry]);
                }
            },
        });
        await clock.settle(limiter.start());
        expect(warned.length).toBe(warnings);
        if (warnings > 0) {
            expect(warned[0][0]).toBe(2_000);
            expect(warned[0][1].message).toContain('processJob is still running 2000 ms after it started');
            expect(warned[0][1].data).toEqual({ job: { id: 1 }, attempt: 1 });
        }
    });

    it('keeps a timed-out attempt in its slot until processJob really ends, and returns late ok: true', async () => {
        const { clock, limiter, started, done } = setup({
            jobs: 2,
            concurrency: 1,
            maxRetries: 0,
            timeoutMs: 1_000,
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                await sleep(clock, 5_000);
                return { ok: true, data: `late ${job.id}` };
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, 5_000],
        ]);
        expect(done).toEqual([
            [1, 'late 1'],
            [2, 'late 2'],
        ]);
    });

    it('uses exact exponential backoff without jitter, capped at maxRetryDelayMs', async () => {
        let calls = 0;
        const { clock, limiter, started } = setup({
            jobs: 1,
            retryDelayMs: 1_000,
            maxRetryDelayMs: 3_000,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                calls++;
                return calls <= 4
                    ? { ok: false, error: { type: 'retry_backoff', message: 'flaky' } }
                    : { ok: true, data: '' };
            },
        });
        await clock.settle(limiter.start());
        // Exponential delays 1000, 2000 capped at maxRetryDelayMs 3000.
        expect(started.map(([, at]) => at)).toEqual([0, 1_000, 3_000, 6_000, 9_000]);
    });

    it('stop({ abort: true }) aborts running attempts and releases the ones that did not succeed', async () => {
        const reasons: unknown[] = [];
        const { clock, limiter, released } = setup({
            jobs: 2,
            concurrency: 2,
            processJob: (job, { signal }) =>
                new Promise((resolve) => {
                    signal.addEventListener('abort', () => {
                        reasons.push(signal.reason);
                        resolve(
                            job.id === 1
                                ? { ok: false, error: { type: 'fatal', message: 'aborted' } }
                                : { ok: true, data: 'made it' },
                        );
                    });
                }),
        });
        const promise = limiter.start();
        await clock.advance(0);
        void limiter.stop();
        void limiter.stop({ abort: true });
        expect(await clock.settle(promise)).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 1, stoppedBy: 'stop' },
        });
        expect(released).toEqual([1]);
        expect(reasons.map((reason) => (reason as DOMException).name)).toEqual(['AbortError', 'AbortError']);
    });

    it.each([
        ['fails a job that returned fatal', { type: 'fatal' }, 0, 'FATAL'],
        ['fails a job that used its last retry', { type: 'retry_immediate' }, 0, 'RETRIES_EXHAUSTED'],
        ['still releases a job that would retry', { type: 'retry_immediate' }, 4, 'released'],
    ] as const)('stop({ abort: true }) after the result came in %s', async (_label, error, maxRetries, expected) => {
        let gate: Promise<void> | undefined;
        const { clock, limiter, done, released } = setup({
            jobs: 2,
            concurrency: 2,
            maxRetries,
            processJob: async (job) => {
                // Shared timer: both results resolve within same tick.
                gate ??= sleep(clock, 1_000);
                await gate;
                return job.id === 1 ? { ok: true, data: 'result 1' } : { ok: false, error: { ...error, message: 'x' } };
            },
            // Executes after job 2 resolves but prior to limiter completion handling.
            onAttempt: (job) => {
                if (job.id === 1) {
                    void limiter.stop({ abort: true });
                }
            },
        });
        await clock.settle(limiter.start());
        if (expected === 'released') {
            expect(released).toEqual([2]);
        } else {
            expect(done).toEqual([
                [1, 'result 1'],
                [2, expected],
            ]);
            expect(released).toEqual([]);
        }
    });
});

describe('info of a released job', () => {
    it('gives a job released after attempts the settled cost and duration of its last attempt', async () => {
        const infos: [number, string, AttemptInfo][] = [];
        const { clock, limiter } = setup({
            buckets: [createTPM(1_000)],
            fetchJob: fetchFrom([{ job: { id: 1 }, cost: { tokens: 100 } }]),
            processJob: async () => {
                await sleep(clock, 300);
                return {
                    ok: false,
                    error: { type: 'retry_backoff', message: 'x', retryAfterMs: 5_000, usage: { tokens: 30 } },
                };
            },
            onJobDone: (job, result, info) => {
                infos.push([job.id, result.status, info]);
            },
        });
        const run = limiter.start();
        await clock.advance(1_000);
        await clock.settle(limiter.stop());
        await run;
        expect(infos).toEqual([[1, 'released', { attempt: 1, cost: { requests: 1, tokens: 30 }, durationMs: 300 }]]);
    });

    it('gives a job released before its first attempt attempt 0 and its estimated cost', async () => {
        const infos: [number, string, AttemptInfo][] = [];
        // Job 2 buffered waiting for tokens is released by stop prior to first attempt.
        const { clock, limiter } = setup({
            concurrency: 2,
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 100 } },
                { job: { id: 2 }, cost: { tokens: 50 } },
            ]),
            onJobDone: (job, result, info) => {
                infos.push([job.id, result.status, info]);
            },
        });
        const run = limiter.start();
        await clock.advance(100);
        await clock.settle(limiter.stop());
        await run;
        expect(infos).toEqual([
            [2, 'released', { attempt: 0, cost: { requests: 1, tokens: 50 }, durationMs: 0 }],
            [1, 'done', { attempt: 1, cost: { requests: 1, tokens: 100 }, durationMs: 1_000 }],
        ]);
    });
});

describe('hook order with an async onAttempt', () => {
    it.each([
        ['retry_immediate', {}],
        ['retry_backoff', { retryAfterMs: 10 }],
        ['backoff_global', { retryAfterMs: 10 }],
    ] as const)('starts the retry after %s only when the previous onAttempt returned', async (type, extra) => {
        const events: string[] = [];
        let attempts = 0;
        const { clock, limiter } = setup({
            jobs: 1,
            processJob: () => {
                attempts++;
                events.push(`attempt ${attempts} at ${clock.now()}`);
                return attempts === 1
                    ? { ok: false, error: { type, message: 'x', ...extra } }
                    : { ok: true, data: 'ok' };
            },
            onAttempt: async (_job, _result, info) => {
                events.push(`onAttempt ${info.attempt} start`);
                await sleep(clock, 50);
                events.push(`onAttempt ${info.attempt} end`);
            },
            onJobDone: (_job, result) => {
                events.push(`onJobDone ${result.status}`);
            },
        });
        await clock.settle(limiter.start());
        expect(events).toEqual([
            'attempt 1 at 0',
            'onAttempt 1 start',
            'onAttempt 1 end',
            'attempt 2 at 50',
            'onAttempt 2 start',
            'onAttempt 2 end',
            'onJobDone done',
        ]);
    });

    it('releases a job that would retry only after its onAttempt returned when the run stops meanwhile', async () => {
        const events: string[] = [];
        const { clock, limiter } = setup({
            jobs: 1,
            processJob: () => ({ ok: false, error: { type: 'retry_immediate', message: 'x' } }),
            onAttempt: async (_job, _result, info) => {
                events.push(`onAttempt ${info.attempt} start`);
                void limiter.stop();
                await sleep(clock, 50);
                events.push(`onAttempt ${info.attempt} end`);
            },
            onJobDone: (_job, result) => {
                events.push(`onJobDone ${result.status}`);
            },
        });
        const result = await clock.settle(limiter.start());
        expect(events).toEqual(['onAttempt 1 start', 'onAttempt 1 end', 'onJobDone released']);
        expect(result).toEqual({ ok: true, data: { done: 0, failed: 0, released: 1, stoppedBy: 'stop' } });
    });

    it('fetches no new job while a ready retry only waits for its onAttempt', async () => {
        const { clock, limiter, jobs } = setup({
            jobs: 3,
            concurrency: 2,
            maxBufferSize: 2,
            processJob: async (job, { attempt }) => {
                if (job.id === 1 && attempt === 1) {
                    return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
                }
                await sleep(clock, 1_000);
                return { ok: true, data: `result ${job.id}` };
            },
            onAttempt: async (job, _result, info) => {
                if (job.id === 1 && info.attempt === 1) {
                    await sleep(clock, 50);
                }
            },
        });
        const run = limiter.start();
        await clock.advance(10);
        // Job 1 awaits onAttempt while job 2 runs; second slot reserved for job 1 retry.
        expect(jobs.fetches).toBe(2);
        expect(limiter.getStatus()).toMatchObject({ running: 1, buffered: 1 });
        await clock.settle(run);
    });

    it('keeps the retry timer of another job while a ready retry waits for its onAttempt', async () => {
        const started: string[] = [];
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            processJob: async (job, { attempt }) => {
                started.push(`${job.id}#${attempt}@${clock.now()}`);
                if (job.id === 1 && attempt === 1) {
                    await sleep(clock, 10);
                    return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
                }
                if (job.id === 2 && attempt === 1) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 100 } };
                }
                return { ok: true, data: 'ok' };
            },
            onAttempt: async (job, _result, info) => {
                if (job.id === 1 && info.attempt === 1) {
                    await sleep(clock, 1_000);
                }
            },
        });
        await clock.settle(limiter.start());
        expect(started).toEqual(['1#1@0', '2#1@0', '2#2@100', '1#2@1010']);
    });

    it('fetches into the free slots beyond the one a ready retry keeps while it waits for its onAttempt', async () => {
        const { clock, limiter, jobs } = setup({
            jobs: 5,
            concurrency: 5,
            processJob: async (job, { attempt }) => {
                if (job.id === 1 && attempt === 1) {
                    return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
                }
                await sleep(clock, 2_000);
                return { ok: true, data: `result ${job.id}` };
            },
            onAttempt: async (job, _result, info) => {
                if (job.id === 1 && info.attempt === 1) {
                    await sleep(clock, 1_000);
                }
            },
        });
        const run = limiter.start();
        await clock.advance(500);
        // Jobs 2-5 fill 4 slots; fifth slot reserved for job 1 retry.
        expect(limiter.getStatus()).toMatchObject({ running: 4, buffered: 1 });
        expect(jobs.fetches).toBe(5);
        await clock.settle(run);
    });
    it('lets a probe retry that awaits its onAttempt keep the single probe slot of a waiting lock', async () => {
        const started: string[] = [];
        const { clock, limiter } = setup({
            jobs: 3,
            concurrency: 2,
            processJob: async (job, { attempt }) => {
                started.push(`${job.id}#${attempt}@${clock.now()}`);
                if (job.id === 1 && attempt === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 100 } };
                }
                if (job.id === 1 && attempt === 2) {
                    return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
                }
                if (job.id === 2) {
                    await sleep(clock, 50);
                }
                return { ok: true, data: 'ok' };
            },
            onAttempt: async (job, _result, info) => {
                if (job.id === 1 && info.attempt === 2) {
                    await sleep(clock, 50);
                }
            },
        });
        const result = await clock.settle(limiter.start());
        // Job 1 probes at t=150 post-onAttempt; job 3 dispatches upon unlock.
        expect(started).toEqual(['1#1@0', '2#1@0', '1#2@100', '1#3@150', '3#1@150']);
        expect(result).toEqual({ ok: true, data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' } });
    });
});

describe('getStatus', () => {
    const IDLE = { state: 'idle', running: 0, buffered: 0, lockUntil: 0, lastStoppedBy: undefined } as const;

    it('reports running, locked, probing and idle across a run with a global lock', async () => {
        const statuses: [string, LimiterStatus][] = [];
        let attempts = 0;
        const { clock, limiter } = setup({
            jobs: 1,
            concurrency: 1,
            processJob: async () => {
                attempts++;
                statuses.push([`attempt ${attempts}`, limiter.getStatus()]);
                await sleep(clock, 100);
                return attempts === 1
                    ? { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } }
                    : { ok: true, data: 'ok' };
            },
            onAttempt: () => {
                statuses.push([`after attempt ${attempts}`, limiter.getStatus()]);
            },
        });
        expect(limiter.getStatus()).toEqual(IDLE);
        await clock.settle(limiter.start());
        expect(statuses).toEqual([
            ['attempt 1', { ...IDLE, state: 'running', running: 1 }],
            ['after attempt 1', { ...IDLE, state: 'locked', buffered: 1, lockUntil: 1_100 }],
            ['attempt 2', { ...IDLE, state: 'probing', running: 1 }],
            ['after attempt 2', { ...IDLE, state: 'running' }],
        ]);
        expect(limiter.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'empty' });
    });

    it('reports stopping while running attempts finish, then lastStoppedBy stop', async () => {
        const { clock, limiter } = setup({ jobs: 3, concurrency: 2 });
        const run = limiter.start();
        await clock.advance(0);
        expect(limiter.getStatus()).toEqual({ ...IDLE, state: 'running', running: 2 });
        void limiter.stop();
        expect(limiter.getStatus()).toEqual({ ...IDLE, state: 'stopping', running: 2 });
        await clock.settle(run);
        expect(limiter.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'stop' });
    });

    it('keeps lockUntil while idle after a run ended locked by stop() or max_wait_exceeded', async () => {
        const stopped = setup({
            jobs: 1,
            processJob: () => ({ ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 5_000 } }),
        });
        const run = stopped.limiter.start();
        await stopped.clock.advance(0);
        expect(stopped.limiter.getStatus()).toEqual({ ...IDLE, state: 'locked', buffered: 1, lockUntil: 5_000 });
        await stopped.limiter.stop();
        await stopped.clock.settle(run);
        expect(stopped.limiter.getStatus()).toEqual({ ...IDLE, lockUntil: 5_000, lastStoppedBy: 'stop' });

        const exceeded = setup({
            jobs: 1,
            maxWaitMs: 60_000,
            processJob: () => ({
                ok: false,
                error: { type: 'backoff_global', message: 'daily quota', retryAfterMs: 3_600_000 },
            }),
        });
        await exceeded.clock.settle(exceeded.limiter.start());
        expect(exceeded.limiter.getStatus()).toEqual({
            ...IDLE,
            lockUntil: 3_600_000,
            lastStoppedBy: 'max_wait_exceeded',
        });
        await exceeded.clock.settle(exceeded.limiter.start({ force: true }));
        expect(exceeded.limiter.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'empty' });
    });

    it('reports lastStoppedBy error until the next run finishes, and error for an invalid config', async () => {
        let calls = 0;
        const { clock, limiter } = setup({
            fetchJob: () => {
                calls++;
                if (calls === 1) {
                    throw new Error('db down');
                }
                return null;
            },
        });
        await clock.settle(limiter.start());
        expect(limiter.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'error' });
        await clock.settle(limiter.start());
        expect(limiter.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'empty' });

        const invalid = createLimiter({
            fetchJob: () => null,
            processJob: () => ({ ok: true, data: 1 }),
            concurrency: 0,
        });
        expect(invalid.getStatus()).toEqual({ ...IDLE, lastStoppedBy: 'error' });
    });
});

describe('wake and isIdle', () => {
    it('a wake during a fetch is not lost when that fetch returns null', async () => {
        const open: Job[] = [];
        let fetches = 0;
        const { clock, limiter, done } = setup({
            fetchJob: async () => {
                fetches++;
                const job = open.shift();
                await sleep(clock, 100);
                return job ? { job } : null;
            },
        });
        const promise = limiter.start();
        await clock.advance(50);
        open.push({ id: 7 });
        limiter.wake();
        await clock.settle(promise);
        expect(done).toEqual([[7, 'result 7']]);
        expect(fetches).toBe(3);
    });

    it('wake() during an active run fetches again before the run resolves', async () => {
        const open: Job[] = [{ id: 1 }];
        const { clock, limiter, done } = setup({
            fetchJob: () => {
                const job = open.shift();
                return job ? { job } : null;
            },
            onJobDone: (job, result) => {
                done.push([job.id, result.ok ? result.data : result.error.code]);
                if (job.id === 1) {
                    open.push({ id: 2 });
                    limiter.wake();
                }
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(done.map(([id]) => id)).toEqual([1, 2]);
    });

    it('wake() when idle starts a new run automatically unless stopped by user', async () => {
        const open: Job[] = [];
        let fetches = 0;
        const { clock, limiter, done } = setup({
            fetchJob: () => {
                fetches++;
                const job = open.shift();
                return job ? { job } : null;
            },
        });
        expect(limiter.isIdle()).toBe(true);
        open.push({ id: 1 }, { id: 2 });
        limiter.wake();
        expect(limiter.isIdle()).toBe(false);
        await clock.advance(1_000);
        expect(done).toEqual([
            [1, 'result 1'],
            [2, 'result 2'],
        ]);
        expect(limiter.isIdle()).toBe(true);
        expect(fetches).toBe(3);

        await limiter.stop();
        open.push({ id: 3 });
        limiter.wake();
        await clock.advance(1_000);
        expect(fetches).toBe(3);
        expect(limiter.isIdle()).toBe(true);

        await clock.settle(limiter.start());
        expect(done.map(([id]) => id)).toEqual([1, 2, 3]);
    });
});

describe('implementation plan invariants and edge cases', () => {
    it('epoch-guarded lock/unlock: stale stragglers at t=0 neither unlock while locked nor re-lock after unlock', async () => {
        const resolvers = new Map<number, (res: JobResult<string>) => void>();
        const { clock, limiter, started, done } = setup({
            jobs: 4,
            concurrency: 3,
            retryDelayMs: 0,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                const attempt = started.filter(([id]) => id === job.id).length;
                if (job.id === 4 || (job.id >= 2 && attempt > 1)) {
                    return { ok: true, data: `ok-${job.id}` };
                }
                const key = job.id * 10 + attempt;
                return new Promise<JobResult<string>>((resolve) => {
                    resolvers.set(key, resolve);
                });
            },
        });

        const runPromise = limiter.start();
        await clock.advance(0);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
        ]);

        // retryAfterMs=0 enforces 1s minimum lock; probe starts at t=1000.
        resolvers.get(11)?.({ ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 0 } });
        await clock.advance(1_000);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            [1, 1_000],
        ]);

        // Stale success during probe must not unlock or trigger next job.
        resolvers.get(21)?.({ ok: true, data: 'stale-2-ok' });
        await clock.advance(0);
        expect(started.map(([id]) => id)).toEqual([1, 2, 3, 1]);

        // Probe success unlocks limiter and starts job 4.
        resolvers.get(12)?.({ ok: true, data: 'probe-1-ok' });
        await clock.advance(0);
        expect(done).toContainEqual([1, 'probe-1-ok']);
        expect(done).toContainEqual([4, 'ok-4']);

        // Stale backoff_global failure must not re-lock.
        resolvers.get(31)?.({
            ok: false,
            error: { type: 'backoff_global', message: 'late 429', retryAfterMs: 60_000 },
        });
        await clock.advance(0);

        // Unlocked: retry proceeds without 60s lock.
        const result = await clock.settle(runPromise);
        expect(result).toEqual({ ok: true, data: { done: 4, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(clock.now()).toBe(1_000);
    });

    it('stop() during a probe keeps lock as waiting for next start(), and probe result during stop() still updates lock', async () => {
        let resolveProbe!: (res: JobResult<string>) => void;
        let runPhase = 1;
        const open: Job[] = [{ id: 1 }, { id: 2 }, { id: 3 }];
        const { clock, limiter, started, released } = setup({
            concurrency: 2,
            fetchJob: () => {
                const job = open.shift();
                return job ? { job } : null;
            },
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                if (runPhase === 1 && job.id === 1 && started.length === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: 'outage', retryAfterMs: 1_000 } };
                }
                if (runPhase === 1 && job.id === 1 && started.length === 3) {
                    return new Promise<JobResult<string>>((resolve) => {
                        resolveProbe = resolve;
                    });
                }
                return { ok: true, data: `ok ${job.id}` };
            },
        });

        const run1 = limiter.start();
        await clock.advance(0);
        await clock.advance(1_000);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [1, 1_000],
        ]);

        const stopPromise = limiter.stop();
        resolveProbe({ ok: false, error: { type: 'backoff_global', message: 'still down', retryAfterMs: 500 } });
        await clock.settle(stopPromise);
        const res1 = await run1;
        expect(res1).toEqual({ ok: true, data: { done: 1, failed: 0, released: 1, stoppedBy: 'stop' } });
        expect(released).toEqual([1]);

        runPhase = 2;
        open.unshift({ id: 1 });
        open.push({ id: 4 });
        const run2 = limiter.start();
        await clock.advance(0);
        // Lock waiting until t=1500: no starts at t=1000.
        expect(started.length).toBe(3);

        // Probe initiates before opening second slot.
        await clock.advance(500);
        expect(await clock.settle(run2)).toEqual({
            ok: true,
            data: { done: 3, failed: 0, released: 0, stoppedBy: 'empty' },
        });
    });

    it('per-item retry_backoff exceeding maxWaitMs fails only that item, while backoff_global stops the run', async () => {
        const { clock, limiter, done, outcomes, released } = setup({
            jobs: 3,
            concurrency: 2,
            maxWaitMs: 2_000,
            processJob: (job) => {
                if (job.id === 1) {
                    return {
                        ok: false,
                        error: { type: 'retry_backoff', message: 'item too slow', retryAfterMs: 5_000 },
                    };
                }
                return { ok: true, data: `ok ${job.id}` };
            },
        });

        const res = await clock.settle(limiter.start());
        expect(res).toEqual({ ok: true, data: { done: 2, failed: 1, released: 0, stoppedBy: 'empty' } });
        expect(done).toEqual([
            [1, 'MAX_WAIT_EXCEEDED'],
            [2, 'ok 2'],
            [3, 'ok 3'],
        ]);
        expect(outcomes[0][1].status).toBe('failed');
        expect(released).toEqual([]);
    });

    it('a single bucket wait over maxWaitMs stops the run: running attempts finish unaborted, buffered and retry-waiting jobs are released, never failed', async () => {
        // Job 3 hits daily bucket limit at t=500 while job 2 waits for retry at t=1000 and job 1 runs.
        const wall = createBucket({
            getWaitMs: (cost, now) => (cost.daily ? (now < 500 ? 500 - now : 86_400_000) : 0),
        });
        let abortedOnFinish: boolean | undefined;
        const { clock, limiter, outcomes } = setup({
            concurrency: 2,
            maxWaitMs: 60_000,
            buckets: [wall],
            fetchJob: fetchFrom([{ job: { id: 1 } }, { job: { id: 2 } }, { job: { id: 3 }, cost: { daily: 1 } }]),
            processJob: async (job, { signal }) => {
                if (job.id === 2) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'busy', retryAfterMs: 1_000 } };
                }
                await sleep(clock, 5_000);
                abortedOnFinish = signal.aborted;
                return { ok: true, data: `result ${job.id}` };
            },
        });
        const run = limiter.start();
        await clock.advance(499);
        expect(limiter.getStatus()).toMatchObject({ state: 'running', running: 1, buffered: 2 });
        expect(outcomes).toEqual([]);
        await clock.advance(1);
        expect(limiter.getStatus()).toMatchObject({ state: 'stopping', running: 1, buffered: 0 });
        expect(outcomes.map(([id, outcome]) => [id, outcome.status, !outcome.ok && outcome.error.code])).toEqual([
            [2, 'released', 'MAX_WAIT_EXCEEDED'],
            [3, 'released', 'MAX_WAIT_EXCEEDED'],
        ]);
        const result = await clock.settle(run);
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 0, released: 2, stoppedBy: 'max_wait_exceeded' } });
        expect(outcomes.at(-1)?.[1].status).toBe('done');
        expect(abortedOnFinish).toBe(false);
        expect(clock.now()).toBe(5_000);
    });

    it('per-item retry_backoff fails with MAX_WAIT_EXCEEDED once the cumulative wait since its first failure exceeds maxWaitMs', async () => {
        const { clock, limiter, started, done } = setup({
            jobs: 1,
            maxRetries: 10,
            maxWaitMs: 2_500,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                return { ok: false, error: { type: 'retry_backoff', message: 'slow item', retryAfterMs: 1_000 } };
            },
        });
        const result = await clock.settle(limiter.start());
        // First failure at t=0: cumulative wait at t=2000 reaches 3000ms, exceeding maxWaitMs (2500ms).
        expect(started).toEqual([
            [1, 0],
            [1, 1_000],
            [1, 2_000],
        ]);
        expect(done).toEqual([[1, 'MAX_WAIT_EXCEEDED']]);
        expect(result).toEqual({ ok: true, data: { done: 0, failed: 1, released: 0, stoppedBy: 'empty' } });
    });

    it('does not count time in a global lock toward the retry_backoff budget of a job', async () => {
        const calls = new Map<number, number>();
        const { clock, limiter, started, done } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 10_000,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                const call = (calls.get(job.id) ?? 0) + 1;
                calls.set(job.id, call);
                if (job.id === 1 && call <= 2) {
                    // Job 1 waits 1s then 3s (4s net wait), excluding 8s global lock.
                    const retryAfterMs = call === 1 ? 1_000 : 3_000;
                    return { ok: false, error: { type: 'retry_backoff', message: 'slow item', retryAfterMs } };
                }
                if (job.id === 2 && call === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: 'outage', retryAfterMs: 8_000 } };
                }
                return { ok: true, data: `result ${job.id}` };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [2, 8_000],
            [1, 8_000],
            [1, 11_000],
        ]);
        expect(done).toEqual([
            [2, 'result 2'],
            [1, 'result 1'],
        ]);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
    });

    it('still counts the time outside a global lock toward the retry_backoff budget of a job', async () => {
        const calls = new Map<number, number>();
        const { clock, limiter, done } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 10_000,
            processJob: (job) => {
                const call = (calls.get(job.id) ?? 0) + 1;
                calls.set(job.id, call);
                if (job.id === 1) {
                    // Delays of 4s, 4s, 5s; 2s global lock overlaps initial delay.
                    const retryAfterMs = [4_000, 4_000, 5_000][call - 1] ?? 1;
                    return { ok: false, error: { type: 'retry_backoff', message: 'slow item', retryAfterMs } };
                }
                if (call === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: 'outage', retryAfterMs: 2_000 } };
                }
                return { ok: true, data: `result ${job.id}` };
            },
        });
        await clock.settle(limiter.start());
        // Third failure at t=8000: 6s non-lock time + 5s requested exceeds 10s budget.
        expect(done).toEqual([
            [2, 'result 2'],
            [1, 'MAX_WAIT_EXCEEDED'],
        ]);
    });

    it('does not add the time in a global lock to a bucket wait of a job', async () => {
        let callsOfA = 0;
        const { clock, limiter, started } = setup({
            concurrency: 2,
            maxWaitMs: 90_000,
            buckets: [createTPM(100)],
            fetchJob: fetchFrom([
                { job: { id: 1 }, cost: { tokens: 60 } },
                { job: { id: 2 }, cost: { tokens: 60 } },
            ]),
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                if (job.id === 1 && ++callsOfA === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: 'outage', retryAfterMs: 70_000 } };
                }
                return { ok: true, data: `result ${job.id}` };
            },
        });
        const result = await clock.settle(limiter.start());
        // Job 2 token wait and 60s post-unlock window fit within maxWaitMs (90s); dispatches at t=130s.
        expect(started).toEqual([
            [1, 0],
            [1, 70_000],
            [2, 130_000],
        ]);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
    });

    it('never stops for time waiting for a slot (WAIT_FOR_CHANGE) or for timed waits that only add up', async () => {
        // Simulates external slot hold: closed from t=500 to t=20000.
        let isOpen = true;
        const listeners = new Set<() => void>();
        const slot = createBucket({
            getWaitMs: () => (isOpen ? 0 : WAIT_FOR_CHANGE),
            subscribe(onChange) {
                listeners.add(onChange);
                return () => listeners.delete(onChange);
            },
        });
        const tokens = createSlidingWindow({ tokens: 100, perMs: 1_000, used: [{ at: 0, amount: 100 }] });
        const { clock, limiter, started } = setup({
            maxWaitMs: 5_000,
            buckets: [slot, tokens],
            fetchJob: fetchFrom([{ job: { id: 1 }, cost: { tokens: 100 } }]),
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                return { ok: true, data: `result ${job.id}` };
            },
        });
        clock.setTimeout(() => {
            isOpen = false;
        }, 500);
        clock.setTimeout(() => {
            // External usage reserves window prior to slot release, adding 1000ms timed wait.
            tokens.settle(tokens.reserve({ tokens: 100 }, 20_000), { tokens: 100 }, 20_000);
            isOpen = true;
            for (const onChange of listeners) {
                onChange();
            }
        }, 20_000);
        const result = await clock.settle(limiter.start());
        // Cumulative 21s wait composed of discrete intervals; no single interval exceeds maxWaitMs (5s).
        expect(started).toEqual([[1, 21_000]]);
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 0, released: 0, stoppedBy: 'empty' } });
    });

    it('cumulative backoff_global totalWaitMs exceeding maxWaitMs stops the run and start({ force: true }) resets persisted lock', async () => {
        const open: Job[] = [{ id: 1 }, { id: 2 }];
        let failMode = true;
        const { clock, limiter, released, done } = setup({
            concurrency: 1,
            maxWaitMs: 1_500,
            fetchJob: () => {
                const job = open.shift();
                return job ? { job } : null;
            },
            processJob: (job) => {
                if (failMode) {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 1_000 } };
                }
                return { ok: true, data: `ok ${job.id}` };
            },
        });

        // Cumulative backoff delay (2000ms) exceeds maxWaitMs (1500ms).
        const res1 = await clock.settle(limiter.start());
        expect(res1).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(released).toEqual([1]);

        open.unshift({ id: 1 });
        failMode = false;

        const res2 = await clock.settle(limiter.start({ force: true }));
        expect(res2).toEqual({
            ok: true,
            data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' },
        });
        expect(done).toEqual([
            [1, 'ok 1'],
            [2, 'ok 2'],
        ]);
    });

    it('persisted lock exceeding maxWaitMs on start() resolves immediately without hanging, and start({ force: true }) recovers', async () => {
        const open: Job[] = [{ id: 1 }];
        let shouldFail = true;
        const { clock, limiter, released, done } = setup({
            concurrency: 1,
            maxWaitMs: 1_000,
            fetchJob: () => {
                const job = open.shift();
                return job ? { job } : null;
            },
            processJob: (job) => {
                if (shouldFail) {
                    return { ok: false, error: { type: 'backoff_global', message: 'long 429', retryAfterMs: 5_000 } };
                }
                return { ok: true, data: `ok ${job.id}` };
            },
        });

        const res1 = await clock.settle(limiter.start());
        expect(res1).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(released).toEqual([1]);
        open.unshift({ id: 1 });
        shouldFail = false;

        // Unforced start stops immediately when remaining lock exceeds maxWaitMs.
        const res2 = await clock.settle(limiter.start());
        expect(res2).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 0, stoppedBy: 'max_wait_exceeded' },
        });

        const res3 = await clock.settle(limiter.start({ force: true }));
        expect(res3).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'empty' },
        });
        expect(done).toEqual([[1, 'ok 1']]);
    });

    it('stop() while waiting on an RPM bucket with nothing buffered or running resolves without hanging', async () => {
        const clock = createFakeClock();
        const bucket = createRPM(1);
        const open: Job[] = [{ id: 1 }, { id: 2 }];
        const limiter = createLimiter<Job, string>({
            buckets: [bucket],
            concurrency: 1,
            clock,
            fetchJob: () => {
                const job = open.shift();
                return job ? { job } : null;
            },
            processJob: (job) => ({ ok: true, data: `ok ${job.id}` }),
            onJobDone: () => {},
        });

        const runPromise = limiter.start();
        await clock.advance(0);
        // Limiter waits on RPM bucket prior to fetching job 2.
        await limiter.stop();
        const result = await runPromise;
        expect(result).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'stop' },
        });
        expect(open).toEqual([{ id: 2 }]);
    });

    it.each([10, Number.POSITIVE_INFINITY])(
        'maxBufferSize %p lets free slots keep fetching while jobs sleep',
        async (maxBufferSize) => {
            const sleepers = 3;
            let fetches = 0;
            const open: Job[] = Array.from({ length: sleepers + 1 }, (_, i) => ({ id: i + 1 }));
            const started: number[] = [];
            const { clock, limiter } = setup({
                concurrency: 1,
                maxBufferSize,
                fetchJob: () => {
                    fetches++;
                    const job = open.shift();
                    return job ? { job } : null;
                },
                processJob: (job, { attempt }) => {
                    started.push(job.id);
                    if (job.id <= sleepers && attempt === 1) {
                        return { ok: false, error: { type: 'retry_backoff', message: 'wait', retryAfterMs: 1_000 } };
                    }
                    return { ok: true, data: `ok ${job.id}` };
                },
            });

            const runPromise = limiter.start();
            await clock.advance(0);
            // Sleeping retries release slots: successive jobs are fetched before retries mature.
            expect(started).toEqual(Array.from({ length: sleepers + 1 }, (_, i) => i + 1));
            expect(open).toEqual([]);
            expect(await clock.settle(runPromise)).toEqual({
                ok: true,
                data: { done: sleepers + 1, failed: 0, released: 0, stoppedBy: 'empty' },
            });
            expect(fetches).toBe(sleepers + 2);
        },
    );

    it('caps the internal buffer at concurrency when jobs are sleeping on retry_backoff', async () => {
        let fetches = 0;
        const open: Job[] = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }];
        const { clock, limiter } = setup({
            concurrency: 1,
            fetchJob: () => {
                fetches++;
                const job = open.shift();
                return job ? { job } : null;
            },
            processJob: (job) => {
                if (job.id === 1) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'wait', retryAfterMs: 1_000 } };
                }
                return { ok: true, data: `ok ${job.id}` };
            },
        });

        const runPromise = limiter.start();
        await clock.advance(0);
        // Sleeping retry saturates buffer (concurrency 1); pre-fetching paused.
        expect(fetches).toBe(1);
        expect(open.map((j) => j.id)).toEqual([2, 3, 4]);

        await limiter.stop();
        await clock.settle(runPromise);
    });

    it('probe order: initiator and failed probe unshift ahead of retry_backoff items; concurrent backoff_global pushes behind; consecutiveFailures resets on unlock', async () => {
        const resolvers = new Map<string, (res: JobResult<string>) => void>();
        const attempts = new Map<number, number>();
        const { clock, limiter, started } = setup({
            jobs: 3,
            concurrency: 3,
            retryDelayMs: 1_000,
            processJob: (job) => {
                const count = (attempts.get(job.id) ?? 0) + 1;
                attempts.set(job.id, count);
                started.push([job.id, clock.now()]);
                return new Promise<JobResult<string>>((resolve) => {
                    resolvers.set(`${job.id}-${count}`, resolve);
                });
            },
        });

        const runPromise = limiter.start();
        await clock.advance(0);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
        ]);

        resolvers.get('1-1')?.({ ok: false, error: { type: 'retry_backoff', message: 'item-1', retryAfterMs: 100 } });
        await Promise.resolve();
        await Promise.resolve();

        // Lock initiator placed at front of queue.
        resolvers.get('2-1')?.({ ok: false, error: { type: 'backoff_global', message: '429-initiator' } });
        await Promise.resolve();
        await Promise.resolve();

        // Concurrent global failure queued behind lock initiator.
        resolvers.get('3-1')?.({ ok: false, error: { type: 'backoff_global', message: '429-concurrent' } });
        await Promise.resolve();
        await Promise.resolve();

        // Lock initiator executes as sole probe upon lock expiry.
        await clock.advance(1_000);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            [2, 1_000],
        ]);

        // Failed probe increments consecutiveFailures and prepends to queue.
        resolvers.get('2-2')?.({ ok: false, error: { type: 'backoff_global', message: '429-probe-fail' } });
        await clock.advance(2_000);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            [2, 1_000],
            [2, 3_000],
        ]);

        // Successful probe unlocks limiter, resets consecutiveFailures, and drains queue.
        resolvers.get('2-3')?.({ ok: true, data: 'ok 2' });
        await clock.advance(0);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            [2, 1_000],
            [2, 3_000],
            [1, 3_000],
            [3, 3_000],
        ]);

        // Reset on unlock: subsequent global backoff uses initial base delay.
        resolvers.get('3-2')?.({ ok: true, data: 'ok 3' });
        resolvers.get('1-2')?.({ ok: false, error: { type: 'backoff_global', message: 'new-429' } });
        await clock.advance(1_000);
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [3, 0],
            [2, 1_000],
            [2, 3_000],
            [1, 3_000],
            [3, 3_000],
            [1, 4_000],
        ]);

        resolvers.get('1-3')?.({ ok: true, data: 'ok 1' });
        await clock.settle(runPromise);
    });

    it('no stall while locked: when waiting past lockUntil and buffer is full of sleeping retry_backoff items, scheduleNext wakes at earliest retryAt', async () => {
        let job1Calls = 0;
        let job2Calls = 0;
        const { clock, limiter, started, done } = setup({
            jobs: 2,
            concurrency: 2,
            processJob: (job) => {
                started.push([job.id, clock.now()]);
                if (job.id === 1) {
                    job1Calls++;
                    if (job1Calls === 1) {
                        return { ok: false, error: { type: 'backoff_global', message: 'lock 500', retryAfterMs: 500 } };
                    }
                    if (job1Calls === 2) {
                        // Probe fails with retry_backoff: lock stays waiting while buffer sleeps.
                        return {
                            ok: false,
                            error: { type: 'retry_backoff', message: 'item sleep', retryAfterMs: 1_500 },
                        };
                    }
                    return { ok: true, data: 'ok 1' };
                }
                job2Calls++;
                if (job2Calls === 1) {
                    return {
                        ok: false,
                        error: { type: 'retry_backoff', message: 'item 2 sleep', retryAfterMs: 1_200 },
                    };
                }
                return { ok: true, data: 'ok 2' };
            },
        });

        const res = await clock.settle(limiter.start());
        expect(res).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        // At earliest retryAt (t=1200), job 2 probes, succeeds, and unlocks.
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [1, 500],
            [2, 1_200],
            [1, 2_000],
        ]);
        expect(done).toEqual([
            [2, 'ok 2'],
            [1, 'ok 1'],
        ]);
    });

    it('backoff math: large failure counts (> 1024) with retryDelayMs: 0 never produce NaN or Infinity', async () => {
        let calls = 0;
        const { clock, limiter, done } = setup({
            jobs: 1,
            retryDelayMs: 0,
            maxRetries: 2_000,
            processJob: () => {
                calls++;
                if (calls <= 1_100) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'fast retry' } };
                }
                return { ok: true, data: 'survived 1100 retries' };
            },
        });

        const res = await clock.settle(limiter.start());
        expect(res).toEqual({ ok: true, data: { done: 1, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(done).toEqual([[1, 'survived 1100 retries']]);
        expect(calls).toBe(1_101);
    });

    it('fails the run with BUCKET_FAILED when pre-fetch bucket getWaitMs() throws or returns invalid negative wait', async () => {
        const clock = createFakeClock();
        const badBucket = createBucket({
            getWaitMs: () => -5,
        });
        const limiter = createLimiter<Job, string>({
            buckets: [badBucket],
            clock,
            fetchJob: () => ({ job: { id: 1 } }),
            processJob: () => ({ ok: true, data: 'ok' }),
        });

        const res = await clock.settle(limiter.start());
        expect(res.ok).toBe(false);
        if (!res.ok) {
            expect(res.error.code).toBe('BUCKET_FAILED');
        }
    });

    it('rolls back earlier bucket reservations when a subsequent bucket throws during reserve()', async () => {
        const clock = createFakeClock();
        const firstBucket = createRPM(1);
        let shouldThrow = true;
        const secondBucket = createBucket({
            getWaitMs: () => 0,
            reserve: () => {
                if (shouldThrow) {
                    throw new Error('second bucket exploded');
                }
                return undefined;
            },
        });

        const limiter = createLimiter<Job, string>({
            buckets: [firstBucket, secondBucket],
            clock,
            fetchJob: () => ({ job: { id: 1 } }),
            processJob: () => ({ ok: true, data: 'ok' }),
        });

        const res = await clock.settle(limiter.start());
        expect(res.ok).toBe(false);
        if (!res.ok) {
            expect(res.error.code).toBe('BUCKET_FAILED');
        }
        // Rollback on failure: firstBucket retains full capacity.
        shouldThrow = false;
        expect(firstBucket.getWaitMs({ requests: 1 }, 0)).toBe(0);
    });

    it('covers wake() while locked > maxWaitMs, bucket wait > maxWaitMs (pre-fetch & buffered), Case 4a/4c during stop, and timeout rejection', async () => {
        // wake() is no-op when remaining lock duration exceeds maxWaitMs.
        const open1: Job[] = [{ id: 1 }];
        const { clock: c1, limiter: l1 } = setup({
            maxWaitMs: 1_000,
            fetchJob: () => {
                const job = open1.shift();
                return job ? { job } : null;
            },
            processJob: () => ({ ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 5_000 } }),
        });
        await c1.settle(l1.start());
        expect(l1.isIdle()).toBe(true);
        open1.push({ id: 2 });
        l1.wake();
        expect(l1.isIdle()).toBe(true);

        // Pre-fetch check wait exceeds maxWaitMs.
        const c2 = createFakeClock();
        const slowRpm = createRPM(1);
        const open2: FetchedJob<Job>[] = [
            { job: { id: 1 }, cost: { tokens: 100 } },
            { job: { id: 2 }, cost: { tokens: 100 } },
        ];
        const l2 = createLimiter<Job, string>({
            buckets: [slowRpm, createTPM(100)],
            maxWaitMs: 10_000,
            clock: c2,
            fetchJob: () => open2.shift() ?? null,
            processJob: (job) => ({ ok: true, data: `ok ${job.id}` }),
        });
        const res2 = await c2.settle(l2.start());
        expect(res2).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'max_wait_exceeded' },
        });

        // Buffered job cost wait exceeds maxWaitMs.
        const c3 = createFakeClock();
        const tpmBucket = createTPM(100);
        const open3: FetchedJob<Job>[] = [
            { job: { id: 1 }, cost: { tokens: 100 } },
            { job: { id: 2 }, cost: { tokens: 100 } },
        ];
        const l3 = createLimiter<Job, string>({
            buckets: [tpmBucket],
            maxWaitMs: 10_000,
            clock: c3,
            fetchJob: () => open3.shift() ?? null,
            processJob: (job) => ({ ok: true, data: `ok ${job.id}` }),
        });
        const res3 = await c3.settle(l3.start());
        expect(res3).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });

        // Concurrent in-flight backoff_global exceeds maxWaitMs.
        const resolvers = new Map<number, (res: JobResult<string>) => void>();
        const { clock: c4, limiter: l4 } = setup({
            jobs: 3,
            concurrency: 3,
            maxWaitMs: 2_000,
            processJob: (job) =>
                new Promise<JobResult<string>>((resolve) => {
                    resolvers.set(job.id, resolve);
                }),
        });
        const run4 = l4.start();
        await c4.advance(0);
        resolvers.get(1)?.({ ok: false, error: { type: 'backoff_global', message: '429-1', retryAfterMs: 500 } });
        await Promise.resolve();
        // Concurrent failure exceeding maxWaitMs halts limiter.
        resolvers.get(2)?.({ ok: false, error: { type: 'backoff_global', message: '429-2', retryAfterMs: 5_000 } });
        await Promise.resolve();
        // Late failure arriving during stop is released directly.
        resolvers.get(3)?.({ ok: false, error: { type: 'backoff_global', message: '429-3', retryAfterMs: 100 } });
        const res4 = await c4.settle(run4);
        expect(res4).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 3, stoppedBy: 'max_wait_exceeded' },
        });

        // Stale failure arriving post-unlock during stop.
        const resolvers5 = new Map<string, (res: JobResult<string>) => void>();
        const attempts5 = new Map<number, number>();
        const { clock: c5, limiter: l5 } = setup({
            jobs: 2,
            concurrency: 2,
            processJob: (job, { signal }) => {
                const att = (attempts5.get(job.id) ?? 0) + 1;
                attempts5.set(job.id, att);
                return new Promise<JobResult<string>>((resolve) => {
                    signal.addEventListener('abort', () => {
                        resolve({ ok: false, error: { type: 'fatal', message: 'aborted' } });
                    });
                    resolvers5.set(`${job.id}-${att}`, resolve);
                });
            },
        });
        const run5 = l5.start();
        await c5.advance(0);
        resolvers5.get('1-1')?.({ ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 1_000 } });
        await c5.advance(1_000);
        resolvers5.get('1-2')?.({ ok: true, data: 'ok 1' });
        await c5.advance(0);
        const stop5 = l5.stop();
        resolvers5.get('2-1')?.({ ok: false, error: { type: 'backoff_global', message: 'stale 429' } });
        await c5.settle(stop5);
        expect(await run5).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 1, stoppedBy: 'stop' },
        });

        // Timeout rejection during active probe attempt.
        let probeStarted = false;
        const { clock: c6, limiter: l6 } = setup({
            jobs: 1,
            concurrency: 1,
            timeoutMs: 100,
            maxRetries: 0,
            processJob: (_job, { attempt, signal }) => {
                if (attempt === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 1_000 } };
                }
                probeStarted = true;
                return new Promise<JobResult<string>>((_, reject) => {
                    signal.addEventListener('abort', () => reject(new Error('threw on abort')));
                });
            },
        });
        const run6 = l6.start();
        await c6.advance(1_000);
        expect(probeStarted).toBe(true);
        const res6 = await c6.settle(run6);
        expect(res6).toEqual({
            ok: true,
            data: { done: 0, failed: 1, released: 0, stoppedBy: 'empty' },
        });

        // Abort active probe attempt via stop({ abort: true }).
        let probeStarted7 = false;
        const { clock: c7, limiter: l7 } = setup({
            jobs: 1,
            concurrency: 1,
            processJob: (_job, { attempt, signal }) => {
                if (attempt === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 1_000 } };
                }
                probeStarted7 = true;
                return new Promise<JobResult<string>>((resolve) => {
                    signal.addEventListener('abort', () =>
                        resolve({ ok: false, error: { type: 'fatal', message: 'aborted' } }),
                    );
                });
            },
        });
        const run7 = l7.start();
        await c7.advance(1_000);
        expect(probeStarted7).toBe(true);
        await l7.stop({ abort: true });
        expect(await run7).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'stop' },
        });
    });
});

describe('concurrency: Infinity', () => {
    it('starts every job at once when no bucket limits', async () => {
        let running = 0;
        let peak = 0;
        const { clock, limiter, started } = setup({
            jobs: 50,
            concurrency: Number.POSITIVE_INFINITY,
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                running++;
                peak = Math.max(peak, running);
                await sleep(clock, 1_000);
                running--;
                return { ok: true, data: '' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 50, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(peak).toBe(50);
        expect(started.every(([, at]) => at === 0)).toBe(true);
    });

    it('still waits for the buckets', async () => {
        const { clock, limiter, started } = setup({ concurrency: Number.POSITIVE_INFINITY, buckets: [createRPM(2)] });
        await clock.settle(limiter.start());
        expect(started.map(([, at]) => at)).toEqual([0, 0, 61_000, 61_000, 122_000]);
    });

    it('still caps through a shared concurrency bucket', async () => {
        let running = 0;
        let peak = 0;
        const { clock, limiter } = setup({
            jobs: 10,
            concurrency: Number.POSITIVE_INFINITY,
            buckets: [createConcurrencyBucket(3)],
            processJob: async () => {
                running++;
                peak = Math.max(peak, running);
                await sleep(clock, 1_000);
                running--;
                return { ok: true, data: '' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(10);
        expect(peak).toBe(3);
    });

    it('sends a single probe after backoff_global, then starts every waiting job at once', async () => {
        let limited = false;
        const { clock, limiter, started } = setup({
            jobs: 4,
            concurrency: Number.POSITIVE_INFINITY,
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                if (job.id === 1 && !limited) {
                    limited = true;
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 30_000 } };
                }
                await sleep(clock, 1_000);
                return { ok: true, data: '' };
            },
        });
        await clock.settle(limiter.start());
        // Job 2 fetched prior to job 1 failure at t=0; probe executes at t=30000, unlocking jobs 3 and 4.
        expect(started).toEqual([
            [1, 0],
            [2, 0],
            [1, 30_000],
            [3, 31_000],
            [4, 31_000],
        ]);
    });

    it('keeps retrying jobs buffered without a cap and still fetches new ones', async () => {
        const attempts = new Map<number, number>();
        const { clock, limiter, started } = setup({
            jobs: 20,
            concurrency: Number.POSITIVE_INFINITY,
            retryDelayMs: 5_000,
            processJob: async (job) => {
                started.push([job.id, clock.now()]);
                const attempt = (attempts.get(job.id) ?? 0) + 1;
                attempts.set(job.id, attempt);
                if (attempt === 1) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'busy' } };
                }
                return { ok: true, data: '' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.done).toBe(20);
        // All 20 initial attempts dispatch concurrently at t=0.
        expect(started.filter(([, at]) => at === 0)).toHaveLength(20);
    });

    it('reports every running attempt in getStatus', async () => {
        const { clock, limiter } = setup({ jobs: 30, concurrency: Number.POSITIVE_INFINITY });
        const promise = limiter.start();
        await clock.advance(0);
        expect(limiter.getStatus()).toMatchObject({ state: 'running', running: 30 });
        await clock.settle(promise);
        expect(limiter.getStatus()).toMatchObject({ state: 'idle', running: 0 });
    });
});

describe('maxWaitMs of a global lock is a deadline', () => {
    const LOCK_429 = { type: 'backoff_global', message: '429', retryAfterMs: 500 } as const;

    it('stops at the deadline instead of probing again after a retry_backoff wait past it', async () => {
        const started: string[] = [];
        const { clock, limiter, outcomes } = setup({
            jobs: 1,
            concurrency: 1,
            maxWaitMs: 1_000,
            processJob: async (_job, { attempt }) => {
                started.push(`#${attempt}@${clock.now()}`);
                if (attempt === 1) {
                    return { ok: false, error: LOCK_429 };
                }
                if (attempt === 2) {
                    await sleep(clock, 400);
                    return { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 900 } };
                }
                return { ok: true, data: 'ok' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(started).toEqual(['#1@0', '#2@500']);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(outcomes[0][1]).toMatchObject({ status: 'released', error: { code: 'MAX_WAIT_EXCEEDED' } });
    });

    it('lets a probe running at the deadline decide: passing later, it unlocks and the run continues', async () => {
        let attempts = 0;
        const { clock, limiter, started } = setup({
            jobs: 2,
            concurrency: 1,
            maxWaitMs: 1_000,
            processJob: async (job) => {
                attempts++;
                started.push([job.id, clock.now()]);
                if (attempts === 1) {
                    return { ok: false, error: LOCK_429 };
                }
                await sleep(clock, 1_000);
                return { ok: true, data: 'late' };
            },
        });
        const run = limiter.start();
        await clock.advance(1_001);
        expect(limiter.getStatus().state).toBe('probing');
        const result = await clock.settle(run);
        expect(started).toEqual([
            [1, 0],
            [1, 500],
            [2, 1_500],
        ]);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(readLockState(limiter)?.status).toBe('unlocked');
    });

    it('lets a probe running at the deadline decide: failing later, it stops the run when it returns', async () => {
        const { clock, limiter, logs } = setup({
            jobs: 1,
            concurrency: 1,
            maxWaitMs: 1_000,
            processJob: async (_job, { attempt }) => {
                if (attempt === 1) {
                    return { ok: false, error: LOCK_429 };
                }
                await sleep(clock, 1_000);
                return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
            },
        });
        const run = limiter.start();
        await clock.advance(1_499);
        expect(limiter.getStatus().state).toBe('probing');
        await clock.advance(1);
        expect(limiter.getStatus().state).not.toBe('probing');
        const result = await clock.settle(run);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(logs.find((entry) => entry.code === 'MAX_WAIT_EXCEEDED')?.data).toEqual({
            waitMs: 1_500,
            maxWaitMs: 1_000,
        });
    });

    it('stops at once when the next probe certainly starts after the deadline', async () => {
        const { clock, limiter, outcomes } = setup({
            jobs: 1,
            concurrency: 1,
            maxWaitMs: 10_000,
            processJob: (_job, { attempt }) =>
                attempt === 1
                    ? { ok: false, error: { ...LOCK_429, retryAfterMs: 1_000 } }
                    : { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 9_500 } },
        });
        const result = await clock.settle(limiter.start());
        // Probe retry at t=10500 exceeds maxWaitMs deadline (t=10000); limiter stops immediately.
        expect(clock.now()).toBe(1_000);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(outcomes[0][1]).toMatchObject({ status: 'released', error: { code: 'MAX_WAIT_EXCEEDED' } });
    });

    it('checks the deadline when the next retry is due, not earlier, while a job started before the lock still runs', async () => {
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 10_000,
            processJob: async (job, { attempt }) => {
                if (job.id === 2) {
                    await sleep(clock, 20_000);
                    return { ok: true, data: 'slow' };
                }
                return attempt === 1
                    ? { ok: false, error: { ...LOCK_429, retryAfterMs: 1_000 } }
                    : { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 9_500 } };
            },
        });
        const run = limiter.start();
        // Failed probe retries at t=10500; limiter remains locked until retry deadline passes.
        await clock.advance(10_499);
        expect(limiter.getStatus().state).toBe('locked');
        await clock.advance(1);
        expect(limiter.getStatus().state).toBe('stopping');
        expect(await clock.settle(run)).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
    });

    it('stops at the deadline while the probe waits for a concurrency slot held elsewhere', async () => {
        const shared = createConcurrencyBucket(1);
        const { clock, limiter, outcomes } = setup({
            jobs: 1,
            concurrency: 1,
            maxWaitMs: 1_000,
            buckets: [shared],
            processJob: (_job, { attempt }) =>
                attempt === 1 ? { ok: false, error: LOCK_429 } : { ok: true, data: 'ok' },
        });
        let result: unknown;
        void limiter.start().then((value) => {
            result = value;
        });
        await clock.advance(100);
        const held = shared.reserve({ requests: 1 }, clock.now());
        await clock.advance(900);
        expect(limiter.getStatus().state).toBe('locked');
        await clock.advance(1);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
        expect(outcomes[0][1]).toMatchObject({ status: 'released', error: { code: 'MAX_WAIT_EXCEEDED' } });
        shared.settle(held, {}, clock.now());
    });

    it('does not stop early while the probe waits for a concurrency slot that may free before the deadline', async () => {
        const shared = createConcurrencyBucket(2);
        const started: string[] = [];
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 10_000,
            buckets: [shared],
            processJob: async (job, { attempt }) => {
                started.push(`${job.id}#${attempt}@${clock.now()}`);
                if (attempt > 1) {
                    return { ok: true, data: 'ok' };
                }
                if (job.id === 1) {
                    return { ok: false, error: { ...LOCK_429, retryAfterMs: 1_000 } };
                }
                await sleep(clock, 10);
                return { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 9_995 } };
            },
        });
        const run = limiter.start();
        await clock.advance(20);
        const held = [shared.reserve({ requests: 1 }, 20), shared.reserve({ requests: 1 }, 20)];
        // Probe waits for capacity at t=1000; in-flight retry at t=10005 exceeds deadline.
        await clock.advance(1_980);
        expect(limiter.getStatus().state).toBe('locked');
        shared.settle(held[0], {}, clock.now());
        const result = await clock.settle(run);
        expect(started).toEqual(['1#1@0', '2#1@0', '1#2@2000', '2#2@10005']);
        expect(result).toEqual({ ok: true, data: { done: 2, failed: 0, released: 0, stoppedBy: 'empty' } });
        shared.settle(held[1], {}, clock.now());
    });

    it('ends the run as empty when a job started before the lock returns after the deadline with nothing left to probe', async () => {
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 1_000,
            processJob: async (job, { attempt }) => {
                if (job.id === 1) {
                    return attempt === 1
                        ? { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 100 } }
                        : { ok: false, error: { type: 'fatal', message: 'no' } };
                }
                await sleep(clock, 1_500);
                return { ok: true, data: 'ok' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(clock.now()).toBe(1_500);
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 1, released: 0, stoppedBy: 'empty' } });
        expect(limiter.getStatus().lastStoppedBy).toBe('empty');
    });

    it('stops at the deadline instead of letting a job started before the lock probe after it', async () => {
        const started: string[] = [];
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 1_000,
            processJob: async (job, { attempt }) => {
                started.push(`${job.id}#${attempt}@${clock.now()}`);
                await sleep(clock, job.id === 2 && attempt === 1 ? 1_500 : 10);
                if (job.id === 1) {
                    return attempt === 1
                        ? { ok: false, error: { ...LOCK_429, retryAfterMs: 100 } }
                        : { ok: false, error: { type: 'fatal', message: 'x' } };
                }
                return attempt === 1
                    ? { ok: false, error: { type: 'retry_immediate', message: 'x' } }
                    : { ok: true, data: 'ok' };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(started).toEqual(['1#1@0', '2#1@0', '1#2@110']);
        expect(result).toEqual({
            ok: true,
            data: { done: 0, failed: 1, released: 1, stoppedBy: 'max_wait_exceeded' },
        });
    });

    it('lets a backoff_global without retryAfterMs from a job started before the lock leave the lock to the probe', async () => {
        const started: string[] = [];
        const { clock, limiter } = setup({
            jobs: 2,
            concurrency: 2,
            maxWaitMs: 10_000,
            processJob: async (job, { attempt }) => {
                started.push(`${job.id}#${attempt}@${clock.now()}`);
                await sleep(clock, job.id === 2 && attempt === 1 ? 500 : 10);
                if (job.id === 1) {
                    return attempt === 1
                        ? { ok: false, error: { ...LOCK_429, retryAfterMs: 100 } }
                        : { ok: false, error: { type: 'fatal', message: 'x' } };
                }
                return attempt === 1
                    ? { ok: false, error: { type: 'backoff_global', message: '503' } }
                    : { ok: true, data: 'ok' };
            },
        });
        const result = await clock.settle(limiter.start());
        // Lock expires at t=110; 503 at t=500 adds no wait and triggers job 2 probe.
        expect(started).toEqual(['1#1@0', '2#1@0', '1#2@110', '2#2@500']);
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 1, released: 0, stoppedBy: 'empty' } });
    });
});

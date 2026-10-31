// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    type Bucket,
    type Clock,
    createBucket,
    createLimiter,
    createTPD,
    type FetchedJob,
    type JobResult,
    type LogEntry,
    runAll,
    WAIT_FOR_CHANGE,
} from '../../src/index.js';
import { callProcessJob } from '../../src/job-result.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

// Regression tests for analysis report findings.

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

/** Yields entry once, then returns null. */
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

function sleep(clock: FakeClock, ms: number) {
    return new Promise<void>((resolve) => clock.setTimeout(resolve, ms));
}

/** Creates a bucket returning WAIT_FOR_CHANGE for token costs and zero otherwise. */
function createTokenGate(): Bucket {
    return createBucket({
        getWaitMs: (cost) => (cost.tokens ? WAIT_FOR_CHANGE : 0),
        subscribe: () => () => {},
    });
}

describe('3.1 hook order when a backoff_global exceeds maxWaitMs', () => {
    it('3b: calls onAttempt before onJobDone, with the real cost and duration', async () => {
        const clock = createFakeClock();
        const events: string[] = [];
        const infos: unknown[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            maxWaitMs: 5_000,
            fetchJob: source(1).fetchJob,
            processJob: async (): Promise<JobResult<number>> => {
                await sleep(clock, 300);
                return {
                    ok: false,
                    error: { type: 'backoff_global', message: '429', retryAfterMs: 60_000, usage: { tokens: 7 } },
                };
            },
            onAttempt: (job) => {
                events.push(`attempt ${job}`);
            },
            onJobDone: (job, outcome, info) => {
                events.push(`done ${job} ${outcome.status} ${outcome.ok ? '' : outcome.error.code}`);
                infos.push(info);
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data).toEqual({ done: 0, failed: 0, released: 1, stoppedBy: 'max_wait_exceeded' });
        expect(events).toEqual(['attempt 1', 'done 1 released MAX_WAIT_EXCEEDED']);
        expect(infos).toEqual([{ attempt: 1, cost: { requests: 1, tokens: 7 }, durationMs: 300 }]);
    });

    it('3c: a straggler over budget releases the buffered probe first, then gets its own onAttempt and onJobDone', async () => {
        const clock = createFakeClock();
        const events: string[] = [];
        const infos: [number, unknown][] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 2,
            maxWaitMs: 5_000,
            fetchJob: source(2).fetchJob,
            processJob: async (job): Promise<JobResult<number>> => {
                await sleep(clock, job * 100);
                const retryAfterMs = job === 1 ? 1_000 : 60_000;
                return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs } };
            },
            onAttempt: (job) => {
                events.push(`attempt ${job}`);
            },
            onJobDone: (job, outcome, info) => {
                events.push(`done ${job} ${outcome.ok ? '' : outcome.error.code}`);
                infos.push([job, info]);
            },
        });
        await clock.settle(limiter.start());
        expect(events).toEqual(['attempt 1', 'done 1 MAX_WAIT_EXCEEDED', 'attempt 2', 'done 2 MAX_WAIT_EXCEEDED']);
        expect(infos).toEqual([
            // Job 1 attempt info is preserved on release.
            [1, { attempt: 1, cost: { requests: 1 }, durationMs: 100 }],
            [2, { attempt: 1, cost: { requests: 1 }, durationMs: 200 }],
        ]);
    });
});

describe('3.2 runAll forwards onJobDone', () => {
    it('calls the caller onJobDone per item with the original job, and still returns all outcomes', async () => {
        const clock = createFakeClock();
        const seen: [string, string, number][] = [];
        const result = await clock.settle(
            runAll<string, string>([{ job: 'a' }, { job: 'b' }], {
                clock,
                processJob: (job) =>
                    job === 'a' ? { ok: true, data: 'A' } : { ok: false, error: { type: 'fatal', message: 'no' } },
                onJobDone: (job, outcome, info) => {
                    seen.push([job, outcome.status, info.attempt]);
                },
            }),
        );
        expect(seen.sort()).toEqual([
            ['a', 'done', 1],
            ['b', 'failed', 1],
        ]);
        expect(result.ok && result.data.map((item) => item.status)).toEqual(['done', 'failed']);
    });

    it('still records the outcome when the caller onJobDone throws, and rejects a non-function', async () => {
        const result = await runAll([{ job: 1 }], {
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: () => {
                throw new Error('db down');
            },
        });
        expect(result.ok && result.data).toEqual([{ ok: true, status: 'done', data: 1 }]);
        const invalid = await runAll([{ job: 1 }], {
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: 'nope' as never,
        });
        expect(!invalid.ok && invalid.error.code).toBe('INVALID_CONFIG');
    });
});

describe('3.4 HOOK_FAILED names the job', () => {
    it('logs the job, outcome or result, info and error for a throwing onAttempt and onJobDone', async () => {
        const logs: LogEntry[] = [];
        const boom = new Error('db down');
        await createLimiter<number, number>({
            fetchJob: source(1).fetchJob,
            processJob: () => ({ ok: true, data: 5 }),
            onAttempt: () => {
                throw boom;
            },
            onJobDone: () => {
                throw boom;
            },
            log: (entry) => logs.push(entry),
        }).start();
        const failures = logs.filter((entry) => entry.code === 'HOOK_FAILED');
        const info = { attempt: 1, cost: { requests: 1 }, durationMs: expect.any(Number) };
        expect(failures.map((entry) => [entry.message, entry.data])).toEqual([
            ['onAttempt threw.', { job: 1, result: { ok: true, data: 5 }, info, error: boom }],
            ['onJobDone threw.', { job: 1, outcome: { ok: true, status: 'done', data: 5 }, info, error: boom }],
        ]);
    });
});

describe('3.5 limits reached during a stop', () => {
    function stopWhileFailing(error: JobResult<number>, options: { maxRetries?: number; maxWaitMs?: number }) {
        const clock = createFakeClock();
        const outcomes: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            ...options,
            fetchJob: source(1).fetchJob,
            processJob: async () => {
                await sleep(clock, 1_000);
                return error;
            },
            onJobDone: (_job, outcome) => {
                outcomes.push(`${outcome.status} ${outcome.ok ? '' : outcome.error.code}`);
            },
        });
        // Stop during active attempt so result arrives during stopping phase.
        clock.setTimeout(() => void limiter.stop(), 500);
        return clock.settle(limiter.start()).then(() => outcomes);
    }

    it('fails a job that used its last retry, instead of releasing it with a fresh budget', async () => {
        const outcomes = await stopWhileFailing(
            { ok: false, error: { type: 'retry_immediate', message: 'x' } },
            { maxRetries: 0 },
        );
        expect(outcomes).toEqual(['failed RETRIES_EXHAUSTED']);
    });

    it('fails a job whose next retry_backoff wait exceeds maxWaitMs', async () => {
        const outcomes = await stopWhileFailing(
            { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 10_000 } },
            { maxWaitMs: 5_000 },
        );
        expect(outcomes).toEqual(['failed MAX_WAIT_EXCEEDED']);
    });

    it('still releases a job that would only retry', async () => {
        const outcomes = await stopWhileFailing(
            { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 100 } },
            {},
        );
        expect(outcomes).toEqual(['released STOPPED']);
    });
});

describe('4.2 / 4.5 wake() only restarts after an empty run, and returns the status', () => {
    it('starts before the first run and after an empty run', async () => {
        const clock = createFakeClock();
        let fetches = 0;
        const limiter = createLimiter<number, number>({
            clock,
            fetchJob: async () => {
                fetches++;
                await sleep(clock, 10);
                return null;
            },
            processJob: () => ({ ok: true, data: 1 }),
        });
        expect(limiter.wake()).toEqual({
            state: 'running',
            running: 0,
            buffered: 0,
            lockUntil: 0,
            lastStoppedBy: undefined,
        });
        await clock.advance(10);
        expect(limiter.getStatus().lastStoppedBy).toBe('empty');
        expect(limiter.wake().state).toBe('running');
        await clock.advance(10);
        expect(fetches).toBe(2);
    });

    it('stays idle after stop() and after an error until start() is called', async () => {
        const clock = createFakeClock();
        let isBroken = false;
        const limiter = createLimiter<number, number>({
            clock,
            fetchJob: () => {
                if (isBroken) {
                    throw new Error('db down');
                }
                return null;
            },
            processJob: () => ({ ok: true, data: 1 }),
        });
        await limiter.stop();
        expect(limiter.wake()).toMatchObject({ state: 'idle', lastStoppedBy: undefined });

        isBroken = true;
        const failed = await clock.settle(limiter.start());
        expect(!failed.ok && failed.error.code).toBe('FETCH_FAILED');
        isBroken = false;
        expect(limiter.wake()).toMatchObject({ state: 'idle', lastStoppedBy: 'error' });
        expect(limiter.isIdle()).toBe(true);

        await clock.settle(limiter.start());
        expect(limiter.getStatus().lastStoppedBy).toBe('empty');
        expect(limiter.wake().state).toBe('running');
    });

    it('does not lease and release a job on every wake() after a bucket max_wait_exceeded', async () => {
        const clock = createFakeClock();
        const jobs = source(5);
        const released: number[] = [];
        // Exhaust daily token budget so each wait (~24h) exceeds maxWaitMs.
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 60_000,
            buckets: [createTPD(1_000, { used: 1_000 })],
            fetchJob: () => {
                const entry = jobs.fetchJob();
                return entry && { ...entry, cost: { tokens: 10 } };
            },
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: (job, outcome) => {
                if (outcome.status === 'released') {
                    released.push(job);
                }
            },
        });
        const first = await clock.settle(limiter.start());
        expect(first.ok && first.data.stoppedBy).toBe('max_wait_exceeded');
        expect(released).toEqual([1]);
        for (let i = 0; i < 3; i++) {
            expect(limiter.wake().lastStoppedBy).toBe('max_wait_exceeded');
            await clock.advance(1_000);
        }
        expect(jobs.fetches).toBe(1);
        expect(released).toEqual([1]);
    });

    it('returns the idle status of an invalid limiter', () => {
        expect(createLimiter({} as never).wake()).toEqual({
            state: 'idle',
            running: 0,
            buffered: 0,
            lockUntil: 0,
            lastStoppedBy: 'error',
        });
    });
});

describe('5.2 a wait for a change does not hide a timed wait over maxWaitMs', () => {
    it('stops at once when a buffered job waits for a change and about 24 h for a daily bucket', async () => {
        const clock = createFakeClock();
        const released: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 60_000,
            buckets: [createTokenGate(), createTPD(1_000, { used: 1_000 })],
            fetchJob: once({ job: 1, cost: { tokens: 10 } }),
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: (_job, outcome) => {
                released.push(outcome.ok ? 'done' : outcome.error.code);
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.stoppedBy).toBe('max_wait_exceeded');
        expect(released).toEqual(['MAX_WAIT_EXCEEDED']);
        expect(clock.now()).toBe(0);
    });

    it('stops at once in the pre-fetch check, too', async () => {
        const clock = createFakeClock();
        const gate = createBucket({ getWaitMs: () => WAIT_FOR_CHANGE, subscribe: () => () => {} });
        const daily = createBucket({ getWaitMs: () => 86_400_000 });
        let fetches = 0;
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 60_000,
            buckets: [gate, daily],
            fetchJob: () => {
                fetches++;
                return null;
            },
            processJob: () => ({ ok: true, data: 1 }),
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.stoppedBy).toBe('max_wait_exceeded');
        expect(fetches).toBe(0);
    });
});

describe('5.3 bucket waits of a buffered job never add up', () => {
    /** Creates a bucket yielding sequential wait times for costs with special. */
    function createScriptedBucket(waits: number[]): Bucket {
        return createBucket({ getWaitMs: (cost) => (cost.special ? (waits.shift() ?? 0) : 0) });
    }

    it('keeps waiting while each single wait fits maxWaitMs, even when they add up to more than it', async () => {
        const clock = createFakeClock();
        const outcomes: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 1_000,
            buckets: [createScriptedBucket([400, 400, 400, 400])],
            fetchJob: once({ job: 1, cost: { special: 1 } }),
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: (_job, outcome) => {
                outcomes.push(outcome.ok ? 'done' : outcome.error.code);
            },
        });
        const result = await clock.settle(limiter.start());
        // Four 400 ms waits total 1600 ms without any single wait exceeding maxWaitMs (1000 ms).
        expect(result.ok && result.data).toEqual({ done: 1, failed: 0, released: 0, stoppedBy: 'empty' });
        expect(clock.now()).toBe(1_600);
        expect(outcomes).toEqual(['done']);
    });

    it('stops at the first single wait over maxWaitMs, also after shorter ones', async () => {
        const clock = createFakeClock();
        const outcomes: string[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            maxWaitMs: 1_000,
            buckets: [createScriptedBucket([400, 400, 1_200])],
            fetchJob: once({ job: 1, cost: { special: 1 } }),
            processJob: () => ({ ok: true, data: 1 }),
            onJobDone: (_job, outcome) => {
                outcomes.push(outcome.ok ? outcome.status : `${outcome.status} ${outcome.error.code}`);
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result.ok && result.data.stoppedBy).toBe('max_wait_exceeded');
        expect(clock.now()).toBe(800);
        expect(outcomes).toEqual(['released MAX_WAIT_EXCEEDED']);
    });
});

describe('5.4 the backoff exponent counts retry_backoff failures only', () => {
    it('waits retryDelayMs after three retry_immediate failures and a first retry_backoff', async () => {
        const clock = createFakeClock();
        const starts: number[] = [];
        const limiter = createLimiter<number, number>({
            clock,
            retryDelayMs: 1_000,
            fetchJob: source(1).fetchJob,
            processJob: (): JobResult<number> => {
                starts.push(clock.now());
                if (starts.length <= 3) {
                    return { ok: false, error: { type: 'retry_immediate', message: 'x' } };
                }
                if (starts.length === 4) {
                    return { ok: false, error: { type: 'retry_backoff', message: 'x' } };
                }
                return { ok: true, data: 1 };
            },
        });
        await clock.settle(limiter.start());
        expect(starts).toEqual([0, 0, 0, 0, 1_000]);
    });
});

describe('5.8 buckets are copied', () => {
    it('ignores changes to the buckets array after createLimiter', async () => {
        const buckets: Bucket[] = [];
        const limiter = createLimiter<number, number>({
            buckets,
            fetchJob: source(2).fetchJob,
            processJob: () => ({ ok: true, data: 1 }),
        });
        buckets.push(
            createBucket({
                getWaitMs: () => {
                    throw new Error('should never be asked');
                },
            }),
        );
        const result = await limiter.start();
        expect(result.ok && result.data.done).toBe(2);
    });
});

describe('5.9 a throwing clock in callProcessJob', () => {
    it('runs without a timeout when clock.setTimeout throws, and ignores a throwing clearTimeout', async () => {
        const fake = createFakeClock();
        const throwingSet: Clock = {
            now: fake.now,
            setTimeout: () => {
                throw new Error('no timers');
            },
            clearTimeout: fake.clearTimeout,
        };
        expect(
            await callProcessJob(
                { processJob: () => ({ ok: true, data: 1 }), clock: throwingSet, timeoutMs: 100 },
                'job',
                1,
            ),
        ).toEqual({ ok: true, data: 1 });
        const throwingClear: Clock = {
            now: fake.now,
            setTimeout: fake.setTimeout,
            clearTimeout: () => {
                throw new Error('no timers');
            },
        };
        expect(
            await callProcessJob(
                { processJob: () => ({ ok: true, data: 2 }), clock: throwingClear, timeoutMs: 100 },
                'job',
                1,
            ),
        ).toEqual({ ok: true, data: 2 });
    });
});

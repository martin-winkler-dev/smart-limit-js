// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    createConcurrencyBucket,
    createLimiter,
    createRPM,
    createTPM,
    type JobErrorType,
    type JobResult,
} from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

/** Seeded linear congruential generator (LCG) for reproducible runs. */
function createRandom(seed: number): () => number {
    let state = seed;
    return () => {
        state = (state * 1_664_525 + 1_013_904_223) % 4_294_967_296;
        return state / 4_294_967_296;
    };
}

function sleep(clock: FakeClock, ms: number, signal?: AbortSignal) {
    return new Promise<void>((resolve) => {
        const timer = clock.setTimeout(resolve, ms);
        signal?.addEventListener('abort', () => {
            clock.clearTimeout(timer);
            resolve();
        });
    });
}

const ERROR_TYPES: JobErrorType[] = ['backoff_global', 'retry_backoff', 'retry_immediate', 'fatal'];

/** Executes single randomized run with varied limits, outcomes, and mid-run stops. */
async function runRandomScenario(seed: number): Promise<string[]> {
    const random = createRandom(seed);
    function pick(max: number) {
        return Math.floor(random() * max);
    }
    const clock = createFakeClock();
    const concurrency = 1 + pick(4);
    const total = 1 + pick(25);
    const buckets = [createRPM(5 + pick(50)), createTPM(1_000)];
    if (random() < 0.5) {
        buckets.push(createConcurrencyBucket(1 + pick(4)));
    }
    const problems: string[] = [];
    const outcomes = new Map<number, number>();
    const stopInsideAtJob = random() < 0.15 ? pick(total) : -1;
    let fetched = 0;
    let fetching = 0;
    let running = 0;
    const limiter = createLimiter<number, number>({
        clock,
        concurrency,
        buckets,
        maxRetries: pick(4),
        retryDelayMs: pick(3_000),
        maxRetryDelayMs: 20_000,
        maxWaitMs: random() < 0.2 ? Number.POSITIVE_INFINITY : 30_000 + pick(120_000),
        ...(random() < 0.3 ? { timeoutMs: 500 + pick(3_000) } : {}),
        fetchJob: async () => {
            fetching++;
            if (fetching > 1) {
                problems.push('fetchJob called while another fetch was in flight');
            }
            if (random() < 0.3) {
                await sleep(clock, pick(50));
            }
            fetching--;
            if (fetched >= total) {
                return null;
            }
            // Token cost up to 1200 forces some jobs to exceed 1000 TPM limit.
            return { job: fetched++, cost: { tokens: pick(1_200) } };
        },
        processJob: async (_job, { signal }): Promise<JobResult<number>> => {
            running++;
            if (running > concurrency) {
                problems.push(`${running} attempts running with concurrency ${concurrency}`);
            }
            await sleep(clock, pick(4_000), signal);
            running--;
            if (random() < 0.4) {
                return { ok: true, data: 1, usage: { tokens: pick(500) } };
            }
            const type = ERROR_TYPES[pick(ERROR_TYPES.length)];
            const retryAfterMs = random() < 0.4 ? { retryAfterMs: pick(10_000) } : {};
            return { ok: false, error: { type, message: type, ...retryAfterMs } };
        },
        onJobDone: async (job) => {
            outcomes.set(job, (outcomes.get(job) ?? 0) + 1);
            if (job === stopInsideAtJob) {
                // Fire-and-forget stop() to prevent deadlock since limiter awaits onJobDone.
                void limiter.stop({ abort: random() < 0.5 });
            }
        },
    });

    const run = limiter.start();
    let stopped: ReturnType<typeof limiter.stop> | undefined;
    if (random() < 0.2) {
        const abort = random() < 0.5;
        clock.setTimeout(() => {
            stopped = limiter.stop({ abort });
        }, pick(20_000));
    }
    let result: Awaited<typeof run> | undefined;
    try {
        result = await clock.settle(run);
    } catch (error) {
        problems.push(`start() never resolved: ${String(error)}`);
    }
    if (result && !result.ok) {
        problems.push(`run failed with ${result.error.code}`);
    }
    if (result?.ok) {
        const { done, failed, released, stoppedBy } = result.data;
        if (done + failed + released !== fetched) {
            problems.push(`summary ${JSON.stringify(result.data)} does not add up to ${fetched} fetched jobs`);
        }
        if (stoppedBy === 'empty' && released !== 0) {
            problems.push('jobs released although the source ran empty');
        }
        if (stopped && JSON.stringify(await stopped) !== JSON.stringify(result)) {
            problems.push('stop() resolved with a different result than start()');
        }
    }
    for (let job = 0; job < fetched; job++) {
        if (outcomes.get(job) !== 1) {
            problems.push(`job ${job} got onJobDone ${outcomes.get(job) ?? 0} times`);
        }
    }
    if (!limiter.isIdle()) {
        problems.push('limiter not idle after the run');
    }
    return problems;
}

/** Simulates global outage to verify serialized probe attempts until recovery. */
async function runOutageScenario(seed: number): Promise<string[]> {
    const random = createRandom(seed);
    function pick(max: number) {
        return Math.floor(random() * max);
    }
    const clock = createFakeClock();
    const concurrency = 1 + pick(5);
    const recoverAt = 5_000 + pick(60_000);
    const attempts: { start: number; end: number }[] = [];
    let firstFailureAt = Number.POSITIVE_INFINITY;
    const limiter = createLimiter<number, number>({
        clock,
        concurrency,
        maxWaitMs: Number.POSITIVE_INFINITY,
        retryDelayMs: 100 + pick(2_000),
        maxRetryDelayMs: 10_000,
        fetchJob: (() => {
            let next = 0;
            const total = concurrency + pick(10);
            return () => (next < total ? { job: next++ } : null);
        })(),
        processJob: async (): Promise<JobResult<number>> => {
            const attempt = { start: clock.now(), end: 0 };
            attempts.push(attempt);
            await sleep(clock, 1 + pick(3_000));
            attempt.end = clock.now();
            if (attempt.start < recoverAt) {
                firstFailureAt = Math.min(firstFailureAt, attempt.end);
                const retryAfterMs = random() < 0.5 ? { retryAfterMs: pick(5_000) } : {};
                return { ok: false, error: { type: 'backoff_global', message: '503', ...retryAfterMs } };
            }
            return { ok: true, data: 1 };
        },
    });
    const result = await clock.settle(limiter.start());
    const problems: string[] = [];
    if (!result.ok || result.data.failed + result.data.released !== 0) {
        problems.push(`run did not finish every job: ${JSON.stringify(result)}`);
    }
    const probes = attempts
        .filter((attempt) => attempt.start >= firstFailureAt && attempt.start < recoverAt)
        .sort((a, b) => a.start - b.start);
    for (let i = 1; i < probes.length; i++) {
        if (probes[i].start < probes[i - 1].end) {
            problems.push(`two attempts in flight during the outage at ${probes[i].start}`);
        }
    }
    return problems;
}

describe('randomized runs (seeded)', () => {
    it('never hangs, loses, or double-reports a job, and keeps concurrency and serial fetching, also with stop() inside a callback', async () => {
        const failing: string[] = [];
        for (let seed = 1; seed <= 400; seed++) {
            const problems = await runRandomScenario(seed);
            if (problems.length > 0) {
                failing.push(`seed ${seed}: ${[...new Set(problems)].join('; ')}`);
            }
        }
        expect(failing).toEqual([]);
    });

    it('starts only one probe at a time during a global outage', async () => {
        const failing: string[] = [];
        for (let seed = 1; seed <= 200; seed++) {
            const problems = await runOutageScenario(seed);
            if (problems.length > 0) {
                failing.push(`seed ${seed}: ${problems.join('; ')}`);
            }
        }
        expect(failing).toEqual([]);
    });
});

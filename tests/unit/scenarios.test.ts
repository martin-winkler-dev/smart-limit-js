// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    type AttemptInfo,
    createConcurrencyBucket,
    createLimiter,
    createRPM,
    createTPM,
    type FetchedJob,
    type JobOutcome,
} from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

type JobStatus = 'open' | 'leased' | 'done' | 'failed';

type DbJobRow = {
    id: number;
    prompt: string;
    estimatedTokens: number;
    status: JobStatus;
    result?: string;
    errorCode?: string;
    actualTokens?: number;
    attempts: number;
    releaseCount: number;
};

type LlmResponse = {
    text: string;
    totalTokens: number;
};

/** Simulates external job queue database with lease management. */
function createMockDatabase(count: number, defaultTokens = 100) {
    const rows: DbJobRow[] = Array.from({ length: count }, (_, idx) => ({
        id: idx + 1,
        prompt: `Prompt #${idx + 1}`,
        estimatedTokens: defaultTokens,
        status: 'open',
        attempts: 0,
        releaseCount: 0,
    }));

    return {
        rows,
        fetchJob(): FetchedJob<DbJobRow> | null {
            const nextOpen = rows.find((row) => row.status === 'open');
            if (!nextOpen) {
                return null;
            }
            nextOpen.status = 'leased';
            return {
                job: nextOpen,
                cost: { requests: 1, tokens: nextOpen.estimatedTokens },
            };
        },
        onJobDone(job: DbJobRow, outcome: JobOutcome<LlmResponse>, info: AttemptInfo): void {
            job.attempts = info.attempt;
            if (outcome.status === 'done') {
                job.status = 'done';
                job.result = outcome.data.text;
                job.actualTokens = outcome.data.totalTokens;
                job.errorCode = undefined;
                return;
            }
            if (outcome.status === 'failed') {
                job.status = 'failed';
                job.errorCode = outcome.error.code;
                return;
            }
            // Return released job to 'open' state in database.
            job.status = 'open';
            job.errorCode = outcome.error.code;
            job.releaseCount++;
        },
        insertJobs(newCount: number, tokens = defaultTokens): DbJobRow[] {
            const startId = rows.length + 1;
            const added: DbJobRow[] = Array.from({ length: newCount }, (_, idx) => ({
                id: startId + idx,
                prompt: `Prompt #${startId + idx}`,
                estimatedTokens: tokens,
                status: 'open',
                attempts: 0,
                releaseCount: 0,
            }));
            rows.push(...added);
            return added;
        },
        byStatus(status: JobStatus): DbJobRow[] {
            return rows.filter((row) => row.status === status);
        },
    };
}

function sleepWithSignal(clock: FakeClock, ms: number, signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) {
        return Promise.resolve(false);
    }
    return new Promise<boolean>((resolve) => {
        function onAbort() {
            clock.clearTimeout(timer);
            resolve(false);
        }
        const timer = clock.setTimeout(() => {
            signal.removeEventListener('abort', onAbort);
            resolve(true);
        }, ms);
        signal.addEventListener('abort', onAbort, { once: true });
    });
}

describe('End-to-End In-Memory Scenarios', () => {
    it('Scenario 1: Full LLM pipeline with global 429 outage, single-probe recovery, and mixed error types', async () => {
        const clock = createFakeClock();
        const db = createMockDatabase(10, 200);
        const callLog: { id: number; at: number; attempt: number }[] = [];
        const transientFailedOnce = new Set<number>();
        const immediateFailedOnce = new Set<number>();

        const globalOutageUntil = 2_500;

        const limiter = createLimiter<DbJobRow, LlmResponse>({
            concurrency: 3,
            maxRetries: 3,
            retryDelayMs: 1_000,
            clock,
            fetchJob: () => db.fetchJob(),
            processJob: async (job, { attempt, signal }) => {
                callLog.push({ id: job.id, at: clock.now(), attempt });
                const completed = await sleepWithSignal(clock, 100, signal);
                if (!completed) {
                    return { ok: false, error: { type: 'fatal', message: 'aborted' } };
                }

                // Job 4: fatal error (no retries).
                if (job.id === 4) {
                    return {
                        ok: false,
                        error: {
                            type: 'fatal',
                            message: '400 Bad Request: invalid prompt schema',
                            usage: { tokens: 10 },
                        },
                    };
                }

                // Global 429 outage between 100ms and 2500ms.
                const now = clock.now();
                if (now >= 100 && now < globalOutageUntil) {
                    return {
                        ok: false,
                        error: {
                            type: 'backoff_global',
                            message: '429 Too Many Requests: provider overloaded',
                            retryAfterMs: 1_000,
                            usage: { tokens: 0 },
                        },
                    };
                }

                // Job 5: single retry_backoff.
                if (job.id === 5 && !transientFailedOnce.has(5)) {
                    transientFailedOnce.add(5);
                    return {
                        ok: false,
                        error: {
                            type: 'retry_backoff',
                            message: 'Document sandbox temporarily busy',
                            retryAfterMs: 300,
                            usage: { tokens: 20 },
                        },
                    };
                }

                // Job 6: single retry_immediate blip.
                if (job.id === 6 && !immediateFailedOnce.has(6)) {
                    immediateFailedOnce.add(6);
                    return {
                        ok: false,
                        error: {
                            type: 'retry_immediate',
                            message: 'ECONNRESET',
                            usage: { tokens: 0 },
                        },
                    };
                }

                return {
                    ok: true,
                    data: { text: `Answer for #${job.id}`, totalTokens: 150 },
                    usage: { tokens: 150 },
                };
            },
            onJobDone: (job, outcome, info) => db.onJobDone(job, outcome, info),
        });

        const summary = await clock.settle(limiter.start());
        expect(summary).toEqual({
            ok: true,
            data: { done: 9, failed: 1, released: 0, stoppedBy: 'empty' },
        });

        expect(db.byStatus('done').length).toBe(9);
        expect(db.byStatus('failed').map((r) => [r.id, r.errorCode])).toEqual([[4, 'FATAL']]);
        expect(db.byStatus('leased').length).toBe(0);
        expect(db.byStatus('open').length).toBe(0);

        // Outage window: single probes at 1100ms and 2200ms.
        const callsDuringOutage = callLog.filter((c) => c.at > 0 && c.at <= 2_200);
        expect(callsDuringOutage.map((c) => c.at)).toEqual([1_100, 2_200]);

        // Probe at t=3300 succeeds alone; all 3 slots run concurrently at t=3400.
        const callsAt3300 = callLog.filter((c) => c.at === 3_300);
        expect(callsAt3300.length).toBe(1);
        const callsAt3400 = callLog.filter((c) => c.at === 3_400);
        expect(callsAt3400.length).toBe(3);

        expect(globalOutageUntil).toBe(2_500);
    });

    it('Scenario 2: User stop({ abort: true }) mid-flight releases leased jobs back to open, ignores wake(), and resumes cleanly on start()', async () => {
        const clock = createFakeClock();
        const db = createMockDatabase(6, 100);

        const limiter = createLimiter<DbJobRow, LlmResponse>({
            concurrency: 2,
            clock,
            fetchJob: () => db.fetchJob(),
            processJob: async (job, { signal }) => {
                // Job 1 retry_backoff buffers while jobs 2 and 3 run.
                if (job.id === 1 && db.rows[0].releaseCount === 0) {
                    return {
                        ok: false,
                        error: { type: 'retry_backoff', message: 'wait a bit', retryAfterMs: 2_000 },
                    };
                }
                const completed = await sleepWithSignal(clock, 500, signal);
                if (!completed) {
                    return { ok: false, error: { type: 'fatal', message: 'Request aborted by caller' } };
                }
                return {
                    ok: true,
                    data: { text: `Completed #${job.id}`, totalTokens: 80 },
                    usage: { tokens: 80 },
                };
            },
            onJobDone: (job, outcome, info) => db.onJobDone(job, outcome, info),
        });

        const run1Promise = limiter.start();
        await clock.advance(100);

        // At t=100: job 1 buffered for retry; jobs 2 and 3 in-flight.
        expect(db.byStatus('leased').map((r) => r.id)).toEqual([1, 2, 3]);

        const stopResult = await clock.settle(limiter.stop({ abort: true }));
        const run1Result = await run1Promise;
        expect(stopResult).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 3, stoppedBy: 'stop' },
        });
        expect(run1Result).toEqual(stopResult);

        // Buffered and in-flight jobs reset to 'open' in database.
        expect(db.byStatus('leased').length).toBe(0);
        expect(db.byStatus('open').length).toBe(6);
        expect(db.rows.slice(0, 3).map((r) => r.errorCode)).toEqual(['STOPPED', 'STOPPED', 'STOPPED']);

        // wake() is ignored after stop.
        limiter.wake();
        await clock.advance(1_000);
        expect(limiter.isIdle()).toBe(true);
        expect(db.byStatus('done').length).toBe(0);

        // Explicit start() resumes processing.
        const run2Result = await clock.settle(limiter.start());
        expect(run2Result).toEqual({
            ok: true,
            data: { done: 6, failed: 0, released: 0, stoppedBy: 'empty' },
        });
        expect(db.byStatus('done').length).toBe(6);
        expect(db.byStatus('open').length).toBe(0);
        expect(db.byStatus('leased').length).toBe(0);
    });

    it('Scenario 3: Outage exceeding maxWaitMs flushes leased jobs back to open, persists lock across start(), and recovers via start({ force: true })', async () => {
        const clock = createFakeClock();
        const db = createMockDatabase(4, 100);
        let providerDown = true;

        const limiter = createLimiter<DbJobRow, LlmResponse>({
            concurrency: 2,
            maxWaitMs: 5_000,
            clock,
            fetchJob: () => db.fetchJob(),
            processJob: async (job, { signal }) => {
                await sleepWithSignal(clock, 50, signal);
                if (providerDown) {
                    return {
                        ok: false,
                        error: {
                            type: 'backoff_global',
                            message: '503 Service Unavailable: maintenance window',
                            retryAfterMs: 30_000, // Exceeds maxWaitMs.
                        },
                    };
                }
                return {
                    ok: true,
                    data: { text: `Recovered #${job.id}`, totalTokens: 90 },
                    usage: { tokens: 90 },
                };
            },
            onJobDone: (job, outcome, info) => db.onJobDone(job, outcome, info),
        });

        const res1 = await clock.settle(limiter.start());
        expect(res1).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 2, stoppedBy: 'max_wait_exceeded' },
        });
        expect(db.byStatus('leased').length).toBe(0);
        expect(db.byStatus('open').length).toBe(4);
        expect(db.rows.slice(0, 2).map((r) => r.errorCode)).toEqual(['MAX_WAIT_EXCEEDED', 'MAX_WAIT_EXCEEDED']);

        // Remaining lock exceeds maxWaitMs: immediate stop without leasing.
        await clock.advance(1_000);
        const res2 = await clock.settle(limiter.start());
        expect(res2).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 0, stoppedBy: 'max_wait_exceeded' },
        });
        expect(db.byStatus('leased').length).toBe(0);

        // Force start bypasses persisted lock.
        providerDown = false;
        const res3 = await clock.settle(limiter.start({ force: true }));
        expect(res3).toEqual({
            ok: true,
            data: { done: 4, failed: 0, released: 0, stoppedBy: 'empty' },
        });
        expect(db.byStatus('done').length).toBe(4);
        expect(db.byStatus('leased').length).toBe(0);
    });

    it('Scenario 4: Multi-model limiters sharing concurrency, RPM, and TPM buckets with dynamic token settlement', async () => {
        const clock = createFakeClock();
        const fastDb = createMockDatabase(5, 300);
        const proDb = createMockDatabase(5, 400);

        const sharedConcurrency = createConcurrencyBucket(3);
        const sharedRpm = createRPM(6);
        const sharedTpm = createTPM(1_200);

        let inFlightTotal = 0;
        let maxInFlightObserved = 0;
        const completionTimestamps: number[] = [];

        function makeLimiter(db: ReturnType<typeof createMockDatabase>, actualTokensPerJob: number) {
            return createLimiter<DbJobRow, LlmResponse>({
                concurrency: 3,
                buckets: [sharedConcurrency, sharedRpm, sharedTpm],
                clock,
                fetchJob: () => db.fetchJob(),
                processJob: async (job, { signal }) => {
                    inFlightTotal++;
                    maxInFlightObserved = Math.max(maxInFlightObserved, inFlightTotal);
                    await sleepWithSignal(clock, 200, signal);
                    inFlightTotal--;
                    completionTimestamps.push(clock.now());
                    return {
                        ok: true,
                        data: { text: `Model output #${job.id}`, totalTokens: actualTokensPerJob },
                        // Settle with usage below estimate reclaims TPM capacity.
                        usage: { tokens: actualTokensPerJob },
                    };
                },
                onJobDone: (job, outcome, info) => db.onJobDone(job, outcome, info),
            });
        }

        const fastLimiter = makeLimiter(fastDb, 150);
        const proLimiter = makeLimiter(proDb, 200);

        const [fastRes, proRes] = await clock.settle(Promise.all([fastLimiter.start(), proLimiter.start()]));

        expect(fastRes).toEqual({ ok: true, data: { done: 5, failed: 0, released: 0, stoppedBy: 'empty' } });
        expect(proRes).toEqual({ ok: true, data: { done: 5, failed: 0, released: 0, stoppedBy: 'empty' } });

        expect(maxInFlightObserved).toBe(3);

        // RPM(6) permits 6 completions in initial minute; remaining 4 complete after window refills.
        const firstMinuteJobs = completionTimestamps.filter((t) => t < 60_000);
        const secondMinuteJobs = completionTimestamps.filter((t) => t >= 60_000);
        expect(firstMinuteJobs.length).toBe(6);
        expect(secondMinuteJobs.length).toBe(4);

        expect(fastDb.byStatus('done').length).toBe(5);
        expect(proDb.byStatus('done').length).toBe(5);
    });

    it('Scenario 5: Event-driven external queue using wake() both when idle and while actively running', async () => {
        const clock = createFakeClock();
        const db = createMockDatabase(0, 100);

        const limiter = createLimiter<DbJobRow, LlmResponse>({
            concurrency: 2,
            clock,
            fetchJob: () => db.fetchJob(),
            processJob: async (job, { signal }) => {
                await sleepWithSignal(clock, 200, signal);
                return {
                    ok: true,
                    data: { text: `Processed #${job.id}`, totalTokens: 100 },
                    usage: { tokens: 100 },
                };
            },
            onJobDone: (job, outcome, info) => db.onJobDone(job, outcome, info),
        });

        const initialRun = await clock.settle(limiter.start());
        expect(initialRun).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 0, stoppedBy: 'empty' },
        });
        expect(limiter.isIdle()).toBe(true);

        // Wake idle limiter upon new job insertion.
        db.insertJobs(2);
        limiter.wake();
        expect(limiter.isIdle()).toBe(false);

        // Wake active limiter upon additional job insertion.
        await clock.advance(100);
        expect(db.byStatus('leased').map((r) => r.id)).toEqual([1, 2]);
        db.insertJobs(2);
        limiter.wake();

        await clock.advance(300);
        expect(limiter.isIdle()).toBe(true);
        expect(db.byStatus('done').map((r) => r.id)).toEqual([1, 2, 3, 4]);
        expect(db.byStatus('leased').length).toBe(0);
    });
});

// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createQueue, createRPM, createTPM, type JobOutcome, type JobResult, type LogEntry } from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';
import { isObject } from '../../src/util.js';

/** Fake clock tracking active timer count. */
function createTrackedClock(): FakeClock & { activeTimers: () => number } {
    const clock = createFakeClock();
    const active = new Set<unknown>();
    return {
        ...clock,
        setTimeout(callback, ms) {
            const timer = clock.setTimeout(() => {
                active.delete(timer);
                callback();
            }, ms);
            active.add(timer);
            return timer;
        },
        clearTimeout(timer) {
            active.delete(timer);
            clock.clearTimeout(timer);
        },
        activeTimers: () => active.size,
    };
}

function sleep(clock: FakeClock, ms: number): Promise<void> {
    return new Promise((resolve) => clock.setTimeout(resolve, ms));
}

function summary(outcome: JobOutcome<unknown>): string {
    return outcome.ok ? `${outcome.status} ${String(outcome.data)}` : `${outcome.status} ${outcome.error.code}`;
}

describe('createQueue', () => {
    it('resolves each push with its outcome and starts jobs in push order', async () => {
        const clock = createFakeClock();
        const started: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            concurrency: 2,
            processJob: async (job) => {
                started.push(job);
                await sleep(clock, job === 'a' ? 300 : 100);
                return job === 'bad'
                    ? { ok: false, error: { type: 'fatal', message: 'no' } }
                    : { ok: true, data: job.toUpperCase() };
            },
        });
        const pushed = ['a', 'b', 'bad', 'c', 'd'].map((job) => queue.push(job));
        const outcomes = await clock.settle(Promise.all(pushed));
        expect(outcomes.map(summary)).toEqual(['done A', 'done B', 'failed FATAL', 'done C', 'done D']);
        expect(started).toEqual(['a', 'b', 'bad', 'c', 'd']);
        expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0, lastStoppedBy: 'empty' });
    });

    it('ends the run as soon as nothing is left, without waiting for room in a full window, and starts again on push', async () => {
        const clock = createTrackedClock();
        const rpm = createRPM(2);
        const queue = createQueue<number, number>({
            clock,
            buckets: [rpm],
            processJob: (job) => ({ ok: true, data: job }),
        });
        await clock.settle(Promise.all([queue.push(1), queue.push(2)]));
        expect(clock.now()).toBe(0);
        expect(queue.getStatus().state).toBe('idle');
        expect(clock.activeTimers()).toBe(0);

        // Window saturated: next push waits for available capacity.
        const third = queue.push(3);
        expect(summary(await clock.settle(third))).toBe('done 3');
        expect(clock.now()).toBe(60_000);
        expect(queue.getStatus().state).toBe('idle');
        expect(clock.activeTimers()).toBe(0);
    });

    it('sends a single probe after backoff_global, also for jobs pushed during the lock', async () => {
        const clock = createFakeClock();
        const calls: string[] = [];
        let failed = false;
        const queue = createQueue<string, string>({
            clock,
            concurrency: 3,
            processJob: async (job) => {
                calls.push(`${job}@${clock.now()}`);
                await sleep(clock, 100);
                if (job === 'a' && !failed) {
                    failed = true;
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 1000 } };
                }
                return { ok: true, data: job };
            },
        });
        const a = queue.push('a');
        await clock.advance(500);
        expect(queue.getStatus().state).toBe('locked');
        const others = ['b', 'c', 'd'].map((job) => queue.push(job));
        const outcomes = await clock.settle(Promise.all([a, ...others]));
        expect(outcomes.map(summary)).toEqual(['done a', 'done b', 'done c', 'done d']);
        // Job 'a' locks t=100..1100 and probes alone; 'b', 'c', 'd' dispatch upon unlock at t=1200.
        expect(calls).toEqual(['a@0', 'a@1100', 'b@1200', 'c@1200', 'd@1200']);
    });

    it('a stop for maxWaitMs releases every pushed job; the next push starts again, and the lock stays', async () => {
        const clock = createFakeClock();
        const calls: string[] = [];
        let hasQuota = false;
        const queue = createQueue<string, string>({
            clock,
            concurrency: 1,
            maxWaitMs: 5000,
            processJob: async (job) => {
                calls.push(job);
                await sleep(clock, 10);
                return hasQuota
                    ? { ok: true, data: job }
                    : { ok: false, error: { type: 'backoff_global', message: 'quota', retryAfterMs: 60_000 } };
            },
        });
        const first = ['a', 'b', 'c'].map((job) => queue.push(job));
        const outcomes = await clock.settle(Promise.all(first));
        expect(outcomes.map(summary)).toEqual([
            'released MAX_WAIT_EXCEEDED',
            'released MAX_WAIT_EXCEEDED',
            'released MAX_WAIT_EXCEEDED',
        ]);
        // Only job initiating maxWaitMs stop retains cause error.
        expect(!outcomes[0].ok && outcomes[0].error.cause).toMatchObject({ message: 'quota' });
        expect(!outcomes[1].ok && outcomes[1].error.cause).toBeUndefined();
        expect(queue.getStatus()).toMatchObject({
            state: 'idle',
            lastStoppedBy: 'max_wait_exceeded',
            lockUntil: 60_010,
        });

        // Lock duration exceeds maxWaitMs: immediate release without dispatch.
        expect(summary(await clock.settle(queue.push('d')))).toBe('released MAX_WAIT_EXCEEDED');
        expect(calls).toEqual(['a']);

        // Lock within maxWaitMs: subsequent push executes probe.
        await clock.advance(56_000);
        hasQuota = true;
        const later = ['e', 'f'].map((job) => queue.push(job));
        expect((await clock.settle(Promise.all(later))).map(summary)).toEqual(['done e', 'done f']);
        expect(calls).toEqual(['a', 'e', 'f']);
    });

    it('ends an aborted job at once wherever it waits, and never runs it again', async () => {
        const clock = createFakeClock();
        const calls: string[] = [];
        const signals = new Map<string, AbortSignal>();
        const queue = createQueue<string, string>({
            clock,
            concurrency: 1,
            processJob: async (job, { signal }) => {
                calls.push(job);
                signals.set(job, signal);
                await sleep(clock, 1000);
                if (job === 'retry' && calls.filter((call) => call === 'retry').length === 1) {
                    return { ok: false, error: { type: 'retry_backoff', message: '500', retryAfterMs: 5000 } };
                }
                return { ok: true, data: job };
            },
        });
        const controllers = new Map(['running', 'retry', 'waiting'].map((job) => [job, new AbortController()]));
        const pushed = new Map(
            ['running', 'retry', 'waiting', 'last'].map((job) => [
                job,
                queue.push(job, { signal: controllers.get(job)?.signal }),
            ]),
        );
        const results = new Map<string, string>();
        for (const [job, promise] of pushed) {
            void promise.then((outcome) => results.set(job, `${summary(outcome)}@${clock.now()}`));
        }

        // In-flight: aborted immediately, reflecting in processJob signal.
        await clock.advance(500);
        controllers.get('running')?.abort(new Error('caller gone'));
        await clock.advance(0);
        expect(results.get('running')).toBe('released ABORTED@500');
        expect(signals.get('running')?.aborted).toBe(true);
        expect(await pushed.get('running')).toMatchObject({ error: { cause: { message: 'caller gone' } } });

        // Queued: aborted immediately before execution.
        controllers.get('waiting')?.abort();
        await clock.advance(0);
        expect(results.get('waiting')).toBe('released ABORTED@500');

        // Awaiting retry: aborted without second attempt while 'last' claims slot.
        await clock.advance(2500);
        expect(calls).toEqual(['running', 'retry', 'last']);
        controllers.get('retry')?.abort();
        await clock.advance(0);
        expect(results.get('retry')).toBe('released ABORTED@3000');

        expect(summary(await clock.settle(pushed.get('last') as Promise<JobOutcome<string>>))).toBe('done last');
        expect(calls).toEqual(['running', 'retry', 'last']);
        expect(queue.getStatus().state).toBe('idle');
    });

    it('ends an aborted job that waits in the buffer for room in a bucket at once, without booking it', async () => {
        const clock = createFakeClock();
        const tpm = createTPM(100);
        const calls: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            concurrency: 2,
            buckets: [tpm],
            processJob: (job) => {
                calls.push(`${job}@${clock.now()}`);
                return { ok: true, data: job };
            },
        });
        const controller = new AbortController();
        const a = queue.push('a', { cost: { tokens: 80 } });
        const b = queue.push('b', { cost: { tokens: 80 }, signal: controller.signal });
        await clock.advance(1000);
        expect(queue.getStatus()).toMatchObject({ buffered: 1, waiting: 0 });
        controller.abort();
        expect(summary(await clock.settle(b))).toBe('released ABORTED');
        expect(clock.now()).toBe(1000);
        expect(summary(await a)).toBe('done a');
        expect(tpm.getWaitMs({ tokens: 20 }, clock.now())).toBe(0);
        expect(summary(await clock.settle(queue.push('c', { cost: { tokens: 20 } })))).toBe('done c');
        expect(calls).toEqual(['a@0', 'c@1000']);
    });

    it('releases a job pushed with an aborted signal at once', async () => {
        const queue = createQueue<string, string>({ processJob: (job) => ({ ok: true, data: job }) });
        const outcome = await queue.push('a', { signal: AbortSignal.abort('gone') });
        expect(outcome).toMatchObject({ ok: false, status: 'released', error: { code: 'ABORTED', cause: 'gone' } });
        expect(queue.getStatus().state).toBe('idle');
    });

    it('ends the run at once when the job a lock waits for is aborted', async () => {
        const clock = createTrackedClock();
        const queue = createQueue<string, string>({
            clock,
            processJob: () => ({ ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 10_000 } }),
        });
        const controller = new AbortController();
        const pushed = queue.push('a', { signal: controller.signal });
        await clock.advance(100);
        expect(queue.getStatus().state).toBe('locked');
        controller.abort();
        expect(summary(await clock.settle(pushed))).toBe('released ABORTED');
        await clock.advance(0);
        expect(clock.now()).toBe(100);
        expect(queue.getStatus()).toMatchObject({ state: 'idle', lastStoppedBy: 'empty', lockUntil: 10_000 });
        expect(clock.activeTimers()).toBe(0);
    });

    it('ends the run at once when the last waiting job is aborted during a bucket wait (Gemini4 21)', async () => {
        const clock = createTrackedClock();
        const queue = createQueue<string, string>({
            clock,
            concurrency: 2,
            buckets: [createRPM(1)],
            processJob: (job) => ({ ok: true, data: job }),
        });
        expect(summary(await clock.settle(queue.push('a')))).toBe('done a');
        const controller = new AbortController();
        const pushed = queue.push('b', { signal: controller.signal });
        await clock.advance(100);
        expect(queue.getStatus()).toMatchObject({ state: 'running', waiting: 1 });
        controller.abort();
        expect(summary(await clock.settle(pushed))).toBe('released ABORTED');
        await clock.advance(0);
        expect(clock.now()).toBe(100);
        expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0, lastStoppedBy: 'empty' });
        expect(clock.activeTimers()).toBe(0);
    });

    it('ends the run at once when the probe and the waiting job of a lock are aborted (Gemini4 21)', async () => {
        const clock = createTrackedClock();
        const queue = createQueue<string, string>({
            clock,
            processJob: () => ({ ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 10_000 } }),
        });
        const probe = new AbortController();
        const other = new AbortController();
        const pushed = [queue.push('a', { signal: probe.signal })];
        await clock.advance(100);
        pushed.push(queue.push('b', { signal: other.signal }));
        await clock.advance(100);
        expect(queue.getStatus()).toMatchObject({ state: 'locked', waiting: 1 });
        probe.abort();
        expect(summary(await clock.settle(pushed[0]))).toBe('released ABORTED');
        // Probe abort re-arms lock timer for job 'b'.
        expect(queue.getStatus()).toMatchObject({ state: 'locked', waiting: 1 });
        other.abort();
        expect(summary(await clock.settle(pushed[1]))).toBe('released ABORTED');
        await clock.advance(0);
        expect(clock.now()).toBe(200);
        expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0, lastStoppedBy: 'empty' });
        expect(clock.activeTimers()).toBe(0);
    });

    it('stop() releases every pushed job with STOPPED, lets the running one finish, and a later push starts again', async () => {
        const clock = createFakeClock();
        const queue = createQueue<string, string>({
            clock,
            concurrency: 1,
            processJob: async (job) => {
                await sleep(clock, 1000);
                return { ok: true, data: job };
            },
        });
        const first = ['a', 'b', 'c'].map((job) => queue.push(job));
        await clock.advance(100);
        // Jobs 'b' and 'c' remain queued awaiting slot capacity.
        expect(queue.getStatus()).toMatchObject({ running: 1, buffered: 0, waiting: 2 });
        const stopped = queue.stop();
        // Job pushed during stopping phase executes in subsequent run.
        const late = queue.push('late');
        expect((await clock.settle(Promise.all(first))).map(summary)).toEqual([
            'done a',
            'released STOPPED',
            'released STOPPED',
        ]);
        expect(await clock.settle(stopped)).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'stop' },
        });
        expect(summary(await clock.settle(late))).toBe('done late');
        expect(summary(await clock.settle(queue.push('after')))).toBe('done after');
    });

    it('fails a job with an invalid cost or signal, and releases every push when the options are invalid', async () => {
        const queue = createQueue<string, string>({ processJob: (job) => ({ ok: true, data: job }) });
        expect(summary(await queue.push('a', { cost: { tokens: -1 } }))).toBe('failed INVALID_COST');
        expect(summary(await queue.push('a', { signal: 'no' as never }))).toBe('failed INVALID_CONFIG');

        const invalid = createQueue<string, string>({ concurrency: 0, processJob: (job) => ({ ok: true, data: job }) });
        expect(summary(await invalid.push('a'))).toBe('released INVALID_CONFIG');
        expect(summary(await invalid.push('b'))).toBe('released INVALID_CONFIG');

        const throwing = createQueue<string, string>({
            get processJob(): () => JobResult<string> {
                throw new Error('getter failed');
            },
        });
        expect(await throwing.push('a')).toMatchObject({ status: 'released', error: { code: 'INVALID_CONFIG' } });
        expect(await throwing.stop()).toMatchObject({ ok: false, error: { code: 'INVALID_CONFIG' } });
    });

    it('fails a push whose options throw while read with INVALID_CONFIG, and the queue keeps working', async () => {
        const queue = createQueue<string, string>({ processJob: (job) => ({ ok: true, data: job }) });
        const options = {
            get cost(): never {
                throw new Error('getter failed');
            },
        };
        expect(await queue.push('a', options)).toMatchObject({
            status: 'failed',
            error: { code: 'INVALID_CONFIG', cause: expect.any(Error) },
        });
        expect(summary(await queue.push('b'))).toBe('done b');
    });

    it('releases a job aborted while the limiter fetches it, without running it', async () => {
        const clock = createFakeClock();
        const started: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            processJob: (job) => {
                started.push(job);
                return { ok: true, data: job };
            },
        });
        const controller = new AbortController();
        // Abort arrives before fetchJob resolves.
        const pushed = queue.push('a', { signal: controller.signal });
        controller.abort('gone');
        expect(await clock.settle(pushed)).toMatchObject({
            status: 'released',
            error: { code: 'ABORTED', cause: 'gone' },
        });
        expect(started).toEqual([]);
        expect(summary(await clock.settle(queue.push('b')))).toBe('done b');
        expect(started).toEqual(['b']);
    });

    it('releases a running job aborted while the queue stops with ABORTED, and the stop still finishes', async () => {
        const clock = createFakeClock();
        const signals: AbortSignal[] = [];
        const queue = createQueue<string, string>({
            clock,
            processJob: async (job, { signal }) => {
                signals.push(signal);
                await sleep(clock, 100);
                return { ok: true, data: job };
            },
        });
        const controller = new AbortController();
        const pushed = queue.push('a', { signal: controller.signal });
        await clock.advance(0);
        expect(queue.getStatus().running).toBe(1);
        const stopped = queue.stop();
        controller.abort('gone');
        expect(signals[0].aborted).toBe(true);
        expect(await clock.settle(pushed)).toMatchObject({
            status: 'released',
            error: { code: 'ABORTED', cause: 'gone' },
        });
        expect(await clock.settle(stopped)).toMatchObject({ ok: true, data: { done: 1, stoppedBy: 'stop' } });
        expect(queue.getStatus()).toMatchObject({ state: 'idle', running: 0, waiting: 0 });
    });

    it('hands the probe to the next job when the caller aborts the probe, without retry warning or failure', async () => {
        const clock = createFakeClock();
        const logs: LogEntry[] = [];
        const started: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            maxWaitMs: 10_000,
            log: (entry) => logs.push(entry),
            processJob: async (job, { attempt, signal }): Promise<JobResult<string>> => {
                started.push(`${job}#${attempt}@${clock.now()}`);
                if (job === 'a' && attempt === 1) {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 500 } };
                }
                if (job === 'a') {
                    await new Promise((resolve) => signal.addEventListener('abort', resolve));
                    return { ok: false, error: { type: 'retry_immediate', message: 'aborted' } };
                }
                return { ok: true, data: job };
            },
        });
        const controller = new AbortController();
        const a = queue.push('a', { signal: controller.signal });
        await clock.advance(100);
        // Pushed during lock: awaits probe completion.
        const b = queue.push('b');
        await clock.advance(600);
        controller.abort('gone');
        expect((await clock.settle(Promise.all([a, b]))).map(summary)).toEqual(['released ABORTED', 'done b']);
        expect(started).toEqual(['a#1@0', 'a#2@500', 'b#1@700']);
        expect(logs.filter((entry) => entry.level !== 'log').map((entry) => entry.message)).toEqual([
            'Global backoff triggered (429), pausing for 500 ms.',
        ]);
        expect(logs.at(-1)?.data).toEqual({ done: 1, failed: 0, released: 1, stoppedBy: 'empty' });
    });

    it('counts a caller-aborted job whose processJob throws or returns fatal as released, without an error log', async () => {
        for (const ending of ['throw', 'fatal'] as const) {
            const clock = createFakeClock();
            const logs: LogEntry[] = [];
            const queue = createQueue<string, string>({
                clock,
                log: (entry) => logs.push(entry),
                processJob: async (_job, { signal }): Promise<JobResult<string>> => {
                    await new Promise((resolve) => signal.addEventListener('abort', resolve));
                    if (ending === 'throw') {
                        signal.throwIfAborted();
                    }
                    return { ok: false, error: { type: 'fatal', message: 'aborted' } };
                },
            });
            const controller = new AbortController();
            const pushed = queue.push('a', { signal: controller.signal });
            await clock.advance(100);
            controller.abort('gone');
            expect(summary(await clock.settle(pushed))).toBe('released ABORTED');
            await clock.advance(0);
            expect(logs.filter((entry) => entry.level === 'error')).toEqual([]);
            expect(logs.at(-1)?.data).toEqual({ done: 0, failed: 0, released: 1, stoppedBy: 'empty' });
        }
    });

    it('releases a job pushed while the run stops with the next stop(), and never starts it', async () => {
        const clock = createFakeClock();
        const started: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            processJob: async (job) => {
                started.push(job);
                await sleep(clock, 1_000);
                return { ok: true, data: job };
            },
        });
        const a = queue.push('a');
        await clock.advance(100);
        void queue.stop();
        const b = queue.push('b');
        expect(await clock.settle(queue.stop())).toEqual({
            ok: true,
            data: { done: 1, failed: 0, released: 0, stoppedBy: 'stop' },
        });
        expect((await Promise.all([a, b])).map(summary)).toEqual(['done a', 'released STOPPED']);
        await clock.advance(5_000);
        expect(started).toEqual(['a']);
        expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0 });
    });

    it('releases a job pushed while the run stops for maxWaitMs with a following stop()', async () => {
        const clock = createFakeClock();
        const started: string[] = [];
        const queue = createQueue<string, string>({
            clock,
            maxWaitMs: 1_000,
            processJob: async (job): Promise<JobResult<string>> => {
                started.push(job);
                if (job === 'a') {
                    return { ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 5_000 } };
                }
                await sleep(clock, 1_000);
                return { ok: true, data: job };
            },
        });
        const b = queue.push('b');
        const a = queue.push('a');
        await clock.advance(0);
        expect(queue.getStatus().state).toBe('stopping');
        const c = queue.push('c');
        await clock.settle(queue.stop());
        const outcomes = await clock.settle(Promise.all([a, b, c]));
        expect(outcomes.map(summary)).toEqual(['released MAX_WAIT_EXCEEDED', 'done b', 'released STOPPED']);
        expect(started).toEqual(['b', 'a']);
    });

    it('logs the pushed job, not its internal wrapper, in Job failed, HOOK_FAILED and the hang warning', async () => {
        const clock = createFakeClock();
        const logs: LogEntry[] = [];
        const queue = createQueue<string, string>({
            clock,
            timeoutMs: 100,
            maxRetries: 0,
            processJob: async () => {
                await sleep(clock, 500);
                return { ok: false, error: { type: 'fatal', message: 'no' } };
            },
            onAttempt: () => {
                throw new Error('hook');
            },
            log: (entry) => logs.push(entry),
        });
        await clock.settle(queue.push('a'));
        const jobs = logs
            .filter((entry) => isObject(entry.data) && 'job' in entry.data)
            .map((entry) => [entry.code ?? entry.level, (entry.data as { job: unknown }).job]);
        expect(jobs).toEqual([
            ['warn', 'a'],
            ['HOOK_FAILED', 'a'],
            ['RETRIES_EXHAUSTED', 'a'],
        ]);
    });

    it('passes onAttempt the pushed job', async () => {
        const seen: string[] = [];
        const queue = createQueue<string, string>({
            processJob: (job) => ({ ok: true, data: job }),
            onAttempt: (job, result, info) => {
                seen.push(`${job} ${result.ok} ${info.attempt}`);
            },
        });
        await queue.push('a');
        expect(seen).toEqual(['a true 1']);
    });

    it('starts every pushed job at once in push order with concurrency Infinity', async () => {
        const clock = createTrackedClock();
        const started: [number, number][] = [];
        const queue = createQueue<number, number>({
            clock,
            concurrency: Number.POSITIVE_INFINITY,
            processJob: async (job) => {
                started.push([job, clock.now()]);
                await sleep(clock, 100);
                return { ok: true, data: job };
            },
        });
        const jobs = Array.from({ length: 40 }, (_, index) => index);
        const outcomes = await clock.settle(Promise.all(jobs.map((job) => queue.push(job))));
        expect(outcomes.map(summary)).toEqual(jobs.map((job) => `done ${job}`));
        expect(started).toEqual(jobs.map((job) => [job, 0]));
        expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0, running: 0 });
        expect(clock.activeTimers()).toBe(0);
    });

    it.each([
        ['another job waits', ['a', 'b']],
        ['it is the only job', ['a']],
    ])(
        'ends the run and starts the next job when a job aborted during onAttempt is released after it, when %s',
        async (_name, jobs) => {
            const clock = createTrackedClock();
            const controller = new AbortController();
            const started: string[] = [];
            const queue = createQueue<string, string>({
                clock,
                concurrency: 1,
                processJob: (job, { attempt }) => {
                    started.push(`${job}#${attempt}@${clock.now()}`);
                    return job === 'a'
                        ? { ok: false, error: { type: 'retry_immediate', message: 'x' } }
                        : { ok: true, data: job };
                },
                onAttempt: async () => {
                    await sleep(clock, 100);
                },
            });
            const pushed = jobs.map((job) => queue.push(job, job === 'a' ? { signal: controller.signal } : {}));
            await clock.advance(50);
            controller.abort();
            await clock.advance(1_000);
            expect((await Promise.all(pushed)).map(summary)).toEqual(
                jobs.map((job) => (job === 'a' ? 'released ABORTED' : `done ${job}`)),
            );
            expect(started).toEqual(jobs.map((job) => `${job}#1@${job === 'a' ? 0 : 100}`));
            expect(queue.getStatus()).toMatchObject({ state: 'idle', waiting: 0, running: 0 });
            expect(clock.activeTimers()).toBe(0);
        },
    );
});

describe('createQueue priority', () => {
    /** Creates queue holding slot until release trigger is invoked. */
    function createHeldQueue() {
        const clock = createFakeClock();
        const started: string[] = [];
        let release: (() => void) | undefined;
        const queue = createQueue<string, string>({
            clock,
            concurrency: 1,
            processJob: async (job) => {
                started.push(job);
                if (job === 'hold') {
                    await new Promise<void>((resolve) => {
                        release = resolve;
                    });
                }
                return { ok: true, data: job };
            },
        });
        const held = queue.push('hold');
        return { clock, queue, started, held, release: () => release?.() };
    }

    it.each([
        ['higher priority first', [0, 0, 1], ['C', 'A', 'B']],
        ['equal priority keeps push order', [1, 1, 1], ['A', 'B', 'C']],
        ['default is 0', [undefined, -1, 0], ['A', 'C', 'B']],
        ['fast path keeps order', [2, 1, 0], ['A', 'B', 'C']],
        ['insert in the middle', [2, 0, 1], ['A', 'C', 'B']],
    ] as const)('starts waiting jobs by priority: %s', async (_name, priorities, expected) => {
        const { clock, queue, started, held, release } = createHeldQueue();
        await clock.advance(0);
        const pushed = ['A', 'B', 'C'].map((job, index) => queue.push(job, { priority: priorities[index] }));
        expect(queue.getStatus().waiting).toBe(3);
        release();
        await clock.settle(Promise.all([held, ...pushed]));
        expect(started).toEqual(['hold', ...expected]);
    });

    it.each([
        // Buffer size 1 holds 'A' retry, preventing fetches during backoff.
        ['buffer of 1', 1, ['A@0', 'A@1000', 'Y@1010', 'X@1020']],
        // 'X' claims free slot during backoff; 'A' retry precedes 'Y' when slot frees.
        ['buffer of 2', 2, ['A@0', 'X@100', 'A@1600', 'Y@1610']],
        // Unbounded queue buffer behaves identically to buffer size 2.
        ['default buffer', undefined, ['A@0', 'X@100', 'A@1600', 'Y@1610']],
    ] as const)(
        'keeps a retry in the limiter ahead of higher-priority waiting jobs (%s)',
        async (_name, maxBufferSize, expected) => {
            const clock = createFakeClock();
            const started: string[] = [];
            let failed = false;
            const queue = createQueue<string, string>({
                clock,
                concurrency: 1,
                maxBufferSize,
                processJob: async (job) => {
                    started.push(`${job}@${clock.now()}`);
                    if (job === 'A' && !failed) {
                        failed = true;
                        return { ok: false, error: { type: 'retry_backoff', message: 'x', retryAfterMs: 1000 } };
                    }
                    await sleep(clock, job === 'X' ? 1500 : 10);
                    return { ok: true, data: job };
                },
            });
            const a = queue.push('A');
            await clock.advance(100);
            const x = queue.push('X', { priority: 5 });
            await clock.advance(100);
            const y = queue.push('Y', { priority: 9 });
            const outcomes = await clock.settle(Promise.all([a, x, y]));
            expect(outcomes.map(summary)).toEqual(['done A', 'done X', 'done Y']);
            expect(started).toEqual(expected);
        },
    );

    it.each([Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, '1', {}])(
        'fails a push with priority %p with INVALID_CONFIG without running it',
        async (priority) => {
            let calls = 0;
            const queue = createQueue<string, string>({
                processJob: (job) => {
                    calls++;
                    return { ok: true, data: job };
                },
            });
            expect(summary(await queue.push('a', { priority: priority as number }))).toBe('failed INVALID_CONFIG');
            expect(calls).toBe(0);
        },
    );

    it('fails a push whose priority getter throws with INVALID_CONFIG', async () => {
        const queue = createQueue<string, string>({ processJob: (job) => ({ ok: true, data: job }) });
        const options = {
            get priority(): number {
                throw new Error('getter failed');
            },
        };
        expect(await queue.push('a', options)).toMatchObject({
            status: 'failed',
            error: { code: 'INVALID_CONFIG', cause: expect.any(Error) },
        });
        expect(summary(await queue.push('b'))).toBe('done b');
    });

    it('removes an aborted high-priority waiting job, and the next job starts', async () => {
        const { clock, queue, started, held, release } = createHeldQueue();
        await clock.advance(0);
        const controller = new AbortController();
        const low = queue.push('low');
        const high = queue.push('high', { priority: 3, signal: controller.signal });
        controller.abort();
        expect(summary(await high)).toBe('released ABORTED');
        expect(queue.getStatus().waiting).toBe(1);
        release();
        await clock.settle(Promise.all([held, low]));
        expect(started).toEqual(['hold', 'low']);
    });

    it('stop() releases waiting jobs of every priority with STOPPED', async () => {
        const { clock, queue, held, release } = createHeldQueue();
        await clock.advance(0);
        const pushed = [-1, 0, 2].map((priority) => queue.push(`p${priority}`, { priority }));
        const stopped = queue.stop();
        release();
        expect((await clock.settle(Promise.all(pushed))).map(summary)).toEqual([
            'released STOPPED',
            'released STOPPED',
            'released STOPPED',
        ]);
        await clock.settle(Promise.all([held, stopped]));
    });

    it('a maxWaitMs stop releases waiting jobs of every priority with MAX_WAIT_EXCEEDED', async () => {
        const clock = createFakeClock();
        const queue = createQueue<string, string>({
            clock,
            concurrency: 1,
            maxWaitMs: 5000,
            processJob: () => ({ ok: false, error: { type: 'backoff_global', message: '429', retryAfterMs: 60_000 } }),
        });
        const pushed = [0, 2, -1].map((priority) => queue.push(`p${priority}`, { priority }));
        const outcomes = await clock.settle(Promise.all(pushed));
        expect(outcomes.map((outcome) => (outcome.ok ? 'ok' : outcome.error.code))).toEqual([
            'MAX_WAIT_EXCEEDED',
            'MAX_WAIT_EXCEEDED',
            'MAX_WAIT_EXCEEDED',
        ]);
    });
});

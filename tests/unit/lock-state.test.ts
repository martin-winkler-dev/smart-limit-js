// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createLimiter, type JobResult, type Limiter } from '../../src/index.js';
import { readLockState } from '../../src/limiter.js';
import type { LimiterLockState } from '../../src/run-state.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

/** Verifies lock state transitions, error tracking, and epoch increments. */

function sleep(clock: FakeClock, ms: number) {
    return new Promise<void>((resolve) => clock.setTimeout(resolve, ms));
}

function lockOf(limiter: Limiter): Omit<LimiterLockState, 'isStoppedByUser' | 'lastStoppedBy'> {
    const state = readLockState(limiter);
    if (!state) {
        throw new Error('no lock state');
    }
    const { isStoppedByUser: _, lastStoppedBy: __, ...lock } = state;
    return lock;
}

const UNLOCKED = { status: 'unlocked', lockedSince: 0, lockUntil: 0, consecutiveFailures: 0 } as const;

describe('lock state machine', () => {
    it('increments the epoch on start, lock, probe start and unlock, and resets the lock on unlock', async () => {
        const clock = createFakeClock();
        const seen: [string, ReturnType<typeof lockOf>][] = [];
        let attempts = 0;
        const limiter: Limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            fetchJob: (() => {
                let next = 1;
                return () => (next <= 2 ? { job: next++ } : null);
            })(),
            processJob: async (job): Promise<JobResult<number>> => {
                attempts++;
                seen.push([`start job ${job} attempt ${attempts}`, lockOf(limiter)]);
                await sleep(clock, 100);
                return attempts <= 2
                    ? { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } }
                    : { ok: true, data: job };
            },
            onAttempt: (job) => {
                seen.push([`after job ${job} attempt ${attempts}`, lockOf(limiter)]);
            },
        });
        expect(lockOf(limiter)).toEqual({ ...UNLOCKED, epoch: 0 });
        const run = limiter.start();
        expect(lockOf(limiter)).toEqual({ ...UNLOCKED, epoch: 1 });
        await clock.settle(run);
        expect(seen).toEqual([
            ['start job 1 attempt 1', { ...UNLOCKED, epoch: 1 }],
            // Lock initialized: increments epoch, sets lockedSince/lockUntil, records first failure.
            [
                'after job 1 attempt 1',
                { status: 'waiting', epoch: 2, lockedSince: 100, lockUntil: 1_100, consecutiveFailures: 1 },
            ],
            // Probe started: increments epoch to retrying state.
            [
                'start job 1 attempt 2',
                { status: 'retrying', epoch: 3, lockedSince: 100, lockUntil: 1_100, consecutiveFailures: 1 },
            ],
            // Probe failed: increments epoch and consecutiveFailures, extends lockUntil.
            [
                'after job 1 attempt 2',
                { status: 'waiting', epoch: 4, lockedSince: 100, lockUntil: 2_200, consecutiveFailures: 2 },
            ],
            [
                'start job 1 attempt 3',
                { status: 'retrying', epoch: 5, lockedSince: 100, lockUntil: 2_200, consecutiveFailures: 2 },
            ],
            // Probe succeeded: increments epoch and resets lock state.
            ['after job 1 attempt 3', { ...UNLOCKED, epoch: 6 }],
            ['start job 2 attempt 4', { ...UNLOCKED, epoch: 6 }],
            ['after job 2 attempt 4', { ...UNLOCKED, epoch: 6 }],
        ]);
        limiter.start();
        expect(lockOf(limiter)).toEqual({ ...UNLOCKED, epoch: 7 });
    });

    it('keeps a persisted lock across runs, gives a new run a fresh lockedSince, and resets it all with force', async () => {
        const clock = createFakeClock();
        const limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            maxWaitMs: 5_000,
            fetchJob: () => ({ job: 1 }),
            processJob: (): JobResult<number> => ({
                ok: false,
                error: { type: 'backoff_global', message: '503', retryAfterMs: 60_000 },
            }),
        });
        const first = await clock.settle(limiter.start());
        expect(first.ok && first.data.stoppedBy).toBe('max_wait_exceeded');
        const persisted = { status: 'waiting', lockedSince: 0, lockUntil: 60_000, consecutiveFailures: 1 } as const;
        expect(lockOf(limiter)).toEqual({ ...persisted, epoch: 2 });
        expect(readLockState(limiter)?.isStoppedByUser).toBe(false);

        // Unforced: retains lockUntil and failures, assigning fresh lockedSince budget.
        await clock.advance(10_000);
        const second = limiter.start();
        expect(lockOf(limiter)).toEqual({ ...persisted, lockedSince: 10_000, epoch: 3 });
        expect(await second).toEqual({
            ok: true,
            data: { done: 0, failed: 0, released: 0, stoppedBy: 'max_wait_exceeded' },
        });

        // Forced restart: full lock state reset.
        void limiter.start({ force: true });
        expect(lockOf(limiter)).toEqual({ ...UNLOCKED, epoch: 4 });
        await clock.settle(limiter.stop());
        expect(readLockState(limiter)?.isStoppedByUser).toBe(true);
    });

    it('stop() during a probe steps back to waiting without an epoch change, so the probe result still applies', async () => {
        const clock = createFakeClock();
        let attempts = 0;
        const limiter: Limiter = createLimiter<number, number>({
            clock,
            concurrency: 1,
            fetchJob: () => ({ job: 1 }),
            processJob: async (): Promise<JobResult<number>> => {
                attempts++;
                if (attempts === 2) {
                    void limiter.stop();
                    expect(lockOf(limiter)).toEqual({
                        status: 'waiting',
                        epoch: 3,
                        lockedSince: 0,
                        lockUntil: 1_000,
                        consecutiveFailures: 1,
                    });
                    await sleep(clock, 100);
                    return { ok: true, data: 1 };
                }
                return { ok: false, error: { type: 'backoff_global', message: '503', retryAfterMs: 1_000 } };
            },
        });
        const result = await clock.settle(limiter.start());
        expect(result).toEqual({ ok: true, data: { done: 1, failed: 0, released: 0, stoppedBy: 'stop' } });
        expect(attempts).toBe(2);
        // Probe succeeded during stopping phase: unlocks state.
        expect(lockOf(limiter)).toEqual({ ...UNLOCKED, epoch: 4 });
    });

    it('reads nothing for an object that is not a configured limiter', () => {
        const invalid = createLimiter({} as never);
        expect(readLockState(invalid)).toBeUndefined();
    });
});

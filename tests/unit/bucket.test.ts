// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import {
    checkBuckets,
    createBucket,
    createConcurrencyBucket,
    createRPD,
    createRPM,
    createSlidingWindow,
    createTPD,
    createTPM,
    tryReserveBuckets,
    WAIT_FOR_CHANGE,
} from '../../src/bucket.js';
import type { Bucket, Cost } from '../../src/types.js';

describe('createSlidingWindow', () => {
    it('allows up to max, then waits until the oldest entry leaves the window', () => {
        const rpm = createSlidingWindow({ requests: 2, perMs: 60_000 });
        expect(rpm.getWaitMs({ requests: 1 }, 0)).toBe(0);
        book(rpm, { requests: 1 }, 0);
        book(rpm, { requests: 1 }, 10_000);
        expect(rpm.getWaitMs({ requests: 1 }, 20_000)).toBe(40_000);
        expect(rpm.getWaitMs({ requests: 1 }, 60_000)).toBe(0);
    });

    it('waits until enough amount has left the window', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        book(tpm, { tokens: 30 }, 0);
        book(tpm, { tokens: 30 }, 10_000);
        book(tpm, { tokens: 30 }, 20_000);
        expect(tpm.getWaitMs({ tokens: 50 }, 30_000)).toBe(40_000);
    });

    it('ignores costs without its unit (zero-cost jobs are never blocked)', () => {
        const tpm = createSlidingWindow({ tokens: 10, perMs: 60_000 });
        tpm.reserve({ tokens: 10 }, 0);
        expect(tpm.getWaitMs({ requests: 1 }, 0)).toBe(0);
        expect(tpm.getWaitMs({ tokens: 0 }, 0)).toBe(0);
    });

    it('ignores float residue of expired fractional amounts', () => {
        const usd = createSlidingWindow({ usd: 1, perMs: 1_000 });
        for (const amount of [0.2, 0.2, 0.7]) {
            usd.settle(usd.reserve({ usd: amount }, 0), { usd: amount }, 0);
        }
        // Zero USD cost booked after fractional amounts expire.
        usd.reserve({ requests: 1 }, 5_000);
        expect(usd.getWaitMs({ usd: 1 }, 5_000)).toBe(0);
        for (const at of [6_000, 6_100]) {
            usd.settle(usd.reserve({ usd: 0.5 }, at), { usd: 0.5 }, at);
        }
        // Window at capacity: zero cost fits; additional cost waits for oldest 0.5 entry to expire.
        expect(usd.getWaitMs({ requests: 1 }, 6_100)).toBe(0);
        expect(usd.getWaitMs({ usd: 0.1 }, 6_100)).toBe(900);
    });

    it('returns Infinity when the cost can never fit', () => {
        expect(createTPM(100).getWaitMs({ tokens: 101 }, 0)).toBe(Number.POSITIVE_INFINITY);
    });

    it('moves the token usage to the settle time, for less, more and exactly the booked amount', () => {
        for (const used of [20, 100, 150]) {
            const tpm = createSlidingWindow({ tokens: 200, perMs: 60_000 });
            const reservation = tpm.reserve({ tokens: 100 }, 0);
            tpm.settle(reservation, { tokens: used }, 30_000);
            // Settle time determines retention window beyond original booking expiry.
            expect(tpm.getWaitMs({ tokens: 200 - used }, 60_000)).toBe(0);
            expect(tpm.getWaitMs({ tokens: 201 - used }, 60_000)).toBe(30_000);
            expect(tpm.getWaitMs({ tokens: 200 }, 90_000)).toBe(0);
        }
    });

    it('keeps the entries sorted when a middle entry moves to the settle time', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        book(tpm, { tokens: 10 }, 0);
        const middle = tpm.reserve({ tokens: 10 }, 10_000);
        book(tpm, { tokens: 10 }, 20_000);
        tpm.settle(middle, { tokens: 10 }, 30_000);
        // At t=60s, t=20s entry expires before moved t=30s entry.
        expect(tpm.getWaitMs({ tokens: 80 }, 60_000)).toBe(0);
        expect(tpm.getWaitMs({ tokens: 81 }, 60_000)).toBe(20_000);
        expect(tpm.getWaitMs({ tokens: 91 }, 60_000)).toBe(30_000);
        expect(tpm.getWaitMs({ tokens: 100 }, 90_000)).toBe(0);
    });

    it('gives the tokens back at once for a settle with 0', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        const reservation = tpm.reserve({ tokens: 100 }, 0);
        tpm.settle(reservation, {}, 1_000);
        expect(tpm.getWaitMs({ tokens: 100 }, 1_000)).toBe(0);
    });

    it('moves requests to the settle time like tokens, each with its own unit', () => {
        const rpm = createSlidingWindow({ requests: 2, perMs: 60_000 });
        const first = rpm.reserve({ requests: 1, tokens: 500 }, 0);
        const second = rpm.reserve({ requests: 1, tokens: 500 }, 0);
        rpm.settle(first, { requests: 1, tokens: 900 }, 50_000);
        rpm.settle(second, { requests: 0, tokens: 900 }, 50_000);
        // Tokens ignored: single request counted from t=50s; other released.
        expect(rpm.getWaitMs({ requests: 1 }, 60_000)).toBe(0);
        expect(rpm.getWaitMs({ requests: 2 }, 60_000)).toBe(50_000);
        expect(rpm.getWaitMs({ requests: 2 }, 110_000)).toBe(0);
    });

    it('books the whole usage of an entry that already left the window at the settle time', () => {
        for (const used of [10, 40, 90]) {
            const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
            const reservation = tpm.reserve({ tokens: 40 }, 0);
            expect(tpm.getWaitMs({ tokens: 100 }, 60_000)).toBe(0);
            // Attempt duration exceeds window duration.
            tpm.settle(reservation, { tokens: used }, 70_000);
            expect(tpm.getWaitMs({ tokens: 100 - used }, 70_000)).toBe(0);
            expect(tpm.getWaitMs({ tokens: 101 - used }, 70_000)).toBe(60_000);
        }
    });

    it('books the late usage even when nothing moved the window while the attempt ran', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        const reservation = tpm.reserve({ tokens: 10 }, 0);
        tpm.settle(reservation, { tokens: 90 }, 70_000);
        expect(tpm.getWaitMs({ tokens: 11 }, 70_000)).toBe(60_000);
        expect(tpm.getWaitMs({ tokens: 10 }, 70_000)).toBe(0);
    });

    it('books at the newest time when the settle time is out of order', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        const late = tpm.reserve({ tokens: 10 }, 0);
        book(tpm, { tokens: 50 }, 80_000);
        // Monotonic clamp preserves order when settle timestamp precedes newest booking.
        tpm.settle(late, { tokens: 30 }, 70_000);
        expect(tpm.getWaitMs({ tokens: 21 }, 80_000)).toBe(60_000);
        expect(tpm.getWaitMs({ tokens: 20 }, 80_000)).toBe(0);
        expect(tpm.getWaitMs({ tokens: 100 }, 140_000)).toBe(0);
    });

    it('paces with a small window', () => {
        const pace = createSlidingWindow({ requests: 1, perMs: 3000 });
        book(pace, { requests: 1 }, 0);
        expect(pace.getWaitMs({ requests: 1 }, 1_000)).toBe(2_000);
        expect(pace.getWaitMs({ requests: 1 }, 3_000)).toBe(0);
    });

    it('counts used entries until they age out', () => {
        const rpm = createRPM(2, { used: [{ at: 30_000 }, { at: 0, amount: 1 }] });
        expect(rpm.getWaitMs({ requests: 1 }, 40_000)).toBe(20_000);
        expect(rpm.getWaitMs({ requests: 1 }, 60_000)).toBe(0);
    });

    it('counts a used number as used at the first call', () => {
        const tpd = createTPD(1_000, { used: 1_000 });
        expect(tpd.getWaitMs({ tokens: 1 }, 5_000)).toBe(86_400_000);
    });

    it('builds the shortcuts with the right unit and window', () => {
        const rpd = createRPD(1);
        book(rpd, { requests: 1 }, 0);
        expect(rpd.getWaitMs({ requests: 1 }, 0)).toBe(86_400_000);
        const tpm = createTPM(1);
        book(tpm, { tokens: 1 }, 0);
        expect(tpm.getWaitMs({ tokens: 1 }, 0)).toBe(60_000);
    });

    it.each([
        [{ perMs: 60_000 }],
        [{ perMs: 60_000, requests: 1, tokens: 1 }],
        [{ perMs: 0, requests: 1 }],
        [{ perMs: 60_000, requests: -1 }],
        [{ perMs: 60_000, requests: 1, used: -5 }],
        [{ perMs: 60_000, requests: 1, used: [{ at: 'x' }] }],
        [{ per: 60, requests: 1 }],
        [null],
    ])('carries invalid options as configError instead of throwing: %p', (options) => {
        const bucket = createSlidingWindow(options as never);
        expect(bucket.configError?.code).toBe('INVALID_CONFIG');
    });
});

describe('createSlidingWindow notifies subscribers on settle (Gemini4 20.1)', () => {
    it('calls every subscriber once per settle and stops after unsubscribe', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        const calls: string[] = [];
        tpm.subscribe?.(() => calls.push('a'));
        const unsubscribe = tpm.subscribe?.(() => calls.push('b'));
        tpm.settle(tpm.reserve({ tokens: 100 }, 0), { tokens: 10 }, 1_000);
        expect(calls).toEqual(['a', 'b']);
        unsubscribe?.();
        tpm.settle(tpm.reserve({ tokens: 10 }, 1_000), { tokens: 10 }, 2_000);
        expect(calls).toEqual(['a', 'b', 'a']);
    });

    it('keeps notifying the others when one subscriber throws', () => {
        const tpm = createSlidingWindow({ tokens: 100, perMs: 60_000 });
        const calls: string[] = [];
        tpm.subscribe?.(() => {
            throw new Error('boom');
        });
        tpm.subscribe?.(() => calls.push('b'));
        expect(() => tpm.settle(tpm.reserve({ tokens: 1 }, 0), {}, 0)).not.toThrow();
        expect(calls).toEqual(['b']);
    });
});

describe('createSlidingWindow counts unsettled bookings as freeable (Gemini4 20.2)', () => {
    it('waits for a change while in-flight bookings cover the shortfall', () => {
        const tpd = createTPD(1_000);
        const reservation = tpd.reserve({ tokens: 900 }, 0);
        expect(tpd.getWaitMs({ tokens: 500 }, 1_000)).toBe(WAIT_FOR_CHANGE);
        tpd.settle(reservation, { tokens: 100 }, 1_000);
        expect(tpd.getWaitMs({ tokens: 500 }, 1_000)).toBe(0);
    });

    it('gives a timed wait once the in-flight booking settles at its full amount', () => {
        const tpd = createTPD(1_000);
        tpd.settle(tpd.reserve({ tokens: 900 }, 0), { tokens: 900 }, 1_000);
        expect(tpd.getWaitMs({ tokens: 500 }, 1_000)).toBe(86_400_000);
    });

    it('waits only for settled bookings to cover the rest of the shortfall', () => {
        const tpm = createTPM(100);
        book(tpm, { tokens: 30 }, 0);
        book(tpm, { tokens: 30 }, 10_000);
        tpm.reserve({ tokens: 30 }, 20_000);
        // Shortfall of 60 covered by 30 in-flight and 30 settled at t=0.
        expect(tpm.getWaitMs({ tokens: 70 }, 30_000)).toBe(30_000);
    });

    it('stops counting an in-flight booking once it left the window', () => {
        const tpm = createTPM(100);
        tpm.reserve({ tokens: 100 }, 0);
        book(tpm, { tokens: 100 }, 60_000);
        expect(tpm.getWaitMs({ tokens: 1 }, 60_000)).toBe(60_000);
    });
});

describe('createBucket', () => {
    it('defaults every missing hook to "fits" / "nothing to do"', () => {
        const bucket = createBucket();
        expect(bucket.getWaitMs({ requests: 1 }, 0)).toBe(0);
        expect(bucket.reserve({ requests: 1 }, 0)).toBeUndefined();
        expect(() => bucket.settle(undefined, {}, 0)).not.toThrow();
    });

    it('passes the reservation from reserve to settle', () => {
        const settled: unknown[] = [];
        const bucket = createBucket({
            reserve: (cost) => ({ booked: cost.requests }),
            settle: (reservation, actual) => settled.push([reservation, actual]),
        });
        bucket.settle(bucket.reserve({ requests: 1 }, 0), { requests: 0 }, 0);
        expect(settled).toEqual([[{ booked: 1 }, { requests: 0 }]]);
    });
});

describe('createConcurrencyBucket', () => {
    it('allows up to limit attempts in flight, then waits for a change', () => {
        const bucket = createConcurrencyBucket(2);
        const first = bucket.reserve({ requests: 1 }, 0);
        bucket.reserve({ requests: 1 }, 0);
        expect(bucket.getWaitMs({ requests: 1 }, 0)).toBe(WAIT_FOR_CHANGE);
        bucket.settle(first, {}, 10);
        expect(bucket.getWaitMs({ requests: 1 }, 10)).toBe(0);
    });

    it('frees a slot only once per reservation', () => {
        const bucket = createConcurrencyBucket(1);
        const reservation = bucket.reserve({}, 0);
        bucket.settle(reservation, {}, 0);
        bucket.settle(reservation, {}, 0);
        bucket.reserve({}, 0);
        expect(bucket.getWaitMs({}, 0)).toBe(WAIT_FOR_CHANGE);
    });

    it('notifies subscribers on settle, starting with a different one each time', () => {
        const bucket = createConcurrencyBucket(1);
        const calls: string[] = [];
        bucket.subscribe?.(() => calls.push('a'));
        const unsubscribe = bucket.subscribe?.(() => calls.push('b'));
        bucket.settle(bucket.reserve({}, 0), {}, 0);
        bucket.settle(bucket.reserve({}, 0), {}, 0);
        expect(calls).toEqual(['b', 'a', 'a', 'b']);
        unsubscribe?.();
        calls.length = 0;
        bucket.settle(bucket.reserve({}, 0), {}, 0);
        expect(calls).toEqual(['a']);
    });

    it('carries a configError for an invalid limit', () => {
        for (const limit of [0, 1.5, -1, Number.NaN]) {
            expect(createConcurrencyBucket(limit).configError?.code).toBe('INVALID_CONFIG');
        }
    });

    it('is built on createBucket, which passes subscribe through', () => {
        const listeners: (() => void)[] = [];
        const bucket = createBucket({
            subscribe: (onChange) => {
                listeners.push(onChange);
                return () => {};
            },
        });
        bucket.subscribe?.(() => {});
        expect(listeners).toHaveLength(1);
        expect(createBucket().subscribe).toBeUndefined();
    });
});

describe('checkBuckets', () => {
    it('reports a wait for a change together with the longest timed wait, so neither masks the other', () => {
        const full = createConcurrencyBucket(1);
        full.reserve({}, 0);
        expect(checkBuckets([createBucketWaitingFor(500), full], { requests: 1 }, 0)).toEqual({
            ok: true,
            data: { waitMs: 500, waitForChange: true },
        });
        expect(checkBuckets([full], { requests: 1 }, 0)).toEqual({
            ok: true,
            data: { waitMs: 0, waitForChange: true },
        });
    });

    it('still reports COST_EXCEEDS_LIMIT after a WAIT_FOR_CHANGE', () => {
        const full = createConcurrencyBucket(1);
        full.reserve({}, 0);
        const result = checkBuckets([full, createTPM(10)], { tokens: 11 }, 0);
        expect(result.ok ? undefined : result.error.code).toBe('COST_EXCEEDS_LIMIT');
    });

    it('reports BUCKET_FAILED for WAIT_FOR_CHANGE from a bucket without subscribe', () => {
        const result = checkBuckets([createBucketWaitingFor(WAIT_FOR_CHANGE)], { requests: 1 }, 0);
        expect(result.ok ? undefined : result.error.code).toBe('BUCKET_FAILED');
    });

    it('returns the longest wait across all buckets', () => {
        expect(
            checkBuckets(
                [createBucketWaitingFor(5), createBucketWaitingFor(20), createBucketWaitingFor(0)],
                { requests: 1 },
                0,
            ),
        ).toEqual({
            ok: true,
            data: { waitMs: 20, waitForChange: false },
        });
    });

    it('returns 0 without buckets', () => {
        expect(checkBuckets([], { requests: 1 }, 0)).toEqual({ ok: true, data: { waitMs: 0, waitForChange: false } });
    });

    it('reports COST_EXCEEDS_LIMIT when a bucket can never fit the cost', () => {
        const result = checkBuckets([createBucketWaitingFor(5), createTPM(10)], { tokens: 11 }, 0);
        expect(result.ok ? undefined : result.error.code).toBe('COST_EXCEEDS_LIMIT');
    });

    it.each([
        [
            'throws',
            createBucket({
                getWaitMs: () => {
                    throw new Error('boom');
                },
            }),
        ],
        ['returns NaN', createBucketWaitingFor(Number.NaN)],
        ['returns a non-number', createBucketWaitingFor('soon')],
        ['returns a negative number other than WAIT_FOR_CHANGE (-2)', createBucketWaitingFor(-2)],
        ['returns -0.5', createBucketWaitingFor(-0.5)],
        ['returns -Infinity', createBucketWaitingFor(Number.NEGATIVE_INFINITY)],
    ])('reports BUCKET_FAILED when a bucket %s', (_, bucket) => {
        const result = checkBuckets([bucket], { requests: 1 }, 0);
        expect(result.ok ? undefined : result.error.code).toBe('BUCKET_FAILED');
    });
});

describe('tryReserveBuckets', () => {
    it('reserves across all buckets when every bucket returns 0 wait', () => {
        const concurrency = createConcurrencyBucket(1);
        const rpm = createRPM(1);
        const result = tryReserveBuckets([concurrency, rpm], { requests: 1 }, 0);
        expect(result.ok).toBe(true);
        if (result.ok) {
            expect(result.data.reserved).toBe(true);
            if (result.data.reserved) {
                expect(result.data.reservations).toHaveLength(2);
            }
        }
        expect(concurrency.getWaitMs({ requests: 1 }, 0)).toBe(WAIT_FOR_CHANGE);
        // Held in-flight until settled.
        expect(rpm.getWaitMs({ requests: 1 }, 0)).toBe(WAIT_FOR_CHANGE);
    });

    it('does not reserve in any bucket when a bucket reports a wait > 0 or WAIT_FOR_CHANGE', () => {
        const concurrency = createConcurrencyBucket(1);
        const rpm = createRPM(1);
        book(rpm, { requests: 1 }, 0);

        const result = tryReserveBuckets([concurrency, rpm], { requests: 1 }, 10_000);
        expect(result).toEqual({ ok: true, data: { reserved: false, waitMs: 50_000, waitForChange: false } });
        expect(concurrency.getWaitMs({ requests: 1 }, 10_000)).toBe(0);
        concurrency.reserve({}, 10_000);
        expect(tryReserveBuckets([concurrency], { requests: 1 }, 10_000)).toEqual({
            ok: true,
            data: { reserved: false, waitMs: 0, waitForChange: true },
        });
    });

    it('rolls back already-reserved buckets via settle(reservation, {}, now) when a subsequent bucket throws in reserve', () => {
        const concurrency = createConcurrencyBucket(1);
        const tpm = createTPM(100);
        const throwingReserve = createBucket({
            reserve: () => {
                throw new Error('reserve exploded');
            },
        });

        const result = tryReserveBuckets([concurrency, tpm, throwingReserve], { requests: 1, tokens: 80 }, 500);
        expect(result.ok).toBe(false);
        if (!result.ok) {
            expect(result.error.code).toBe('BUCKET_FAILED');
        }
        expect(concurrency.getWaitMs({ requests: 1 }, 500)).toBe(0);
        expect(tpm.getWaitMs({ tokens: 100 }, 500)).toBe(0);
    });

    it('propagates checkBuckets errors without reserving', () => {
        const concurrency = createConcurrencyBucket(1);
        const tpm = createTPM(50);
        const result = tryReserveBuckets([concurrency, tpm], { requests: 1, tokens: 100 }, 0);
        expect(result.ok ? undefined : result.error.code).toBe('COST_EXCEEDS_LIMIT');
        expect(concurrency.getWaitMs({ requests: 1 }, 0)).toBe(0);
    });
});

/** Immediately reserves and settles cost at given timestamp. */
function book(bucket: Bucket, cost: Cost, at: number): void {
    bucket.settle(bucket.reserve(cost, at), cost, at);
}

/** Creates a bucket returning a fixed wait value. */
function createBucketWaitingFor(answer: unknown): Bucket {
    return createBucket({ getWaitMs: () => answer as number });
}

describe('createSlidingWindow keeps its bookings in time order (5.5) and cuts expired ones in batches (6.7)', () => {
    it('counts a used entry in the future as used at the first call', () => {
        const bucket = createSlidingWindow({ requests: 2, perMs: 60_000, used: [{ at: 1_000_000 }] });
        // Future timestamp clamped to current time (t=0), expiring at t=60s.
        expect(bucket.getWaitMs({ requests: 2 }, 0)).toBe(60_000);
        expect(bucket.getWaitMs({ requests: 2 }, 60_000)).toBe(0);
    });

    it('never books in the past when the clock jumps back, so a wait is never too short', () => {
        const bucket = createSlidingWindow({ requests: 2, perMs: 60_000 });
        book(bucket, { requests: 1 }, 20_000);
        // Clock regressed: monotonic booking clamps t=5s to t=20s.
        book(bucket, { requests: 1 }, 5_000);
        expect(bucket.getWaitMs({ requests: 2 }, 30_000)).toBe(50_000);
        expect(bucket.getWaitMs({ requests: 2 }, 80_000)).toBe(0);
    });

    it('keeps exact totals across thousands of bookings', () => {
        const bucket = createSlidingWindow({ requests: 2_000, perMs: 1000 });
        for (let at = 0; at < 3_000; at++) {
            book(bucket, { requests: 1 }, at);
        }
        // Bookings across t=2000..2999 remain in 1 s window.
        expect(bucket.getWaitMs({ requests: 1_000 }, 2_999)).toBe(0);
        expect(bucket.getWaitMs({ requests: 1_001 }, 2_999)).toBe(1);
        // Final booking at t=2999 expires at t=3999.
        expect(bucket.getWaitMs({ requests: 2_000 }, 3_998)).toBe(1);
        expect(bucket.getWaitMs({ requests: 2_000 }, 3_999)).toBe(0);
    });
});

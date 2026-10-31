// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { createLimiterError } from './errors.js';
import type {
    Bucket,
    BucketHooks,
    Cost,
    LimiterError,
    PriorUsage,
    Result,
    SlidingWindowOptions,
    SlidingWindowShortcutOptions,
} from './types.js';
import { callSafely, isFiniteNonNegative, isObject } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Active booking in sliding window; tracks expiration and settle state. */
type Reservation = { at: number; amount: number; inWindow: boolean; isInFlight: boolean };

/** Normalized configuration for sliding window bucket. */
type SlidingWindowConfig = { unit: string; limit: number; windowMs: number; used: PriorUsage };

/** Longest wait across buckets and whether any bucket awaits change notification. */
export type BucketWait = { waitMs: number; waitForChange: boolean };

/** Result of reserving capacity across all buckets. */
export type BucketReservationResult = { reserved: true; reservations: unknown[] } | ({ reserved: false } & BucketWait);

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

/** Signal for `getWaitMs` indicating block until bucket `subscribe` reports change. */
export const WAIT_FOR_CHANGE = -1;

const MS_PER_MINUTE = 60_000;
const MS_PER_DAY = 86_400_000;

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

/** Creates bucket from hook definitions. */
export function createBucket<R = undefined>(hooks: BucketHooks<R> = {}): Bucket {
    const bucket: Bucket = {
        getWaitMs(cost, now) {
            return hooks.getWaitMs ? hooks.getWaitMs(cost, now) : 0;
        },
        reserve(cost, now) {
            return hooks.reserve?.(cost, now);
        },
        // Returns hook result so caller catches async rejections.
        settle(reservation, actualCost, now) {
            return hooks.settle?.(reservation as R, actualCost, now);
        },
    };
    if (hooks.subscribe) {
        bucket.subscribe = hooks.subscribe;
    }
    return bucket;
}

/**
 * **Limits concurrent attempts across limiters**: announces freed slots via round-robin notifications.
 *
 * - caps in-flight attempts at `limit`
 * - notifies subscribers in round-robin order when slot frees
 * - not strictly FIFO: under continuous contention later limiters may claim freed slot
 */
export function createConcurrencyBucket(limit: number): Bucket {
    if (!(Number.isInteger(limit) && limit >= 1)) {
        return {
            ...createBucket(),
            configError: createLimiterError('INVALID_CONFIG', 'concurrency limit must be an integer >= 1'),
        };
    }
    let inFlight = 0;
    let firstListener = 0;
    const listeners: (() => void)[] = [];

    return createBucket<{ isOpen: boolean }>({
        getWaitMs() {
            return inFlight < limit ? 0 : WAIT_FOR_CHANGE;
        },
        reserve() {
            inFlight++;
            return { isOpen: true };
        },
        settle(reservation) {
            if (!reservation?.isOpen) {
                return;
            }
            reservation.isOpen = false;
            inFlight--;
            // Snapshot listeners to support mid-notification unsubscribes.
            const current = [...listeners];
            firstListener = current.length > 0 ? (firstListener + 1) % current.length : 0;
            for (let i = 0; i < current.length; i++) {
                callSafely(current[(firstListener + i) % current.length]);
            }
        },
        subscribe: (onChange) => addListener(listeners, onChange),
    });
}

/**
 * **Sliding window rate limiter bucket**: bounds usage to `limit` units within rolling `perMs` duration.
 *
 * - tracks timestamped reservations within rolling window
 * - notifies subscribers on every settle (estimate may shrink or booking moves)
 * - wait counts in-flight bookings as freeable on settle: shortfall covered by them gives `WAIT_FOR_CHANGE`, else
 *   wait for settled bookings to cover rest (lower bound; avoids false `maxWaitMs` stops)
 * - invalid options return bucket with `configError` instead of throwing
 * - supports bursts up to limit or steady pacing based on window size
 */
export function createSlidingWindow(options: SlidingWindowOptions): Bucket {
    const config = validateSlidingWindowOptions(options);
    if (!config.ok) {
        return { ...createBucket(), configError: createLimiterError('INVALID_CONFIG', config.error) };
    }
    const { unit, limit, windowMs, used } = config.data;
    const reservations: Reservation[] =
        typeof used === 'number'
            ? []
            : used
                  .map((entry) => ({ at: entry.at, amount: entry.amount ?? 1, inWindow: true, isInFlight: false }))
                  .sort((a, b) => a.at - b.at);
    let totalInWindow = reservations.reduce((sum, reservation) => sum + reservation.amount, 0);
    // Portion of totalInWindow booked by unsettled attempts.
    let inFlightInWindow = 0;
    const listeners: (() => void)[] = [];
    // Numeric prior usage booked on first call timestamp.
    let unbookedPriorUsage = typeof used === 'number' ? used : 0;
    let isFirstCall = true;
    // Index of oldest valid reservation; pruned in batches.
    let head = 0;

    /** Advances window time, prunes expired entries, and books initial prior usage. */
    function moveWindowTo(now: number): number {
        if (isFirstCall) {
            isFirstCall = false;
            for (const reservation of reservations) {
                reservation.at = Math.min(reservation.at, now);
            }
            if (unbookedPriorUsage > 0) {
                reservations.push({ at: now, amount: unbookedPriorUsage, inWindow: true, isInFlight: false });
                totalInWindow += unbookedPriorUsage;
                unbookedPriorUsage = 0;
            }
        }
        const time = Math.max(now, reservations.at(-1)?.at ?? Number.NEGATIVE_INFINITY);
        while (head < reservations.length && reservations[head].at + windowMs <= time) {
            totalInWindow -= reservations[head].amount;
            if (reservations[head].isInFlight) {
                inFlightInWindow -= reservations[head].amount;
            }
            reservations[head].inWindow = false;
            head++;
        }
        if (head >= 1024 && head * 2 >= reservations.length) {
            reservations.splice(0, head);
            head = 0;
        }
        return time;
    }

    return createBucket<Reservation>({
        getWaitMs(cost, now) {
            const amount = cost[unit] ?? 0;
            if (amount > limit) {
                return Number.POSITIVE_INFINITY;
            }
            const time = moveWindowTo(now);
            let excess = totalInWindow + amount - limit;
            // Ignores float residue of fractional amounts added to and removed from totalInWindow.
            const tolerance = limit * 1e-9;
            if (excess <= tolerance) {
                return 0;
            }
            // In-flight bookings may settle and free capacity at any time.
            excess -= inFlightInWindow;
            if (excess <= tolerance) {
                return WAIT_FOR_CHANGE;
            }
            // Walk oldest settled reservations until enough leave the window to make room.
            let waitMs = 0;
            for (let i = head; i < reservations.length && excess > tolerance; i++) {
                if (!reservations[i].isInFlight) {
                    excess -= reservations[i].amount;
                    waitMs = reservations[i].at + windowMs - time;
                }
            }
            return Math.max(waitMs, 0);
        },
        reserve(cost, now) {
            const reservation = { at: moveWindowTo(now), amount: cost[unit] ?? 0, inWindow: true, isInFlight: true };
            reservations.push(reservation);
            totalInWindow += reservation.amount;
            inFlightInWindow += reservation.amount;
            return reservation;
        },
        settle(reservation, actualCost, now) {
            const amount = actualCost[unit] ?? 0;
            // Advance window first so expired entries are dropped before recalculating.
            const time = moveWindowTo(now);
            // Moves entry to settle timestamp with actual usage; keeps list chronologically sorted.
            if (reservation.inWindow) {
                totalInWindow -= reservation.amount;
                if (reservation.isInFlight) {
                    inFlightInWindow -= reservation.amount;
                }
                reservations.splice(reservations.indexOf(reservation, head), 1);
            }
            reservation.at = time;
            reservation.amount = amount;
            reservation.inWindow = true;
            reservation.isInFlight = false;
            reservations.push(reservation);
            totalInWindow += amount;
            // Snapshot listeners to support mid-notification unsubscribes.
            for (const listener of [...listeners]) {
                callSafely(listener);
            }
        },
        subscribe: (onChange) => addListener(listeners, onChange),
    });
}

/** Creates requests-per-minute (RPM) sliding window bucket. */
export function createRPM(limit: number, options: SlidingWindowShortcutOptions = {}): Bucket {
    return createSlidingWindow({ ...options, requests: limit, perMs: MS_PER_MINUTE });
}

/** Creates tokens-per-minute (TPM) sliding window bucket. */
export function createTPM(limit: number, options: SlidingWindowShortcutOptions = {}): Bucket {
    return createSlidingWindow({ ...options, tokens: limit, perMs: MS_PER_MINUTE });
}

/** Creates requests-per-day (RPD, rolling 24h) sliding window bucket. */
export function createRPD(limit: number, options: SlidingWindowShortcutOptions = {}): Bucket {
    return createSlidingWindow({ ...options, requests: limit, perMs: MS_PER_DAY });
}

/** Creates tokens-per-day (TPD, rolling 24h) sliding window bucket. */
export function createTPD(limit: number, options: SlidingWindowShortcutOptions = {}): Bucket {
    return createSlidingWindow({ ...options, tokens: limit, perMs: MS_PER_DAY });
}

/**
 * **Evaluates wait duration across buckets**: returns required delay and change-wait flag.
 *
 * - checks all buckets for capacity against `cost` at `now`
 * - returns `COST_EXCEEDS_LIMIT` if cost exceeds capacity
 * - returns `BUCKET_FAILED` on unexpected bucket return values or throws
 */
export function checkBuckets(buckets: readonly Bucket[], cost: Cost, now: number): Result<BucketWait, LimiterError> {
    let waitMs = 0;
    let waitForChange = false;
    for (const bucket of buckets) {
        const answer = callSafely(() => bucket.getWaitMs(cost, now));
        if (
            !answer.ok ||
            typeof answer.data !== 'number' ||
            Number.isNaN(answer.data) ||
            (answer.data < 0 && answer.data !== WAIT_FOR_CHANGE)
        ) {
            return { ok: false, error: createLimiterError('BUCKET_FAILED', answer.ok ? answer.data : answer.error) };
        }
        if (answer.data === Number.POSITIVE_INFINITY) {
            return { ok: false, error: createLimiterError('COST_EXCEEDS_LIMIT', { cost }) };
        }
        if (answer.data === WAIT_FOR_CHANGE) {
            if (typeof bucket.subscribe !== 'function') {
                return { ok: false, error: createLimiterError('BUCKET_FAILED', 'WAIT_FOR_CHANGE without subscribe') };
            }
            waitForChange = true;
        } else {
            waitMs = Math.max(waitMs, answer.data);
        }
    }
    return { ok: true, data: { waitMs, waitForChange } };
}

/**
 * **Atomic bucket reservation**: reserves capacity across all buckets only when none must wait.
 *
 * - checks all buckets first; skips reservations if any bucket requires waiting
 * - rolls back reservations on partial failure with empty settle
 * - returns `BUCKET_FAILED` on bucket errors
 */
export function tryReserveBuckets(
    buckets: readonly Bucket[],
    cost: Cost,
    now: number,
): Result<BucketReservationResult, LimiterError> {
    const check = checkBuckets(buckets, cost, now);
    if (!check.ok) {
        return check;
    }
    if (check.data.waitMs > 0 || check.data.waitForChange) {
        return { ok: true, data: { reserved: false, ...check.data } };
    }
    const reservations: unknown[] = [];
    for (let i = 0; i < buckets.length; i++) {
        const reserved = callSafely(() => buckets[i].reserve(cost, now));
        if (!reserved.ok) {
            for (let j = 0; j < i; j++) {
                callSafely(() => buckets[j].settle(reservations[j], {}, now));
            }
            return { ok: false, error: createLimiterError('BUCKET_FAILED', reserved.error) };
        }
        reservations.push(reserved.data);
    }
    return { ok: true, data: { reserved: true, reservations } };
}

// -------------------------------------------------------------------
// --- 4. Private Functions
// -------------------------------------------------------------------

/** Adds change listener; returns unsubscribe function. */
function addListener(listeners: (() => void)[], onChange: () => void): () => void {
    listeners.push(onChange);
    return () => {
        const index = listeners.indexOf(onChange);
        if (index !== -1) {
            listeners.splice(index, 1);
        }
    };
}

/** Validates sliding window configuration options. */
function validateSlidingWindowOptions(options: SlidingWindowOptions): Result<SlidingWindowConfig, string> {
    if (!isObject(options)) {
        return { ok: false, error: 'sliding window options must be an object' };
    }
    const { perMs, used = 0, ...units } = options;
    if ('per' in options) {
        return { ok: false, error: 'per (seconds) is now perMs (milliseconds)' };
    }
    if (!(isFiniteNonNegative(perMs) && perMs > 0)) {
        return { ok: false, error: 'perMs must be a positive number of milliseconds' };
    }
    const unitNames = Object.keys(units).filter((name) => units[name] !== undefined);
    if (unitNames.length !== 1) {
        return { ok: false, error: `expected exactly one unit next to perMs, got ${unitNames.length}` };
    }
    const unit = unitNames[0];
    const limit = units[unit];
    if (!(isFiniteNonNegative(limit) && limit > 0)) {
        return { ok: false, error: `${unit} must be a positive number` };
    }
    // Job minimum request cost is 1; lower limit would permanently block.
    if (unit === 'requests' && limit < 1) {
        return { ok: false, error: 'requests must be at least 1' };
    }
    if (!isValidPriorUsage(used)) {
        return { ok: false, error: 'used must be a non-negative number or an array of { at, amount? }' };
    }
    return { ok: true, data: { unit, limit, windowMs: perMs, used } };
}

/** Type guard validating numeric or timestamped array prior usage. */
function isValidPriorUsage(value: unknown): value is PriorUsage {
    if (isFiniteNonNegative(value)) {
        return true;
    }
    return (
        Array.isArray(value) &&
        value.every(
            (entry) =>
                isObject(entry) &&
                Number.isFinite(entry.at) &&
                (entry.amount === undefined || isFiniteNonNegative(entry.amount)),
        )
    );
}

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

/** Settled usage in sliding window. */
type Booking = { at: number; amount: number };

/** Handle of unsettled attempt; counts toward window until settled. */
type Reservation = { amount: number; isOpen: boolean };

/** Change subscribers of a bucket. */
type Listeners = { subscribe: (onChange: () => void) => () => void; notify: () => void; rotate: () => void };

/** Normalized configuration for sliding window bucket. */
type SlidingWindowConfig = { unit: string; limit: number; windowMs: number; used: PriorUsage };

/** Computes delay after which a `WAIT_FOR_CHANGE` answer changes without notification. */
type RecheckHint = (cost: Cost, now: number) => number;

/**
 * **Wait across buckets.**
 *
 * - `waitMs`: longest timed wait
 * - `waitForChange`: some bucket awaits change notification
 * - `recheckMs`: delay until next timed re-check; equals `waitMs` unless waiting for change, then delay until all
 *   `WAIT_FOR_CHANGE` buckets can change without notification (`Infinity` if any requires notification); not counted
 *   toward `maxWaitMs`
 */
export type BucketWait = { waitMs: number; waitForChange: boolean; recheckMs: number };

/** Result of reserving capacity across all buckets. */
export type BucketReservationResult = { reserved: true; reservations: unknown[] } | ({ reserved: false } & BucketWait);

// -------------------------------------------------------------------
// --- 2. Constants
// -------------------------------------------------------------------

/** Signal for `getWaitMs` indicating block until bucket `subscribe` reports change. */
export const WAIT_FOR_CHANGE = -1;

const HOOK_NAMES = ['getWaitMs', 'reserve', 'settle', 'subscribe'] as const;

/**
 * **Re-check hints of built-in buckets**: read by `checkBuckets` after a `WAIT_FOR_CHANGE` answer.
 *
 * - sliding window: settled bookings expire without notification while in-flight ones still block
 * - buckets without hint change only with notification
 */
const recheckHints = new WeakMap<Bucket, RecheckHint>();

const MS_PER_MINUTE = 60_000;
const MS_PER_DAY = 86_400_000;

// -------------------------------------------------------------------
// --- 3. Exported Functions
// -------------------------------------------------------------------

/**
 * **Creates bucket from hook definitions**: missing hooks default to "fits" and "nothing to do".
 *
 * - non-object `hooks` or non-function hook properties return a bucket with `configError` instead of throwing
 */
export function createBucket<R = undefined>(hooks: BucketHooks<R> = {}): Bucket {
    // Reading hooks can throw (getter, revoked Proxy).
    const problem = callSafely(() => findHookProblem(hooks));
    if (!problem.ok || problem.data !== undefined) {
        const cause = problem.ok ? problem.data : problem.error;
        return { ...createBucket(), configError: createLimiterError('INVALID_CONFIG', cause) };
    }
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
 * - notifies subscribers when slot frees; next subscriber goes first after each reservation
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
    const listeners = createListeners();

    return createBucket<{ isOpen: boolean }>({
        getWaitMs() {
            return inFlight < limit ? 0 : WAIT_FOR_CHANGE;
        },
        reserve() {
            inFlight++;
            listeners.rotate();
            return { isOpen: true };
        },
        settle(reservation) {
            if (!reservation?.isOpen) {
                return;
            }
            reservation.isOpen = false;
            inFlight--;
            listeners.notify();
        },
        subscribe: listeners.subscribe,
    });
}

/**
 * **Sliding window rate limiter bucket**: bounds usage to `limit` units within rolling `perMs` duration.
 *
 * - tracks timestamped settled usage within rolling window
 * - unsettled attempts count until settle, even when older than window; usage is booked at settle time
 * - notifies subscribers on every settle (estimate may shrink or booking moves); next subscriber goes first
 *   after each reservation
 * - wait counts in-flight bookings as freeable on settle: shortfall covered by them gives `WAIT_FOR_CHANGE`, else
 *   wait for settled bookings to cover rest (lower bound; avoids false `maxWaitMs` stops)
 * - `WAIT_FOR_CHANGE` comes with re-check hint for limiter: delay until expiring settled bookings make room
 *   while in-flight ones stay
 * - invalid options return bucket with `configError` instead of throwing
 * - supports bursts up to limit or steady pacing based on window size
 */
export function createSlidingWindow(options: SlidingWindowOptions): Bucket {
    // Reading options can throw (getter, revoked Proxy).
    const read = callSafely(() => validateSlidingWindowOptions(options));
    const config = read.ok ? read.data : read;
    if (!config.ok) {
        return { ...createBucket(), configError: createLimiterError('INVALID_CONFIG', config.error) };
    }
    const { unit, limit, windowMs, used } = config.data;
    // Ignores float residue of fractional amounts added to and removed from the totals.
    const tolerance = limit * 1e-9;
    // Settled usage in time order.
    const bookings: Booking[] =
        typeof used === 'number'
            ? []
            : used.map((entry) => ({ at: entry.at, amount: entry.amount ?? 1 })).sort((a, b) => a.at - b.at);
    let settledInWindow = bookings.reduce((sum, booking) => sum + booking.amount, 0);
    // Unsettled attempts; never age out, so their settle always wakes waiting limiters.
    let inFlight = 0;
    let inFlightCount = 0;
    const listeners = createListeners();
    // Numeric prior usage booked on first call timestamp.
    let unbookedPriorUsage = typeof used === 'number' ? used : 0;
    let isFirstCall = true;
    // Index of oldest valid booking; pruned in batches.
    let head = 0;

    /** Advances window time, prunes expired bookings, and books initial prior usage. */
    function moveWindowTo(now: number): number {
        if (isFirstCall) {
            isFirstCall = false;
            for (const booking of bookings) {
                booking.at = Math.min(booking.at, now);
            }
            if (unbookedPriorUsage > 0) {
                bookings.push({ at: now, amount: unbookedPriorUsage });
                settledInWindow += unbookedPriorUsage;
                unbookedPriorUsage = 0;
            }
        }
        const time = Math.max(now, bookings.at(-1)?.at ?? Number.NEGATIVE_INFINITY);
        while (head < bookings.length && bookings[head].at + windowMs <= time) {
            settledInWindow -= bookings[head].amount;
            head++;
        }
        if (head === bookings.length) {
            // Drops float residue of fractional amounts.
            settledInWindow = 0;
        }
        if (head >= 1024 && head * 2 >= bookings.length) {
            bookings.splice(0, head);
            head = 0;
        }
        return time;
    }

    /** Walks oldest settled bookings until `excess` has left the window; returns wait until then. */
    function getExpiryWaitMs(excess: number, time: number): number {
        let rest = excess;
        let waitMs = 0;
        for (let i = head; i < bookings.length && rest > tolerance; i++) {
            rest -= bookings[i].amount;
            waitMs = bookings[i].at + windowMs - time;
        }
        return Math.max(waitMs, 0);
    }

    const bucket = createBucket<Reservation>({
        getWaitMs(cost, now) {
            const amount = cost[unit] ?? 0;
            if (amount > limit) {
                return Number.POSITIVE_INFINITY;
            }
            const time = moveWindowTo(now);
            const excess = settledInWindow + inFlight + amount - limit;
            if (excess <= tolerance) {
                return 0;
            }
            // In-flight bookings may settle and free capacity at any time.
            if (excess - inFlight <= tolerance) {
                return WAIT_FOR_CHANGE;
            }
            return getExpiryWaitMs(excess - inFlight, time);
        },
        reserve(cost, now) {
            moveWindowTo(now);
            const reservation = { amount: cost[unit] ?? 0, isOpen: true };
            inFlight += reservation.amount;
            inFlightCount++;
            listeners.rotate();
            return reservation;
        },
        settle(reservation, actualCost, now) {
            if (!reservation?.isOpen) {
                return;
            }
            reservation.isOpen = false;
            inFlightCount--;
            // Reset on last settle drops float residue of fractional amounts.
            inFlight = inFlightCount === 0 ? 0 : inFlight - reservation.amount;
            // Books actual usage at settle timestamp; keeps list in time order.
            const time = moveWindowTo(now);
            const amount = actualCost[unit] ?? 0;
            if (amount > 0) {
                bookings.push({ at: time, amount });
                settledInWindow += amount;
            }
            listeners.notify();
        },
        subscribe: listeners.subscribe,
    });
    // Settled bookings expire without notification: returns delay until they make room alongside in-flight bookings.
    recheckHints.set(bucket, (cost, now) => {
        const amount = cost[unit] ?? 0;
        if (inFlight + amount - limit > tolerance) {
            return Number.POSITIVE_INFINITY;
        }
        const time = moveWindowTo(now);
        return getExpiryWaitMs(settledInWindow + inFlight + amount - limit, time);
    });
    return bucket;
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
    let changeRecheckMs = 0;
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
            const hint = recheckHints.get(bucket);
            changeRecheckMs = Math.max(changeRecheckMs, hint ? hint(cost, now) : Number.POSITIVE_INFINITY);
        } else {
            waitMs = Math.max(waitMs, answer.data);
        }
    }
    return { ok: true, data: { waitMs, waitForChange, recheckMs: Math.max(waitMs, changeRecheckMs) } };
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

/**
 * **Creates subscriber list with round-robin notification**: next subscriber goes first after each reservation.
 *
 * - limiters dispatch in notification order; fixed order lets first subscriber take all freed capacity
 * - `rotate` (called on reserve): advances starting subscriber by one for subsequent notifications
 * - preserves notification order when no reservation occurred in between, so first subscriber keeps its turn until capacity suffices
 * - snapshot per notification supports unsubscribes while notifying
 * - throwing subscriber does not stop the others
 */
function createListeners(): Listeners {
    const listeners: (() => void)[] = [];
    let first = 0;
    let isRotationDue = false;
    return {
        subscribe(onChange) {
            listeners.push(onChange);
            return () => {
                const index = listeners.indexOf(onChange);
                if (index !== -1) {
                    listeners.splice(index, 1);
                }
            };
        },
        notify() {
            const current = [...listeners];
            if (isRotationDue) {
                isRotationDue = false;
                first++;
            }
            first = current.length > 0 ? first % current.length : 0;
            for (let i = 0; i < current.length; i++) {
                callSafely(current[(first + i) % current.length]);
            }
        },
        rotate() {
            isRotationDue = true;
        },
    };
}

/** Returns error message when `hooks` is not an object or has a non-function hook property. */
function findHookProblem<R>(hooks: BucketHooks<R>): string | undefined {
    if (!isObject(hooks)) {
        return 'bucket hooks must be an object';
    }
    const invalid = HOOK_NAMES.find((name) => hooks[name] !== undefined && typeof hooks[name] !== 'function');
    return invalid && `${invalid} must be a function`;
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

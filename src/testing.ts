// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { Clock } from './types.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Test clock allowing deterministic manual time advancement. */
export type FakeClock = Clock & {
    /** Advances virtual time by specified milliseconds, firing elapsed timers in order. */
    advance(ms: number): Promise<void>;
    /**
     * Runs virtual clock until promise settles, advancing timers as needed.
     * @throws When promise never settles or timers run out.
     */
    settle<T>(promise: Promise<T>): Promise<T>;
};

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/**
 * **Creates virtual fake clock for deterministic testing.**
 *
 * - advances time only when stepped via `advance` or `settle`
 * - replaces system clock in `createLimiter({ clock })`
 */
export function createFakeClock(start = 0): FakeClock {
    let time = start;
    let lastId = 0;
    const timers = new Map<number, { at: number; callback: () => void }>();

    /** Finds earliest timer due before or at limit. */
    function nextDue(limit: number) {
        let due: [number, { at: number; callback: () => void }] | undefined;
        for (const timer of timers) {
            if (timer[1].at <= limit && (!due || timer[1].at < due[1].at)) {
                due = timer;
            }
        }
        return due;
    }

    /** Fires next due timer; returns true if a timer fired. */
    async function fire(limit: number): Promise<boolean> {
        const due = nextDue(limit);
        if (!due) {
            return false;
        }
        timers.delete(due[0]);
        time = Math.max(time, due[1].at);
        due[1].callback();
        await flush();
        return true;
    }

    return {
        now: () => time,
        setTimeout(callback, ms) {
            lastId++;
            timers.set(lastId, { at: time + Math.max(0, ms), callback });
            return lastId;
        },
        clearTimeout(timer) {
            timers.delete(timer as number);
        },
        async advance(ms) {
            const target = time + ms;
            await flush();
            while (await fire(target)) {}
            time = target;
            await flush();
        },
        async settle(promise) {
            let settled = false;
            let value: unknown;
            let rejection: { reason: unknown } | undefined;
            void promise.then(
                (v) => {
                    settled = true;
                    value = v;
                },
                (reason: unknown) => {
                    settled = true;
                    rejection = { reason };
                },
            );
            await flush();
            for (let i = 0; i < 100_000 && !settled; i++) {
                if (!(await fire(Number.POSITIVE_INFINITY))) {
                    await flush();
                    if (!settled && timers.size === 0) {
                        throw new Error('promise never settles: no timers left');
                    }
                }
            }
            if (!settled) {
                throw new Error('promise never settles: still pending after 100000 timers');
            }
            if (rejection) {
                throw rejection.reason;
            }
            return value as Awaited<typeof promise>;
        },
    };
}

// -------------------------------------------------------------------
// --- 3. Private Functions
// -------------------------------------------------------------------

/** Yields microtasks and immediate turns to settle pending promises. */
async function flush(): Promise<void> {
    for (let i = 0; i < 3; i++) {
        await new Promise((resolve) => setImmediate(resolve));
    }
}

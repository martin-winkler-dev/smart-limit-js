// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { Awaitable, Result } from './types.js';

// -------------------------------------------------------------------
// --- 1. Constants
// -------------------------------------------------------------------

/** Longest delay setTimeout supports before 32-bit overflow (2^31 - 1 ms). */
export const MAX_TIMER_MS = 2_147_483_647;

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/** Checks whether value is a non-null object. */
export function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

/** Checks whether value is a finite non-negative number. */
export function isFiniteNonNegative(value: unknown): value is number {
    return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

/** Returns shallow copy of cost or undefined if invalid. */
export function validateCost(value: unknown): Record<string, number> | undefined {
    if (!isObject(value) || Array.isArray(value)) {
        return undefined;
    }
    const cost: Record<string, number> = {};
    for (const [unit, amount] of Object.entries(value)) {
        if (!isFiniteNonNegative(amount)) {
            return undefined;
        }
        cost[unit] = amount;
    }
    return cost;
}

/**
 * **Safely executes function**: catches throws and forwards async rejections.
 *
 * - returns Result instead of throwing
 * - does not await returned promises
 * - passes caught promise rejections to `onRejected`
 */
export function callSafely<T>(fn: () => T, onRejected: (error: unknown) => void = () => {}): Result<T, unknown> {
    try {
        const data = fn();
        if (data instanceof Promise) {
            data.catch(onRejected);
        }
        return { ok: true, data };
    } catch (error) {
        return { ok: false, error };
    }
}

/** Safely executes sync or async function returning Result, catching throws and rejections. */
export async function callSafelyAsync<T>(fn: () => Awaitable<T>): Promise<Result<T, unknown>> {
    try {
        return { ok: true, data: await fn() };
    } catch (error) {
        return { ok: false, error };
    }
}

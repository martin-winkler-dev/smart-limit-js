// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { callSafely, callSafelyAsync, isFiniteNonNegative, isObject, validateCost } from '../../src/util.js';

describe('isObject', () => {
    it('accepts objects and arrays', () => {
        expect(isObject({})).toBe(true);
        expect(isObject([])).toBe(true);
        expect(isObject(new Headers())).toBe(true);
    });

    it('rejects null, primitives and functions', () => {
        for (const value of [null, undefined, 0, 'x', true, Symbol('s'), () => 1]) {
            expect(isObject(value)).toBe(false);
        }
    });
});

describe('isFiniteNonNegative', () => {
    it('accepts finite numbers >= 0', () => {
        for (const value of [0, -0, 1, 0.5, Number.MAX_SAFE_INTEGER]) {
            expect(isFiniteNonNegative(value)).toBe(true);
        }
    });

    it('rejects negative, non-finite and non-number values', () => {
        for (const value of [-1, -0.1, Number.NaN, Number.POSITIVE_INFINITY, '1', null, undefined, {}]) {
            expect(isFiniteNonNegative(value)).toBe(false);
        }
    });
});

describe('validateCost', () => {
    it('returns a copy of a valid cost', () => {
        const cost = { requests: 1, tokens: 1_200, free: 0 };
        const result = validateCost(cost);
        expect(result).toEqual(cost);
        expect(result).not.toBe(cost);
    });

    it('accepts an empty object', () => {
        expect(validateCost({})).toEqual({});
    });

    it('returns undefined for non-objects and arrays', () => {
        for (const value of [undefined, null, 1, 'requests', true, [1, 2]]) {
            expect(validateCost(value)).toBeUndefined();
        }
    });

    it('returns undefined when any amount is invalid', () => {
        for (const amount of [-1, Number.NaN, Number.POSITIVE_INFINITY, '1', null, undefined]) {
            expect(validateCost({ requests: 1, tokens: amount })).toBeUndefined();
        }
    });
});

describe('callSafely', () => {
    it('returns the value as a success', () => {
        expect(callSafely(() => 42)).toEqual({ ok: true, data: 42 });
    });

    it('returns a throw as an error result', () => {
        const error = new Error('boom');
        expect(
            callSafely(() => {
                throw error;
            }),
        ).toEqual({ ok: false, error });
    });

    it('does not await promises', () => {
        const promise = Promise.resolve(1);
        const result = callSafely(() => promise);
        expect(result).toEqual({ ok: true, data: promise });
    });

    it('hands the rejection of a returned promise to onRejected', async () => {
        const error = new Error('later');
        const rejections: unknown[] = [];
        const result = callSafely(
            () => Promise.reject(error),
            (reason) => rejections.push(reason),
        );
        expect(result.ok).toBe(true);
        await Promise.resolve();
        await Promise.resolve();
        expect(rejections).toEqual([error]);
    });
});

describe('callSafelyAsync', () => {
    it('returns a resolved value as a success', async () => {
        expect(await callSafelyAsync(async () => 'done')).toEqual({ ok: true, data: 'done' });
    });

    it('accepts a sync function', async () => {
        expect(await callSafelyAsync(() => 7)).toEqual({ ok: true, data: 7 });
    });

    it('catches a sync throw', async () => {
        const error = new Error('sync');
        const result = await callSafelyAsync(() => {
            throw error;
        });
        expect(result).toEqual({ ok: false, error });
    });

    it('catches a throw inside an async function', async () => {
        const error = new Error('async');
        const result = await callSafelyAsync(async () => {
            throw error;
        });
        expect(result).toEqual({ ok: false, error });
    });

    it('catches a rejected promise', async () => {
        const result = await callSafelyAsync(() => Promise.reject('nope'));
        expect(result).toEqual({ ok: false, error: 'nope' });
    });
});

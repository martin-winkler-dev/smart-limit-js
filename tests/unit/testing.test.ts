// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createFakeClock } from '../../src/testing.js';

describe('createFakeClock', () => {
    it('fires timers in time order on advance and skips cleared ones', async () => {
        const clock = createFakeClock(100);
        const fired: [string, number][] = [];
        clock.setTimeout(() => fired.push(['b', clock.now()]), 20);
        clock.setTimeout(() => fired.push(['a', clock.now()]), 10);
        const cleared = clock.setTimeout(() => fired.push(['cleared', clock.now()]), 5);
        clock.clearTimeout(cleared);
        clock.setTimeout(() => fired.push(['late', clock.now()]), 50);
        await clock.advance(30);
        expect(fired).toEqual([
            ['a', 110],
            ['b', 120],
        ]);
        expect(clock.now()).toBe(130);
    });

    it('treats a negative delay as 0', async () => {
        const clock = createFakeClock();
        let at = -1;
        clock.setTimeout(() => {
            at = clock.now();
        }, -5);
        await clock.advance(0);
        expect(at).toBe(0);
    });

    it('settle jumps from timer to timer until the promise settles', async () => {
        const clock = createFakeClock();
        const value = clock.settle(
            new Promise<string>((resolve) => clock.setTimeout(() => resolve(`at ${clock.now()}`), 3_600_000)),
        );
        expect(await value).toBe('at 3600000');
    });

    it('settle gives work outside the fake timers one more flush before giving up', async () => {
        const clock = createFakeClock();
        // Resolves after 4 macrotasks between settle's initial and final flushes.
        const value = new Promise<string>((resolve) => {
            let turns = 0;
            function turn() {
                turns++;
                if (turns === 4) {
                    resolve('outside');
                } else {
                    setImmediate(turn);
                }
            }
            setImmediate(turn);
        });
        expect(await clock.settle(value)).toBe('outside');
    });

    it('settle throws instead of hanging when no timer is left', async () => {
        const clock = createFakeClock();
        await expect(clock.settle(new Promise(() => {}))).rejects.toThrow('promise never settles: no timers left');
    });

    it('settle throws instead of returning undefined when timers keep re-arming forever', async () => {
        const clock = createFakeClock();
        function rearm() {
            clock.setTimeout(rearm, 1);
        }
        rearm();
        await expect(clock.settle(new Promise(() => {}))).rejects.toThrow('still pending after 100000 timers');
    });

    it('settle rejects with the reason of a promise that rejects, without an unhandled rejection', async () => {
        const unhandled: unknown[] = [];
        function onUnhandled(reason: unknown) {
            unhandled.push(reason);
        }
        process.on('unhandledRejection', onUnhandled);
        try {
            const clock = createFakeClock();
            const promise = new Promise((_resolve, reject) => clock.setTimeout(() => reject(new Error('boom')), 10));
            await expect(clock.settle(promise)).rejects.toThrow('boom');
            expect(clock.now()).toBe(10);
            await new Promise((resolve) => setImmediate(resolve));
        } finally {
            process.off('unhandledRejection', onUnhandled);
        }
        expect(unhandled).toEqual([]);
    });
});

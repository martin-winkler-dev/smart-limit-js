// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { type Bucket, createConcurrencyBucket, createQueue, createSlidingWindow, type Queue } from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

type Start = { queue: string; at: number; pushedAt: number };

type Setup = { clock: FakeClock; starts: Start[]; queues: Record<string, Queue<number, number>> };

function sleep(clock: FakeClock, ms: number): Promise<void> {
    return new Promise((resolve) => clock.setTimeout(resolve, ms));
}

/** Creates queues on a shared concurrency bucket and sliding window. */
function setup(names: string[], concurrency: number, requestsPerSecond: number, durationMs: number): Setup {
    const clock = createFakeClock();
    const starts: Start[] = [];
    const buckets: Bucket[] = [
        createConcurrencyBucket(concurrency),
        createSlidingWindow({ requests: requestsPerSecond, perMs: 1000 }),
    ];
    const queues: Record<string, Queue<number, number>> = {};
    for (const name of names) {
        queues[name] = createQueue<number, number>({
            clock,
            concurrency,
            buckets,
            processJob: async (pushedAt) => {
                starts.push({ queue: name, at: clock.now(), pushedAt });
                await sleep(clock, durationMs);
                return { ok: true, data: pushedAt };
            },
        });
    }
    return { clock, starts, queues };
}

/** Pushes multiple jobs into queue simultaneously. */
function pushBacklog(clock: FakeClock, queue: Queue<number, number>, count: number): Promise<unknown> {
    return Promise.all(Array.from({ length: count }, () => queue.push(clock.now())));
}

/** Periodically pushes jobs at fixed intervals until deadline. */
async function pushTrickle(clock: FakeClock, queue: Queue<number, number>, everyMs: number, untilMs: number) {
    const pushed: Promise<unknown>[] = [];
    for (let at = everyMs; at <= untilMs; at += everyMs) {
        await sleep(clock, at - clock.now());
        pushed.push(queue.push(clock.now()));
    }
    await Promise.all(pushed);
    return pushed.length;
}

/** Computes proportional start share per queue before deadline. */
function shares(starts: Start[], untilMs: number): Record<string, number> {
    const early = starts.filter((start) => start.at < untilMs);
    const counts: Record<string, number> = {};
    for (const start of early) {
        counts[start.queue] = (counts[start.queue] ?? 0) + 1;
    }
    return Object.fromEntries(Object.entries(counts).map(([name, count]) => [name, count / early.length]));
}

/** Returns latest start timestamp for given queue. */
function lastStart(starts: Start[], queue: string): number {
    return Math.max(...starts.filter((start) => start.queue === queue).map((start) => start.at));
}

/** Asserts window-bound equal load progresses evenly across queues. */
async function expectEqualSplitOnWindow(names: string[]) {
    const { clock, starts, queues } = setup(names, 10, 5, 10);
    await clock.settle(Promise.all(['a', 'b'].map((name) => pushBacklog(clock, queues[name], 100))));
    const bothBusyUntil = Math.min(lastStart(starts, 'a'), lastStart(starts, 'b'));
    for (let from = 0; from + 1000 <= bothBusyUntil; from += 1000) {
        const inWindow = starts.filter((start) => start.at >= from && start.at < from + 1000);
        expect({ from, a: inWindow.some((start) => start.queue === 'a') }).toEqual({ from, a: true });
        expect({ from, b: inWindow.some((start) => start.queue === 'b') }).toEqual({ from, b: true });
    }
    const split = shares(starts, 10_000);
    expect(split.a).toBeGreaterThanOrEqual(0.4);
    expect(split.a).toBeLessThanOrEqual(0.6);
}

describe('fairness of queues on shared buckets', () => {
    it('window-bound, equal load: both queues start jobs in every window and split evenly', async () => {
        await expectEqualSplitOnWindow(['a', 'b']);
    });

    it('window-bound, equal load: order of creation does not matter', async () => {
        await expectEqualSplitOnWindow(['b', 'a']);
    });

    it('concurrency-bound, equal load: both queues split evenly', async () => {
        const { clock, starts, queues } = setup(['a', 'b'], 2, 1000, 100);
        await clock.settle(Promise.all(['a', 'b'].map((name) => pushBacklog(clock, queues[name], 100))));
        const split = shares(starts, 2000);
        expect(split.a).toBeGreaterThanOrEqual(0.4);
        expect(split.a).toBeLessThanOrEqual(0.6);
    });

    it('window-bound, trickle vs backlog: every trickle job starts within 2,000 ms of its push', async () => {
        const { clock, starts, queues } = setup(['a', 'b'], 10, 5, 10);
        const backlog = pushBacklog(clock, queues.a, 500);
        const count = await clock.settle(pushTrickle(clock, queues.b, 500, 20_000));
        await clock.settle(backlog);
        const trickle = starts.filter((start) => start.queue === 'b');
        expect(trickle).toHaveLength(count);
        expect(Math.max(...trickle.map((start) => start.at - start.pushedAt))).toBeLessThanOrEqual(2000);
    });

    it('concurrency-bound, trickle vs backlog: every trickle job starts within two job durations of its push', async () => {
        const { clock, starts, queues } = setup(['a', 'b'], 2, 1000, 100);
        const backlog = pushBacklog(clock, queues.a, 500);
        const count = await clock.settle(pushTrickle(clock, queues.b, 300, 20_000));
        await clock.settle(backlog);
        const trickle = starts.filter((start) => start.queue === 'b');
        expect(trickle).toHaveLength(count);
        expect(Math.max(...trickle.map((start) => start.at - start.pushedAt))).toBeLessThanOrEqual(2 * 100 + 1);
    });

    it('three queues, window-bound: each gets a fair share', async () => {
        const names = ['a', 'b', 'c'];
        const { clock, starts, queues } = setup(names, 10, 6, 10);
        await clock.settle(Promise.all(names.map((name) => pushBacklog(clock, queues[name], 60))));
        const split = shares(starts, 6000);
        for (const name of names) {
            expect(split[name] ?? 0).toBeGreaterThanOrEqual(0.25);
            expect(split[name] ?? 0).toBeLessThanOrEqual(0.42);
        }
    });
});

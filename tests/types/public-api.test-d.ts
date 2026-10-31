// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { expectTypeOf } from 'bun:test';
import {
    type Bucket,
    createConcurrencyBucket,
    createLimiter,
    createQueue,
    createRPM,
    createSlidingWindow,
    type JobOutcome,
    type JobOutcomeStatus,
    type LimiterError,
    type LimiterState,
    type LimiterStatus,
    type QueueStatus,
    type Result,
    type RunAllResult,
    type RunSummary,
    runAll,
    WAIT_FOR_CHANGE,
} from '../../src/index.js';
import { createFakeClock, type FakeClock } from '../../src/testing.js';

type Paper = { id: number; abstract: string };

const limiter = createLimiter<Paper, { include: boolean }>({
    fetchJob: () => null,
    processJob: async () => ({ ok: true, data: { include: true } }),
    onJobDone: (paper, result) => {
        expectTypeOf(paper).toEqualTypeOf<Paper>();
        expectTypeOf(result.status).toEqualTypeOf<JobOutcomeStatus>();
        if (result.ok) {
            expectTypeOf(result.status).toEqualTypeOf<'done'>();
            expectTypeOf(result.data).toEqualTypeOf<{ include: boolean }>();
        } else {
            expectTypeOf(result.status).toEqualTypeOf<'failed' | 'released'>();
            expectTypeOf(result.error).toEqualTypeOf<LimiterError>();
        }
    },
});

expectTypeOf(limiter.start()).toEqualTypeOf<Promise<Result<RunSummary, LimiterError>>>();
expectTypeOf(limiter.start({ force: true })).toEqualTypeOf<Promise<Result<RunSummary, LimiterError>>>();
expectTypeOf(createRPM(60)).toEqualTypeOf<Bucket>();
expectTypeOf(createSlidingWindow({ inputTokens: 30_000, perMs: 60_000 })).toEqualTypeOf<Bucket>();
expectTypeOf(createConcurrencyBucket(2)).toEqualTypeOf<Bucket>();
expectTypeOf(WAIT_FOR_CHANGE).toEqualTypeOf<number>();
expectTypeOf(createFakeClock()).toEqualTypeOf<FakeClock>();
expectTypeOf(limiter.stop({ abort: true })).toEqualTypeOf<Promise<Result<RunSummary, LimiterError>>>();
expectTypeOf(limiter.wake()).toEqualTypeOf<LimiterStatus>();
expectTypeOf(limiter.isIdle()).toEqualTypeOf<boolean>();
expectTypeOf(limiter.getStatus()).toEqualTypeOf<LimiterStatus>();
expectTypeOf(limiter.getStatus().state).toEqualTypeOf<LimiterState>();
expectTypeOf(limiter.getStatus().lastStoppedBy).toEqualTypeOf<
    'empty' | 'stop' | 'max_wait_exceeded' | 'error' | undefined
>();

const all = runAll<Paper, boolean>([], { processJob: () => ({ ok: true, data: true }) });
expectTypeOf(all).toEqualTypeOf<Promise<RunAllResult<boolean>>>();

const queue = createQueue<Paper, boolean>({ processJob: () => ({ ok: true, data: true }) });
expectTypeOf(
    queue.push({ id: 1, abstract: '' }, { cost: { tokens: 10 }, signal: AbortSignal.timeout(1000) }),
).toEqualTypeOf<Promise<JobOutcome<boolean>>>();
queue.push({ id: 1, abstract: '' }, { priority: -1 });
// @ts-expect-error priority must be a number
queue.push({ id: 1, abstract: '' }, { priority: '1' });
expectTypeOf(queue.getStatus()).toEqualTypeOf<QueueStatus>();
expectTypeOf(queue.getStatus().waiting).toEqualTypeOf<number>();
expectTypeOf(queue.stop({ abort: true })).toEqualTypeOf<Promise<Result<RunSummary, LimiterError>>>();
// @ts-expect-error queue manages own source; fetchJob is disallowed
createQueue<Paper, boolean>({ processJob: () => ({ ok: true, data: true }), fetchJob: () => null });

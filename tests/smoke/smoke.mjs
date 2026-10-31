// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import * as pkg from 'smart-limit';
import * as testingPkg from 'smart-limit/testing';

assert.equal(typeof pkg.createLimiter, 'function');
assert.equal(typeof pkg.createBucket, 'function');
assert.equal(typeof pkg.createSlidingWindow, 'function');
assert.equal(typeof pkg.createConcurrencyBucket, 'function');
assert.equal(typeof pkg.runAll, 'function');
assert.equal(typeof pkg.createQueue, 'function');
assert.equal(typeof pkg.createRPM, 'function');
assert.equal(typeof pkg.createTPM, 'function');
assert.equal(typeof pkg.createRPD, 'function');
assert.equal(typeof pkg.createTPD, 'function');
assert.equal(typeof pkg.createLimiterError, 'function');
assert.equal(typeof pkg.WAIT_FOR_CHANGE, 'number');
assert.equal(pkg.ERROR_CODES.FATAL.length > 0, true);

assert.equal(typeof testingPkg.createFakeClock, 'function');

const limiter = pkg.createLimiter({
    fetchJob: () => null,
    processJob: () => ({ ok: true, data: null }),
    buckets: [pkg.createRPM(60)],
});
assert.deepEqual(await limiter.start(), {
    ok: true,
    data: { done: 0, failed: 0, released: 0, stoppedBy: 'empty' },
});
assert.equal(limiter.isIdle(), true);

const clock = testingPkg.createFakeClock();
const all = pkg.runAll([{ job: 1 }, { job: 2 }], {
    clock,
    buckets: [pkg.createConcurrencyBucket(1)],
    processJob: (job) => ({ ok: true, data: job * 2 }),
});
assert.deepEqual(await clock.settle(all), {
    ok: true,
    data: [
        { ok: true, status: 'done', data: 2 },
        { ok: true, status: 'done', data: 4 },
    ],
});

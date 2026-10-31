// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

const assert = require('node:assert/strict');
const pkg = require('smart-limit');
const testingPkg = require('smart-limit/testing');

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
});
assert.equal(limiter.isIdle(), true);

const clock = testingPkg.createFakeClock(42);
assert.equal(clock.now(), 42);

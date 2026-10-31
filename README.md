# smart-limit

[![Test](https://github.com/martin-winkler-dev/smart-limit-js/actions/workflows/test.yml/badge.svg)](https://github.com/martin-winkler-dev/smart-limit-js/actions/workflows/test.yml)
[![Package](https://github.com/martin-winkler-dev/smart-limit-js/actions/workflows/package.yml/badge.svg)](https://github.com/martin-winkler-dev/smart-limit-js/actions/workflows/package.yml)
[![Bun](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmartin-winkler-dev%2Fsmart-limit-js%2Fmain%2Fpackage.json&query=%24.engines.bun&label=Bun&logo=bun&color=blue)](https://bun.sh)
[![Node.js](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmartin-winkler-dev%2Fsmart-limit-js%2Fmain%2Fpackage.json&query=%24.engines.node&label=Node.js&logo=nodedotjs&color=blue)](https://nodejs.org)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Runs high-volume API calls as fast as limits allow: concurrency, requests and tokens per time window, per-item retries, and single-probe recovery after rate limits or server outages. Zero runtime dependencies; no classes; never throws.

## Install

```sh
bun add smart-limit
# or
npm install smart-limit
```

ESM package with TypeScript types for Node.js >= 22.12 (`require()` supported) and Bun >= 1.3.

## Choosing an Entry Point

| Use Case | Entry Point | Behavior |
|---|---|---|
| Database or external queue with persistent jobs | `createLimiter({ fetchJob, processJob, onJobDone })` | Pulls jobs on demand; caller persists outcomes; see [External store integration](#external-store-integration) |
| Static array of items | `runAll(items, options)` | Batch execution preserving input order; see [A fixed list](#a-fixed-list) |
| Independent callers awaiting individual results (API wrapper) | `createQueue(options)` and `push(job)` | In-memory priority queue; see [A queue in memory](#a-queue-in-memory) |

All three entry points share the same options, bucket engine, and error classification.

## How It Works

1. **Pull (`fetchJob`):** Leases `{ job, cost? }` from an external store when a concurrency slot is free and buckets fit `{ requests: 1 }`.
2. **Execute (`processJob`):** Runs the attempt with `{ attempt, signal }` (`attempt` starts at `1`), returning `{ ok: true, data, usage? }` or `{ ok: false, error }`.
3. **Settle:** Buckets adjust capacity reservations to actual reported `usage`.
4. **Complete (`onJobDone`):** Reports final resolution (`'done'`, `'failed'`, or `'released'`) once per leased job to update the external store.

- **Passive counters:** Buckets calculate capacity: run now (`0`), wait `N` ms, or reject (`Infinity`).
- **Demand-driven pulling:** Ready retries in the buffer dispatch before pulling new jobs. Because job-specific `cost` (e.g. tokens) is unknown until pulled, the limiter pre-checks `{ requests: 1 }`, slot availability, and `buffered < maxBufferSize` (default `concurrency`) before calling `fetchJob()` (one call in flight at a time). Once pulled, buckets reserve the full `{ requests: 1, ...cost }` before execution. When capacity is unavailable, a single wake-up timer is armed instead of polling.
- **Global lock & single probe:**
  - On `backoff_global` (e.g. HTTP 429, 503, network drop), the limiter locks all slots and pauses new starts.
  - Once the backoff window elapses, the limiter dispatches exactly **one probe job** (`queue[0]`).
  - **Unlocking:** Only a successful probe (`ok: true`) clears the lock and opens remaining slots.
  - **Probe failure & rotation:** When a probe fails with a per-job error (`retry_backoff`, `retry_immediate`, or `fatal`), the failed job moves to the end of the queue (or settles if terminal) so a single broken request cannot monopolize the probe slot. The lock remains active, and the next eligible job probes alone. On another `backoff_global`, the probe remains at the front and awaits the new backoff duration.
  - **Lock timeout (`maxWaitMs`):** If no probe succeeds within `maxWaitMs`, the run halts (`stoppedBy: 'max_wait_exceeded'`) and releases all buffered jobs (`status: 'released'`). If the next scheduled backoff or wake-up would exceed `maxWaitMs`, the run halts immediately without waiting. An in-flight probe is not aborted at the deadline: if it succeeds, the lock clears and the run continues. The lock persists across runs so the first job of the next `start()` probes alone (unless cleared via `start({ force: true })`).

## Quickstart

```ts
import { createLimiter, createRPM, createTPM } from 'smart-limit';

const limiter = createLimiter({
    concurrency: 5,
    buckets: [createRPM(60), createTPM(200_000)],
    // Lease one job at a time from DB/queue; return null/undefined when empty
    fetchJob: async () => {
        const job = await claimNextOpenJob();
        if (!job) return null;
        return { job, cost: { tokens: Math.ceil(job.text.length / 4) + 300 } };
    },
    // One attempt; classify every error (see "Classifying errors" below)
    processJob: async (job, { signal }) => {
        try {
            const res = await sdk.generate(job.text, { signal });
            return { ok: true, data: res.output, usage: { tokens: res.usage.totalTokens } };
        } catch (err) {
            return { ok: false, error: classifyError(err, signal) };
        }
    },
    onAttempt: (job, result, info) => logAttempt(job, result, info),
    onJobDone: async (job, result) => {
        if (result.status === 'done') await markDone(job.id, result.data);
        else if (result.status === 'failed') await markFailed(job.id, result.error);
        else await markOpen(job.id); // 'released' on stop() or max_wait_exceeded
    },
    log: ({ level, message, data }) => console[level](message, data ?? ''),
});

// Wake the limiter when producers insert new jobs:
onNewJobsInserted(() => limiter.wake());

process.on('SIGINT', () => limiter.stop({ abort: true }));

const result = await limiter.start();
// { ok: true, data: { done, failed, released, stoppedBy: 'empty' | 'stop' | 'max_wait_exceeded' } }
// or { ok: false, error: LimiterError }
```

### External Store Integration

State resides in memory; the external store owns persistence and leasing:

- **`fetchJob` leases a job:** Atomically claims a job in the database or queue (e.g. `status = 'running'`). The limiter calls `fetchJob` one at a time, holding at most `concurrency + maxBufferSize` leased jobs. Returning `null` or `undefined` signals an empty source; the run finishes once in-flight jobs settle (`stoppedBy: 'empty'`), unless woken via `wake()`.
- **`onJobDone` records the outcome:** Every leased job reaches `onJobDone` once with `'done'`, `'failed'`, or `'released'` (to be returned to open state in the store). If `onJobDone` throws, the limiter logs `HOOK_FAILED` and continues; the callback is not retried.
- **Crash recovery:** If the process terminates abruptly, leased jobs never reach `onJobDone`. Stale leases must be reset on startup or expired via lease deadlines.
- **No callback timeouts:** Only `processJob` observes `timeoutMs`. Unsettled promises in `fetchJob`, `onAttempt`, or `onJobDone` block progress indefinitely.
- **`processJob` must return results:** Thrown exceptions fail the job immediately without retry (`fatal`, `PROCESS_JOB_THREW`). Malformed results fail with `INVALID_RESULT`. Errors must be caught and returned as `{ ok: false, error }`.

## Buckets

| Factory | Limit Type | Description |
|---|---|---|
| `createRPM(n, options?)` / `createRPD(n, options?)` | Requests per minute / day | Sliding window over 60_000 ms or 86_400_000 ms |
| `createTPM(n, options?)` / `createTPD(n, options?)` | Tokens per minute / day | Sliding window over 60_000 ms or 86_400_000 ms |
| `createSlidingWindow({ <unit>: n, perMs: ms, used? })` | Arbitrary unit & window | Rolling window for one positive unit limit (`requests >= 1`) over `perMs > 0`; e.g. `{ requests: 1, perMs: 200 }` paces requests |
| `createConcurrencyBucket(n)` | Cross-limiter concurrency | Caps in-flight attempts at `n` across all limiters sharing the instance |
| `createBucket({ getWaitMs, reserve, settle, subscribe? })` | Custom rate logic | User-defined rules; omitted hooks default to "fits immediately" / no-op |

- **Sliding windows:** Built-in rate buckets enforce rolling windows: usage never exceeds `n` units within any rolling `perMs` window.
- **Pacing requests:** `concurrency` caps simultaneous executions. At startup or after a lock clears, up to `concurrency` jobs start in parallel. A short sliding-window bucket such as `createSlidingWindow({ requests: 1, perMs: 200 })` eliminates bursts: since usage is booked on completion, each start waits `perMs` after the previous request finishes.
- **Cost validation:** Each job defaults to `{ requests: 1, ...cost }` from `fetchJob`. Buckets read only their assigned unit key. Invalid costs fail with `INVALID_COST`; costs exceeding total bucket capacity fail with `COST_EXCEEDS_LIMIT`.
- **Settlement:** When an attempt completes, actual `usage` merges over reserved cost per unit (`{ ...cost, ...usage }`, keeping reserved amounts for omitted units; invalid `usage` is ignored). If usage exceeds the limit, the window throttles subsequent starts until excess capacity rolls out -- even for jobs requiring 0 units.
- **Settlement timestamp:** Usage is booked at completion and expires `perMs` after completion, not start. This matches provider accounting for long-running calls and avoids boundary 429s.
- **In-flight credit:** When computing wait times, sliding windows credit in-flight reservations as pending capacity. If pending attempts cover the shortfall, the bucket returns `WAIT_FOR_CHANGE` instead of a static sleep timer, preventing spurious `maxWaitMs` halts on daily windows.
- **Startup offset:** The `used` option seeds prior usage (e.g. from logs):
  `createTPD(5_000_000, { used: [{ at: timestamp, amount: tokens }, ...] })` (`amount` defaults to `1`) or `{ used: 350_000 }` (booked on the first check).
- **Shared buckets:** Passing the same bucket instance to multiple limiters enforces a shared quota. `createConcurrencyBucket(n)` coordinates global concurrency across limiters. Capacity notifications cycle through limiters via round-robin distribution.
- **Custom & event-driven buckets:** `createBucket({ getWaitMs(cost, now), reserve(cost, now), settle(reservation, actualCost, now), subscribe(onChange) })` defines custom buckets (all hooks optional). `getWaitMs` returns `0` (ready), `> 0` (wait ms), `Infinity` (exceeds limit), or `WAIT_FOR_CHANGE` (`-1`, exported from `'smart-limit'`) when blocked on external events; the limiter subscribes via `subscribe(onChange)` (which returns an unsubscribe function) and re-checks when notified. Reservations across multiple buckets are atomic: capacity is reserved only when all buckets return `0`, and partial reservations roll back via `settle(reservation, {}, now)` if a later `reserve` throws.
- **Configuration errors:** Buckets created with invalid options expose `configError` (e.g. `createLimiterError('INVALID_CONFIG', message)`), resolving `start()` with `INVALID_CONFIG` without throwing.

## Results and Errors

`processJob` returns `{ ok: true, data, usage? }` or `{ ok: false, error: JobError }`:

```ts
type JobError = {
    type: 'backoff_global' | 'retry_backoff' | 'retry_immediate' | 'fatal';
    message: string;
    code?: string;
    retryAfterMs?: number;
    usage?: Cost;
    cause?: unknown;
};
```

| Error `type` | Limiter Action | Semantics & Usage |
|---|---|---|
| `backoff_global` | Global lock & single probe | Locks all concurrency slots for `retryAfterMs` (used as given if `> 0`; else exponential backoff with a 1_000 ms floor, even if `retryDelayMs: 0`). Concurrent failures from the same in-flight wave only extend `lockUntil` if `retryAfterMs` is longer; stale failures from before an unlock re-queue without re-locking. Dispatches one probe job (`queue[0]`); only `ok: true` unlocks slots. Bounded by `maxWaitMs` (does not consume `maxRetries`). Intended exclusively for provider outages or rate limits (429, 503, network drop). |
| `retry_backoff` | Isolated delayed retry | Retries only that job after `retryAfterMs` (or exponential backoff; immediate if `retryDelayMs: 0`), up to `maxRetries`. Fails with `MAX_WAIT_EXCEEDED` if wait exceeds `maxWaitMs` (global lock duration excluded). Other slots continue running. |
| `retry_immediate` | Isolated immediate retry | Retries only that job immediately (`retryAt = 0`), up to `maxRetries`, without advancing its backoff exponent. Other slots continue running. |
| `fatal` | Permanent failure | No retry. Job settles immediately; `onJobDone` receives `{ ok: false, status: 'failed', error }`. |

### Lifecycle Hooks

- **`onAttempt(job, result, info)`:** Invoked after each attempt. Awaited before retries or `onJobDone`.
- **`onJobDone(job, outcome, info)`:** Invoked once per leased job. `info` (`AttemptInfo: { attempt: number, cost: Cost, durationMs: number }`) reflects the final attempt (unstarted jobs receive `{ attempt: 0, cost: bookedCost, durationMs: 0 }`).
  - `{ ok: true, status: 'done', data }`: Job succeeded.
  - `{ ok: false, status: 'failed', error }`: Permanent failure (`FATAL`, `RETRIES_EXHAUSTED`, `MAX_WAIT_EXCEEDED`, `INVALID_COST`, `COST_EXCEEDS_LIMIT`). On attempt failures, `error.cause` holds the final `JobError`.
  - `{ ok: false, status: 'released', error }`: Job flushed on stop (`STOPPED`), lock/quota expiration (`MAX_WAIT_EXCEEDED`), or per-job queue abort (`ABORTED`, `cause: signal.reason`). When a `backoff_global` error directly exceeds `maxWaitMs`, `error.cause` holds that `JobError`.

### Classifying Errors

The limiter never inspects HTTP status codes directly; it relies on the error `type` returned by the wrapper.

```ts
import type { JobError } from 'smart-limit';

function classifyError(err: any, signal: AbortSignal): JobError {
    const message = String(err?.message ?? err);

    // stop({ abort: true }): job is released; error type is ignored
    if (signal.reason?.name === 'AbortError') {
        return { type: 'retry_immediate', message };
    }

    // timeoutMs triggered: isolate slow request; retry alone
    if (signal.reason?.name === 'TimeoutError') {
        return { type: 'retry_backoff', message: 'timed out' };
    }

    // Rate-limited or overloaded provider: no tokens consumed; pause all slots, respect Retry-After
    if ([429, 529].includes(err?.status)) {
        return { type: 'backoff_global', message, retryAfterMs: err.retryAfterMs, usage: { tokens: 0 } };
    }

    // Upstream gateway errors: request may have reached provider; keep booking, pause all slots
    if ([502, 503, 504].includes(err?.status)) {
        return { type: 'backoff_global', message, retryAfterMs: err.retryAfterMs };
    }

    // Network disconnection: request never reached provider; 0 tokens consumed; pause all slots
    if (['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'ENETUNREACH'].includes(err?.code)) {
        return { type: 'backoff_global', message, usage: { tokens: 0 } };
    }

    // Client-side / validation error (bad payload, auth failure): fail immediately
    if (err?.status >= 400 && err?.status < 500) {
        return { type: 'fatal', message, usage: { tokens: 0 } };
    }

    // Malformed response (e.g. invalid JSON output): retry immediately
    if (err instanceof SyntaxError) {
        return { type: 'retry_immediate', message };
    }

    // Default per-job fallback (isolated 500, socket drop): retry with backoff
    return { type: 'retry_backoff', message };
}
```

- **Map infrastructure outages to `backoff_global`:** Network loss, upstream gateway errors (502, 503, 504), and 429 rate limits must return `backoff_global`. Misclassifying them as `retry_backoff` causes the limiter to cycle through jobs individually, burning through retries during an outage instead of holding all slots in a single backoff.
- **Avoid `backoff_global` for single-job errors:** Only a successful probe (`ok: true`) clears a global lock. Returning `backoff_global` for an unrecoverable request error locks all slots until `maxWaitMs` expires, releasing all jobs.
- **Timeouts as `retry_backoff`:** A timeout is usually an isolated slow call. Treating timeouts as `retry_backoff` lets other slots proceed. If an outage hangs all calls, each timed-out probe consumes a retry, and the lock expires once `maxWaitMs` elapses.
- **Disable SDK retries:** Client SDKs should be configured with `maxRetries: 0` so the limiter controls every attempt, sliding window reservation, and timeout.
- **Timeout mapping:** When `timeoutMs` triggers, throwing or returning `fatal` automatically maps to `retry_backoff` with code `TIMEOUT` (retaining any reported `usage`). Explicit `ok: true` or non-fatal error returns take precedence.

#### Usage of Failed Attempts

The limiter cannot infer whether a provider billed a failed attempt. Without explicit `usage` on the error, the booked estimate remains in the sliding window:

- Upfront rejections (429, 401, connection refusal) consume nothing and should set `usage: { tokens: 0 }`.
- Interrupted streams should report partial consumption:

```ts
processJob: async (job, { signal }) => {
    let outputTokens = 0;
    try {
        const stream = await sdk.stream(job.text, { signal });
        for await (const chunk of stream) outputTokens += chunk.tokens;
        return { ok: true, data: stream.output, usage: { tokens: stream.usage.totalTokens } };
    } catch (err) {
        const error = classifyError(err, signal);
        if (outputTokens > 0) {
            error.usage = { tokens: job.inputTokens + outputTokens };
        }
        return { ok: false, error };
    }
},
```

Over-reporting usage delays subsequent starts until the window rolls forward; under-reporting risks provider 429s.

#### Error Resilience

- Exceptions in callbacks (`processJob`, `fetchJob`, hooks, logger) never crash the process.
- Throws or invalid returns in `fetchJob` (`FETCH_FAILED`) or in bucket `getWaitMs`, `reserve`, or `subscribe` (`BUCKET_FAILED`) halt new dispatches, release buffered jobs (`STOPPED`), await active attempts, and resolve the run with `{ ok: false, error }` (`lastStoppedBy: 'error'`).
- Throws or async rejections in `bucket.settle` or `unsubscribe` log `BUCKET_FAILED` while execution continues.
- All errors are structured objects `{ code, message, cause? }` matching `ERROR_CODES`.

## Options

| Option | Default | Allowed Values | Description |
|---|---|---|---|
| `fetchJob` | *(required in `createLimiter`)* | `() => Awaitable<FetchedJob \| null \| undefined>` | Pulls the next job with optional `{ cost }`. Returns `null` or `undefined` when empty. |
| `processJob` | *(required)* | `(job, context) => Awaitable<JobResult>` | Executes a single attempt. `context` provides `{ attempt, signal }` (`attempt` starts at `1`). |
| `buckets` | `[]` | `readonly Bucket[]` | Passive rate limit buckets evaluated before each attempt. |
| `concurrency` | `5` | Integer `>= 1` or `Infinity` | Maximum concurrent jobs in flight. `Infinity` relies strictly on buckets. |
| `maxBufferSize` | `concurrency` (`Infinity` in `createQueue`) | Integer `>= 1` or `Infinity` | Maximum retrying jobs held in buffer before pausing `fetchJob()`. |
| `maxRetries` | `4` | Integer `>= 0` | Maximum retries per job for `retry_backoff` and `retry_immediate`. |
| `retryDelayMs` | `2_000` | Finite number `>= 0` | Initial exponential backoff delay (doubles: 2s, 4s, 8s...). `0` retries immediately. Minimum global backoff is 1_000 ms. |
| `maxRetryDelayMs` | `300_000` (5 min) | Finite number `>= 0` | Ceiling on computed exponential backoff delay. |
| `maxWaitMs` | `900_000` (15 min) | Number `>= 0` (`Infinity` allowed) | Maximum allowed wait for capacity or outage resolution; see [maxWaitMs](#maxwaitms). |
| `timeoutMs` | `0` (disabled) | `> 0` up to `2_147_483_647`, or `0`/`false`/`Infinity` | Per-attempt timeout. Aborts `signal` (`TimeoutError`). Slot stays occupied until promise settles; logs warning at `2 × timeoutMs` if stalled. |
| `onAttempt` | `undefined` | `(job, result, info) => unknown` | Callback after each attempt. Awaited before next retry or completion. |
| `onJobDone` | `undefined` | `(job, outcome, info) => unknown` | Callback invoked once per leased job with final resolution. |
| `clock` | System clock | `{ now, setTimeout, clearTimeout }` | Time source abstraction (e.g. `createFakeClock()` in tests). |
| `log` | `undefined` | `(entry: LogEntry) => void` | Structured logger: `{ level: 'log' \| 'warn' \| 'error', message, code?, data? }`. |

### maxWaitMs

`maxWaitMs` halts runs or fails jobs that hit unresolvable barriers (exhausted quotas or prolonged outages). It never penalizes jobs waiting in concurrency queues.

| Waiting Scenario | Evaluated Against `maxWaitMs` | Result if Exceeded |
|---|---|---|
| Single timed bucket wait (e.g. daily quota exhausted) | Checked prior to `fetchJob` and for each buffered job | Halts the run (`stoppedBy: 'max_wait_exceeded'`) |
| `retryAfterMs` on `backoff_global` | Checked when error is returned | Halts the run (`stoppedBy: 'max_wait_exceeded'`) |
| Global lock duration without a successful probe | Measured continuously from the start of the lock | Halts the run (`stoppedBy: 'max_wait_exceeded'`) |
| Isolated job `retry_backoff` wait | Single delay or cumulative backoff time (excluding global lock duration) | Fails only that job (`MAX_WAIT_EXCEEDED`) |
| Multiple independent bucket waits that individually fit | Sequential checks | Waits are not cumulative; run continues |
| Concurrency slot or `WAIT_FOR_CHANGE` capacity | Waiting for in-flight requests to complete | Never counts toward `maxWaitMs`; bounded by `timeoutMs` |

When a run stops for `maxWaitMs` (`stoppedBy: 'max_wait_exceeded'`):

- No new jobs are dispatched; active attempts finish naturally.
- All buffered jobs (including retries) are released as `status: 'released'` (`code: 'MAX_WAIT_EXCEEDED'`).
- The global lock persists across runs; the first job of a subsequent run probes alone.
- `wake()` does not auto-start after `max_wait_exceeded`; `start()` (or `start({ force: true })` to clear the lock) is required.

## Lifecycle

- **`start(options?: StartOptions)`:** Starts execution and returns `Result<RunSummary, LimiterError>`. Concludes when `fetchJob()` returns `null`/`undefined` (`stoppedBy: 'empty'`), or on `stop()` / `maxWaitMs`. `start({ force: true })` clears an active global lock before starting.
- **`stop(options?: StopOptions)`:** Halts dispatch, flushes buffered jobs to `onJobDone` as `status: 'released'` (`code: 'STOPPED'`), awaits active attempts, and returns `Result<RunSummary, LimiterError>`. `stop({ abort: true })` aborts running attempts immediately via `signal` (`AbortError`).
  > [!IMPORTANT]
  > Awaiting `limiter.stop()` inside limiter callbacks (`onAttempt`, `onJobDone`, `processJob`) deadlocks because `stop()` waits for active callbacks to settle; use `void limiter.stop()` instead.
- **`wake()`:** Signals that new jobs are available and returns `getStatus()`. If active, re-checks immediately. If idle, starts a run in the background when never started or last stopped by `'empty'` (unless a persisted lock exceeds `maxWaitMs`). After `'stop'`, `'max_wait_exceeded'`, or `'error'`, an explicit `start()` is required.
- **`isIdle()`:** Returns `true` when no run is active.
- **`getStatus()`:** Returns an operational snapshot:
  ```ts
  type LimiterStatus = {
      state: 'idle' | 'running' | 'locked' | 'probing' | 'stopping';
      running: number;        // Attempts currently in flight
      buffered: number;       // Jobs waiting in internal retry buffer
      lockUntil: number;      // Expiration timestamp of global lock (0 if unlocked/probing)
      lastStoppedBy: 'empty' | 'stop' | 'max_wait_exceeded' | 'error' | undefined;
  };
  ```

## A Fixed List

`runAll(items, options)` processes an array of `{ job, cost? }` items through an internal limiter and resolves in input order:

```ts
import { runAll, createRPM } from 'smart-limit';

const controller = new AbortController();
process.on('SIGINT', () => controller.abort());

const result = await runAll(prompts.map((prompt) => ({ job: prompt })), {
    concurrency: 3,
    buckets: [createRPM(60)],
    processJob: async (prompt, { signal }) => {
        const res = await sdk.generate(prompt, { signal });
        return { ok: true, data: res.output };
    },
    signal: controller.signal,
});

if (result.ok) {
    console.log(result.data); // JobOutcome<D>[] in input order
} else {
    console.error(result.error);
    console.log(result.data); // Finished and released items preserved
}
```

- **Order preservation:** Every index in `result.data` strictly matches input order.
- **Immediate resolution:** Resolves as soon as the final item completes, without waiting for rate-limit windows to expire.
- **Unstarted items:** Unstarted items in a stopped or failed run are released with `STOPPED` or `MAX_WAIT_EXCEEDED` (`attempt: 0`). Optional `onJobDone` is invoked for every item.
- **External abort:** Passing `signal` triggers an immediate stop (`stop({ abort: true })`), aborting in-flight attempts.
- **Error containment:** If execution fails (e.g. `BUCKET_FAILED`), `runAll` returns `{ ok: false, error, data }`, retaining outcomes for all processed items. Only `INVALID_CONFIG` returns without `data`. Never rejects.

## A Queue in Memory

`createQueue(options)` provides an in-memory queue for callers awaiting individual results (accepts all `LimiterOptions` except `fetchJob` and `onJobDone`):

```ts
import { createQueue, createRPM, createTPM } from 'smart-limit';

const queue = createQueue({
    concurrency: 5,
    buckets: [createRPM(60), createTPM(200_000)],
    processJob: async (prompt, { signal }) => {
        const res = await sdk.generate(prompt, { signal });
        return { ok: true, data: res.output, usage: { tokens: res.usage.totalTokens } };
    },
    maxWaitMs: 60_000,
});

async function generate(prompt: string, signal?: AbortSignal) {
    const outcome = await queue.push(prompt, {
        cost: { tokens: estimateTokens(prompt) },
        signal,
        priority: 0,
    });
    if (outcome.ok) return outcome.data;
    throw outcome.error; // e.g. RETRIES_EXHAUSTED, MAX_WAIT_EXCEEDED, ABORTED
}
```

- **Priority ordering:** Higher `priority` numbers run first (negatives allowed, default `0`). Equal priority runs FIFO. Priority governs dispatch into the limiter; retries maintain buffer order.
- **Unbounded buffer:** `maxBufferSize` defaults to `Infinity`. Retrying jobs never block free concurrency slots from accepting pending items.
- **Global lock:** A `backoff_global` lock pauses all queue callers. Exactly one probe executes alone.
- **Automatic execution:** Pushing starts the limiter automatically if idle (including after `stop()` or `max_wait_exceeded`). When empty, the run ends immediately.
- **Per-job abort:** Aborting `signal` resolves `push()` immediately with `ABORTED` (`status: 'released'`, `cause: signal.reason`) and removes waiting or buffered entries. If an attempt is in flight, its context signal aborts and settles without retry.
- **Fair capacity:** Queues sharing bucket instances interleave fairly: each waiting queue dispatches one job per turn.
- **Graceful shutdown:** `queue.stop(options?: StopOptions)` flushes waiting jobs (`status: 'released'`, `code: 'STOPPED'`), stops the limiter, and awaits active attempts (or aborts them if `abort: true`).
- **Queue status:** `queue.getStatus()` returns standard `LimiterStatus` augmented with `waiting` count.

## Error Codes Reference

All errors returned by the limiter, `runAll`, and `createQueue` contain stable error codes in `ERROR_CODES` (constructible via `createLimiterError(code, cause?)`):

| Code | Meaning | Context |
|---|---|---|
| `INVALID_CONFIG` | Invalid options or bucket definitions | Resolved on `start()`, `stop()`, `runAll()`, or `push()` |
| `ALREADY_RUNNING` | `limiter.start()` called while a run is active | Returned by `start()` |
| `FETCH_FAILED` | `fetchJob()` threw or returned invalid data | Halts the limiter run |
| `INVALID_COST` | Job cost is not an object of non-negative numbers | Fails that individual job |
| `COST_EXCEEDS_LIMIT` | Job cost exceeds total bucket capacity | Fails that individual job immediately |
| `BUCKET_FAILED` | Bucket threw or returned an invalid wait value | Halts run or logs error on settle/unsubscribe |
| `PROCESS_JOB_THREW` | `processJob()` threw instead of returning result | Attempt `JobError.code` (`fatal`, settles as `FATAL`) |
| `INVALID_RESULT` | `processJob()` returned malformed result object | Attempt `JobError.code` (`fatal`, settles as `FATAL`) |
| `TIMEOUT` | Attempt duration exceeded `timeoutMs` | Attempt `JobError.code` (`retry_backoff`) |
| `FATAL` | `processJob()` returned `fatal` error type | Fails that individual job immediately |
| `RETRIES_EXHAUSTED` | Retry attempts exceeded `maxRetries` | Fails that individual job |
| `MAX_WAIT_EXCEEDED` | Wait duration exceeded `maxWaitMs` | Fails job (retry) or halts run (global lock/quota) |
| `STOPPED` | Limiter was stopped before job finished | Releases unfinished jobs (`status: 'released'`) |
| `ABORTED` | Job's abort signal fired before completion | Releases job (`status: 'released'`) |
| `HOOK_FAILED` | `onAttempt` or `onJobDone` callback threw | Logged to logger; execution continues |

## Limitations

- **Single-process:** State resides in memory. Separate processes do not sync automatically. Multi-process deployments must partition limits across workers or seed usage via `used`. Bucket hooks are synchronous and cannot await remote stores (e.g. Redis).
- **Limiter-scoped locks:** Limiters sharing buckets share counts, not `backoff_global` locks. Routing shared traffic through a single `createLimiter` or `createQueue` instance pauses all callers on 429s.
- **Head-of-line ordering:** Jobs are evaluated in fetch/priority order. A job requiring large capacity delays subsequent jobs behind it until capacity accumulates.
- **System clock dependency:** Sliding windows depend on `Date.now()`. Clock adjustments backward delay starts; adjustments forward expire bookings early.

## Testing

`createFakeClock(start = 0)` from `smart-limit/testing` provides deterministic virtual time without real-time delays (`settle(promise)` throws if the promise stalls with no timers left or after `100_000` timers):

```ts
import { createLimiter, createRPM } from 'smart-limit';
import { createFakeClock } from 'smart-limit/testing';

const clock = createFakeClock();

const limiter = createLimiter({
    clock,
    concurrency: 1,
    buckets: [createRPM(10)],
    fetchJob,
    processJob,
});

// settle() advances virtual time across pending timers until execution finishes:
const resultPromise = limiter.start();
const result = await clock.settle(resultPromise);
if (result.ok) {
    console.log(result.data); // RunSummary: { done, failed, released, stoppedBy }
}

// Advance virtual time by discrete increments:
await clock.advance(60_000);
```

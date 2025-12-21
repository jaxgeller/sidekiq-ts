# Sidekiq-TS API Spec (Draft)

## Goals
- Provide a TypeScript-first port of Sidekiq with a familiar API for jobs, client enqueueing, server processing, and testing.
- Enforce full Redis schema and payload compatibility with the Ruby Sidekiq version for interop.
- Offer a minimal, composable API surface that can grow toward parity (not a one-shot port).
- Use Vitest for library tests.

## Non-goals (initial scope)
- Rails/ActiveJob integration.
- Web UI (may be added later, but not in the first phase).
- Enterprise/Pro features.

## Public package shape

```ts
import {
  Sidekiq,
  Job,
  Client,
  Testing,
  Stats,
  Queue,
  ScheduledSet,
  RetrySet,
  DeadSet,
} from "sidekiq-ts";
```

## Core Concepts

### Job classes (class-based only)
Jobs are classes with a `perform(...args)` method. They extend `Job` which adds Sidekiq-like class methods. Function-based jobs are out of scope.
Jobs should be registered with `Sidekiq.registerJob` in worker processes so the runner can resolve class names from Redis.

```ts
class HardJob extends Job<[number, number]> {
  static sidekiqOptions = { queue: "critical", retry: 5 };

  async perform(a: number, b: number) {
    // do work
  }
}

await HardJob.performAsync(1, 2);
await HardJob.performIn(60, 1, 2);
await HardJob.set({ queue: "low" }).performAsync(3, 4);
```

#### Job class API
- `static sidekiqOptions: JobOptions` (default job options)
- `static setSidekiqOptions(opts: JobOptions): void`
- `static queueAs(name: string): void`
- `static retryIn(fn): void` (custom retry delay or discard/kill)
- `static retriesExhausted(fn): void` (hook for exhausted retries)
- `static set(opts: JobSetterOptions): JobSetter`
- `static performAsync(...args): Promise<string | null>`
- `static performIn(secondsOrTimestamp, ...args): Promise<string | null>`
- `static performAt(timestamp, ...args): Promise<string | null>`
- `static performBulk(args: unknown[][], options?: BulkOptions): Promise<(string | null)[]>`
- `static performInline(...args): Promise<boolean | null>` (testing/inline)

Job instance API
- `jid: string` (job id)
- `logger()` (from config)
- `interrupted(): boolean` (server shutdown awareness)

### Client
Responsible for pushing jobs to Redis.

```ts
const client = new Client({ config: Sidekiq.defaultConfiguration });
await client.push({ class: HardJob, args: [1, 2] });
await Client.enqueue(HardJob, 1, 2);
```

Client API
- `constructor({ config?, redis?, middleware? })`
- `push(payload: JobPayload): Promise<string | null>`
- `pushBulk(payload: BulkPayload): Promise<(string | null)[]>`
- `cancel(jid: string): Promise<boolean>` (reserved for iterable jobs; optional)

Static helpers
- `Client.push(payload)`
- `Client.pushBulk(payload)`
- `Client.enqueue(JobClass, ...args)`
- `Client.enqueueTo(queue, JobClass, ...args)`
- `Client.enqueueIn(interval, JobClass, ...args)`
- `Client.enqueueToIn(queue, interval, JobClass, ...args)`
- `Client.via(redis, fn)` (temporary client routing)

### Configuration

```ts
Sidekiq.configureServer((config) => {
  config.redis = { url: "redis://localhost:6379/0" };
  config.concurrency = 10;
  config.queues = ["critical", "default", "low"];
  config.clientMiddleware.use(MyClientMiddleware);
  config.serverMiddleware.use(MyServerMiddleware);
});

Sidekiq.configureClient((config) => {
  config.redis = { url: "redis://localhost:6379/0" };
});
```

Config surface (initial)
- `redis: { url: string; ... }`
- `concurrency: number`
- `queues: string[] | Array<[string, number]>` (weighted)
- `timeout: number` (shutdown timeout seconds)
- `pollIntervalAverage: number`
- `errorHandlers: Array<(err, ctx) => void>`
- `deathHandlers: Array<(job, err) => void>`
- `lifecycleEvents: { startup, quiet, shutdown, heartbeat }`
- `logger`
- `strictArgs: "raise" | "warn" | "none"`
- `maxRetries: number` (default 25)
- `deadMaxJobs: number` (default 10000)
- `deadTimeoutInSeconds: number` (default 180 days)

### Middleware
Client and server middleware chains mirror Sidekiq's `call` pattern.

```ts
class MyClientMiddleware {
  async call(jobClass, payload, queue, redis, next) {
    return await next();
  }
}

class MyServerMiddleware {
  async call(jobInstance, payload, queue, next) {
    return await next();
  }
}
```

### Server runner
A Node-side worker which polls Redis and executes jobs.

```ts
Sidekiq.registerJob(HardJob);
const runner = await Sidekiq.run();
// later
await runner.quiet();
await runner.stop();
```

Runner API
- `Sidekiq.run({ config? }): Promise<Runner>`
- `Sidekiq.registerJob(JobClass): void`
- `Runner.quiet()` (stop fetching new jobs)
- `Runner.stop()` (graceful shutdown)
- `Runner.restart()` (optional)

CLI (planned)
- `sidekiq-ts` command reading a JSON config and starting a runner.

### Testing
Testing mode mimics Sidekiq's fake/inline behavior.

```ts
Testing.fake();
await HardJob.performAsync(1, 2);
HardJob.jobs.length; // 1
HardJob.drain();
```

Testing API
- `Testing.fake()` (default in tests)
- `Testing.inline()`
- `Testing.disable()`
- `Job.jobs`, `Job.clearAll()`, `Job.drainAll()`
- `MyJob.jobs`, `MyJob.clear()`, `MyJob.drain()`, `MyJob.performOne()`
- `Queues` helper for per-queue introspection

### Data API
- `Stats` (processed, failed, enqueued, queue sizes, latency)
- `Queue` (list/enumerate jobs in a queue)
- `ScheduledSet`, `RetrySet`, `DeadSet` (ZSET-based)
- `ProcessSet`/`WorkerSet` (server process info, optional)

## Redis schema compatibility
- Match Sidekiq's Redis schema and payloads exactly (key names, structures, timestamps, stats).
- Use the same key layout as Sidekiq:
  - `queue:<name>` LIST for immediate jobs
  - `schedule`, `retry`, `dead` ZSETs
  - `stat:processed`, `stat:failed`
  - `queues` SET
- Job payloads JSON-encoded with string keys and `jid`, `queue`, `class`, `args`, `enqueued_at`, optional `at`.

## Retry and failure behavior
- Default `retry: true` uses Sidekiq-like backoff.
- `retry: number` sets max retries.
- `retryIn(fn)` hook to customize backoff.
- `retriesExhausted(fn)` hook for final failure handling.
- `retryFor` max retry time window (optional).

## Serialization and strict args
- Only JSON-serializable args are allowed by default.
- `strictArgs` mode to warn or raise when complex args appear.
- Optional `dumpJson`/`loadJson` hooks in config for custom serialization.

## Implementation Plan (Phased)

Phase 1: Core enqueueing + execution (done)
- Implement config, Redis connection, logger, and JSON helpers.
- Implement `Job` base class and `Client.push`/`pushBulk`.
- Implement basic runner with concurrency and queue polling.
- Add strict args validation and minimal retry handling.
- Add tests with Vitest for enqueue/perform and scheduling.

Phase 2: Scheduling + retries (in progress)
- Scheduled poller to move due jobs from `schedule` and `retry` to queues.
- Retry set handling and exponential backoff.
- Dead set handling and death handlers.
- Tests for retry behavior and scheduled jobs.

Phase 3: Middleware + Testing utilities
- Client/server middleware chains.
- Testing fake/inline modes and in-memory queues.
- Tests for middleware and testing mode behavior.

Phase 4: Data API + CLI
- Stats/Queue/Sets API.
- CLI runner with JSON config file support.
- Tests for data API and CLI config parsing.

## Open questions
- Which Ruby Sidekiq version is the compatibility target for schema and payload nuances?

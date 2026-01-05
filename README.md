# sidekiq-ts

A TypeScript implementation of [Sidekiq](https://sidekiq.org/) for Node.js. Process background jobs with Redis-backed queues, featuring type-safe job definitions, automatic retries, scheduled jobs, leader election, and cron scheduling.

## Features

- **Type-safe jobs** - Generic argument types with full TypeScript support
- **Multiple queues** - Priority-based queue processing with weighted selection
- **Job scheduling** - Execute jobs immediately, after a delay, or at specific times
- **Automatic retries** - Exponential backoff with configurable retry limits
- **Dead letter queue** - Failed jobs preserved for debugging
- **Leader election** - Coordinate across distributed workers
- **Cron jobs** - Periodic job scheduling with standard cron expressions
- **Middleware** - Customize job enqueueing and execution
- **Rate limiting** - Control concurrent operations and API call rates
- **CLI** - Run workers from the command line
- **Testing utilities** - Fake and inline modes for testing

## Requirements

- Node.js >= 24.12.0
- Redis server

## Installation

```bash
npm install sidekiq-ts
```

## Quick Start

```typescript
import { Job, Sidekiq } from "sidekiq-ts";

// Configure Redis connection
Sidekiq.defaultConfiguration.redis = { url: "redis://localhost:6379" };

// Define a job
class WelcomeEmailJob extends Job<[string, string]> {
  perform(email: string, name: string) {
    console.log(`Sending welcome email to ${name} <${email}>`);
  }
}

// Register the job
Sidekiq.registerJob(WelcomeEmailJob);

// Enqueue jobs
await WelcomeEmailJob.performAsync("alice@example.com", "Alice");

// Start the worker
const runner = await Sidekiq.run();

// Later, stop gracefully
await runner.stop();
```

## Defining Jobs

Extend the `Job` class with a type parameter specifying your argument types:

```typescript
import { Job } from "sidekiq-ts";

class ProcessOrderJob extends Job<[string, number, boolean]> {
  perform(orderId: string, amount: number, isPriority: boolean) {
    // Your job logic here
  }
}
```

The `perform` method can be synchronous or return a Promise:

```typescript
class FetchDataJob extends Job<[string]> {
  async perform(url: string) {
    const response = await fetch(url);
    // Process response...
  }
}
```

### Accessing Job Context

Inside `perform`, you can access:

```typescript
class MyJob extends Job<[string]> {
  async perform(data: string) {
    // Unique job ID
    console.log(this.jid);

    // Check if worker is stopping (for graceful shutdown)
    if (this.interrupted()) {
      return; // Exit early
    }

    // Get the AbortSignal for cancellation-aware APIs
    const response = await fetch(url, { signal: this.signal });
  }
}
```

The `signal` property returns an `AbortSignal` that is aborted when the worker is shutting down. Use it with `fetch()`, streams, database drivers, or any API that supports `AbortSignal` for graceful cancellation.

## Job Options

Configure jobs using the static `sidekiqOptions` property:

```typescript
class PaymentJob extends Job<[string, number]> {
  static sidekiqOptions = {
    queue: "critical",      // Queue name (default: "default")
    retry: 5,               // Number of retries (default: 25, false to disable)
    backtrace: 10,          // Lines of backtrace to keep (default: false)
    dead: true,             // Add to dead queue on failure (default: true)
    tags: ["payments"],     // Tags for categorization
  };

  perform(orderId: string, amount: number) {
    // ...
  }
}
```

### Custom Retry Delay

Override the default exponential backoff:

```typescript
PaymentJob.retryIn((retryCount, error, payload) => {
  // Return delay in seconds
  return 10 * Math.pow(2, retryCount - 1); // 10s, 20s, 40s, 80s...
});
```

### Retries Exhausted Handler

Handle permanent failures:

```typescript
PaymentJob.retriesExhausted((payload, error) => {
  console.log(`Job ${payload.jid} failed permanently: ${error.message}`);
  // Notify external service, send alerts, etc.

  return undefined;  // Send to dead queue
  // return "discard"; // Skip dead queue entirely
});
```

### Per-Job Option Override

Override options for a specific enqueue:

```typescript
await NotificationJob.set({ queue: "critical" }).performAsync(userId, message);
```

## Enqueueing Jobs

### Immediate Execution

```typescript
const jid = await MyJob.performAsync(arg1, arg2);
```

### Delayed Execution

Execute after a delay (in seconds):

```typescript
const jid = await ReportJob.performIn(300, "daily-report"); // 5 minutes
```

### Scheduled Execution

Execute at a specific Unix timestamp:

```typescript
const tomorrow = Math.floor(Date.now() / 1000) + 86400;
const jid = await ReportJob.performAt(tomorrow, "weekly-report");
```

### Bulk Enqueueing

Enqueue multiple jobs efficiently:

```typescript
const jids = await ReminderJob.performBulk([
  [1, "Meeting at 3pm"],
  [2, "Review PR #123"],
  [3, "Deploy to staging"],
]);
```

## Configuration

### Redis Connection

```typescript
Sidekiq.defaultConfiguration.redis = {
  url: "redis://localhost:6379",
  // Or use environment variable
  // url: process.env.REDIS_URL
};
```

### Server Configuration

Configure worker-specific options:

```typescript
Sidekiq.configureServer({
  concurrency: 10,           // Worker threads (default: 5)
  timeout: 30,               // Shutdown timeout in seconds (default: 25)
  maxRetries: 25,            // Default retry limit (default: 25)
  tag: "worker-1",           // Process tag for identification
  labels: ["api", "prod"],   // Process labels
});
```

### Client Configuration

Configure client-only options (for processes that only enqueue jobs):

```typescript
Sidekiq.configureClient({
  strictArgs: "raise", // "raise" | "warn" | "none"
});
```

## Queue Configuration

### Simple Queues

Process queues in random order with equal priority:

```typescript
Sidekiq.configureServer({
  queues: ["default", "emails", "reports"],
});
```

### Weighted Queues

Higher weights get proportionally more processing time:

```typescript
Sidekiq.configureServer({
  queues: [
    ["critical", 5],   // 5x weight
    ["default", 2],    // 2x weight
    ["background", 1], // 1x weight
  ],
});
```

## Running Workers

### Programmatic

```typescript
import { Sidekiq } from "sidekiq-ts";

// Import your job files to register them
import "./jobs/email-job.js";
import "./jobs/report-job.js";

// Start worker with automatic signal handling
const runner = await Sidekiq.run({ signals: true });
// SIGINT/SIGTERM will gracefully stop the worker
```

The `signals: true` option registers handlers for `SIGINT`, `SIGTERM`, and `SIGTSTP` that gracefully shut down the worker. For custom signal handling:

```typescript
const runner = await Sidekiq.run();

process.on("SIGTERM", async () => {
  await runner.stop();
  process.exit(0);
});
```

### CLI

```bash
npx sidekiq [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-C, --config PATH` | Path to JSON config file (default: sidekiq.json if present) |
| `-c, --concurrency NUM` | Number of worker threads |
| `-e, --environment ENV` | Application environment |
| `-g, --tag TAG` | Process tag for identification |
| `-q, --queue QUEUE[,WT]` | Queue with optional weight (repeatable) |
| `-r, --require PATH` | File to import before startup (repeatable) |
| `-t, --timeout NUM` | Shutdown timeout in seconds |
| `-v, --verbose` | Enable debug logging |
| `-V, --version` | Print version and exit |
| `-h, --help` | Show help message |

**Examples:**

```bash
# Basic usage
npx sidekiq -r ./dist/jobs.js

# Multiple queues with weights
npx sidekiq -q critical,5 -q default,2 -q background -c 10

# With config file
npx sidekiq -C config/sidekiq.json -e production
```

## Configuration File

Create `sidekiq.json` in your project root:

```json
{
  "concurrency": 10,
  "queues": [
    ["critical", 5],
    ["default", 1]
  ],
  "timeout": 30,
  "require": [
    "./dist/jobs/index.js"
  ],
  "redis": {
    "url": "redis://localhost:6379"
  },
  "development": {
    "concurrency": 2
  },
  "production": {
    "concurrency": 20,
    "redis": {
      "url": "redis://prod-redis:6379"
    }
  }
}
```

Environment-specific settings override the defaults when `NODE_ENV` matches.

## Middleware

### Client Middleware

Intercept jobs during enqueueing:

```typescript
import { Sidekiq } from "sidekiq-ts";

class LoggingMiddleware {
  call(
    jobClass: string | unknown,
    payload: JobPayload,
    queue: string,
    redis: RedisClient,
    next: () => Promise<JobPayload | null>
  ) {
    console.log(`Enqueueing ${payload.class} to ${queue}`);
    return next();
  }
}

Sidekiq.useClientMiddleware(LoggingMiddleware);
```

### Server Middleware

Intercept jobs during execution:

```typescript
class TimingMiddleware {
  async call(
    job: Job,
    payload: JobPayload,
    queue: string,
    next: () => Promise<void>
  ) {
    const start = Date.now();
    try {
      await next();
    } finally {
      console.log(`${payload.class} took ${Date.now() - start}ms`);
    }
  }
}

Sidekiq.useServerMiddleware(TimingMiddleware);
```

## Rate Limiting

Limit concurrent operations or API call rates across all workers. Similar to [Sidekiq Enterprise rate limiting](https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting).

### Creating Limiters

```typescript
import { Limiter } from "sidekiq-ts";

// Limit concurrent operations (distributed mutex)
const apiLimiter = Limiter.concurrent("external-api", 50);

// Limit operations per time boundary (resets at interval start)
const emailLimiter = Limiter.bucket("email-send", 100, "minute");

// Limit operations in a rolling window
const requestLimiter = Limiter.window("api-requests", 1000, "hour");
```

### Using in Jobs

```typescript
import { Job, Limiter } from "sidekiq-ts";

const stripeLimiter = Limiter.concurrent("stripe-api", 25);

class ChargeCustomerJob extends Job<[string, number]> {
  async perform(customerId: string, amount: number) {
    await stripeLimiter.withinLimit(async () => {
      await stripe.charges.create({
        customer: customerId,
        amount,
      });
    });
  }
}
```

When the limit is exceeded, `withinLimit()` throws an `OverLimitError`.

### Auto-Reschedule with Middleware

To automatically reschedule jobs when rate limited (like Sidekiq Enterprise):

```typescript
import { Sidekiq, ensureRateLimitMiddleware } from "sidekiq-ts";

// Add the middleware before starting the worker
ensureRateLimitMiddleware(Sidekiq.defaultConfiguration);

const runner = await Sidekiq.run();
```

The middleware catches `OverLimitError` and reschedules the job with backoff (~5 minutes + jitter). After 20 reschedules, the job fails normally.

### Limiter Types

#### Concurrent Limiter

Limits how many operations can run simultaneously across all workers:

```typescript
// Max 10 concurrent Stripe API calls
const limiter = Limiter.concurrent("stripe", 10, {
  lockTimeout: 180, // Auto-release lock after 180 seconds (default)
});
```

#### Bucket Limiter

Allows N operations per time interval, resetting at boundaries:

```typescript
// 100 emails per minute, resets at :00 of each minute
const limiter = Limiter.bucket("email", 100, "minute");

// With numeric interval (seconds)
const limiter = Limiter.bucket("sms", 50, 30); // 50 per 30 seconds
```

Intervals: `"second"`, `"minute"`, `"hour"`, `"day"`, or number of seconds.

#### Window Limiter

Allows N operations in a rolling time window:

```typescript
// 1000 requests per hour, rolling window
const limiter = Limiter.window("api", 1000, "hour");
```

### Limiter Methods

```typescript
// Execute within the rate limit
await limiter.withinLimit(async () => {
  await doWork();
});

// Check current status (non-blocking)
const status = await limiter.check();
console.log(status.allowed);  // true if under limit
console.log(status.current);  // current usage
console.log(status.limit);    // max allowed

// Reset the limiter
await limiter.reset();
```

### Dynamic Limiters

Create per-user or per-resource limiters with dynamic names:

```typescript
class UserApiCallJob extends Job<[string, string]> {
  async perform(userId: string, endpoint: string) {
    // Each user gets their own rate limit
    const userLimiter = Limiter.window(`user-${userId}-api`, 100, "minute");

    await userLimiter.withinLimit(async () => {
      await fetch(endpoint);
    });
  }
}
```

Redis keys are created per unique name: `limiter:user-123-api`, `limiter:user-456-api`, etc.

For high-throughput scenarios, cache limiter instances:

```typescript
const userLimiters = new Map<string, ILimiter>();

function getUserLimiter(userId: string): ILimiter {
  let limiter = userLimiters.get(userId);
  if (!limiter) {
    limiter = Limiter.window(`user-${userId}-api`, 100, "minute");
    userLimiters.set(userId, limiter);
  }
  return limiter;
}
```

### Custom Key Prefix

```typescript
const limiter = Limiter.concurrent("api", 10, {
  keyPrefix: "myapp", // Redis key: myapp:api (default: limiter:api)
});
```

## Leader Election

For tasks that should only run on one worker (like cron jobs), use leader election:

```typescript
const runner = await Sidekiq.run();

// Check if this process is the leader
if (runner.leader()) {
  console.log("I am the leader!");
}
```

### Leader Lifecycle Events

```typescript
Sidekiq.on("leader", () => {
  console.log("Became leader");
});

Sidekiq.on("follower", () => {
  console.log("Lost leadership");
});
```

## Periodic Jobs (Cron)

Schedule jobs to run on a cron schedule. Only the leader process enqueues periodic jobs.

```typescript
import { Job, Sidekiq } from "sidekiq-ts";

class DailyReportJob extends Job<[]> {
  perform() {
    console.log("Generating daily report...");
  }
}

Sidekiq.registerJob(DailyReportJob);

const runner = await Sidekiq.run();

// Register a cron job (runs at midnight daily)
runner.periodicScheduler.register("0 0 * * *", DailyReportJob, {
  queue: "reports",
  args: [],
});
```

### Cron Expression Format

Standard 5-field cron expressions:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12 or jan-dec)
│ │ │ │ ┌───────────── day of week (0-6 or sun-sat)
│ │ │ │ │
* * * * *
```

**Examples:**

| Expression | Description |
|------------|-------------|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour |
| `0 0 * * *` | Daily at midnight |
| `0 9 * * 1-5` | Weekdays at 9am |
| `0 0 1 * *` | First day of month |

## Monitoring & API

### Statistics

```typescript
import { Stats } from "sidekiq-ts";

const stats = new Stats(config);
await stats.fetch();

console.log(stats.processed);    // Total processed jobs
console.log(stats.failed);       // Total failed jobs
console.log(stats.enqueued);     // Jobs waiting in queues
console.log(stats.scheduled);    // Scheduled job count
console.log(stats.retry);        // Jobs awaiting retry
console.log(stats.dead);         // Jobs in dead queue
console.log(stats.processes);    // Active worker count
```

### Queue Information

```typescript
import { Queue } from "sidekiq-ts";

const queue = new Queue("default", config);
console.log(await queue.size());    // Jobs in queue
console.log(await queue.latency()); // Oldest job age in seconds
await queue.clear();                // Remove all jobs
```

### Process Information

```typescript
import { ProcessSet } from "sidekiq-ts";

const processes = new ProcessSet(config);
for await (const process of processes) {
  console.log(process.identity);    // hostname:pid
  console.log(process.concurrency); // Worker threads
  console.log(process.busy);        // Currently processing
  console.log(process.queues);      // Assigned queues
}
```

### Dead Jobs

```typescript
import { DeadSet } from "sidekiq-ts";

const dead = new DeadSet(config);
console.log(await dead.size());

for await (const entry of dead) {
  console.log(entry.item.class);
  console.log(entry.item.error_message);

  await entry.retry();  // Re-enqueue the job
  // or
  await entry.delete(); // Remove permanently
}
```

## Testing

### Fake Mode

Collect jobs without executing them:

```typescript
import { Testing, Queues } from "sidekiq-ts";

Testing.fake();

await MyJob.performAsync("test");

// Check enqueued jobs
const jobs = Queues.jobs();
expect(jobs).toHaveLength(1);
expect(jobs[0].class).toBe("MyJob");

// Clear for next test
Queues.clearAll();

// Disable fake mode
Testing.disable();
```

### Inline Mode

Execute jobs synchronously:

```typescript
Testing.inline();

await MyJob.performAsync("test"); // Executes immediately

Testing.disable();
```

### Scoped Testing

```typescript
await Testing.fake(async () => {
  await MyJob.performAsync("test");
  expect(Queues.jobs()).toHaveLength(1);
}); // Automatically restores previous mode
```

## Error Handling

### Error Handlers

Called for every job failure (including retries):

```typescript
Sidekiq.on("error", (error, context) => {
  console.error(`Job failed: ${error.message}`);
  // Send to error tracking service
});
```

### Death Handlers

Called when a job exhausts all retries:

```typescript
Sidekiq.on("death", (payload, error) => {
  console.error(`Job ${payload.jid} died: ${error.message}`);
  // Alert on-call, create incident ticket, etc.
});
```

## Lifecycle Events

```typescript
Sidekiq.on("startup", () => {
  console.log("Worker starting");
});

Sidekiq.on("quiet", () => {
  console.log("Worker quieting (no new jobs)");
});

Sidekiq.on("shutdown", () => {
  console.log("Worker shutting down");
});

Sidekiq.on("heartbeat", () => {
  // Called every 10 seconds
});
```

## Graceful Shutdown

The worker handles these signals:

| Signal | Action |
|--------|--------|
| `SIGINT` | Graceful shutdown |
| `SIGTERM` | Graceful shutdown |
| `SIGTSTP` | Quiet mode (stop accepting new jobs) |
| `SIGTTIN` | Dump current job state to logs |

### Quiet Mode

When a worker enters quiet mode (`runner.quiet()` or `SIGTSTP`):

1. Stops polling queues for new jobs
2. In-flight jobs continue running to completion
3. Worker stays alive but idle

This is useful for graceful deploys—quiet the old workers, start new ones, then stop the old workers.

### Stop/Shutdown

When `runner.stop()` is called (or `SIGINT`/`SIGTERM` received):

1. **Quiet first** — Stops accepting new jobs
2. **Signal jobs** — Aborts the `AbortSignal` so jobs can detect shutdown via `this.signal` or `this.interrupted()`
3. **Wait for jobs** — Waits up to `timeout` seconds (default: 25) for in-flight jobs to complete
4. **Requeue incomplete jobs** — Any jobs still running after the timeout are pushed back to their Redis queues (`RPUSH queue:<name>`) so another worker can pick them up
5. **Cleanup Redis** — Removes this worker from the `processes` set and deletes heartbeat keys

### Redis Cleanup on Shutdown

When a worker shuts down cleanly, it removes its presence from Redis:

```
SREM processes <identity>       # Remove from active processes set
UNLINK <identity>:work          # Delete work-in-progress tracking
UNLINK <identity>               # Delete heartbeat data
```

Jobs that didn't complete within the timeout are requeued:

```
RPUSH queue:<name> <job-payload>  # Push back to queue for retry
```

This ensures no jobs are lost during deployments or restarts.

## Production

### Redis Configuration

For production Redis:

```typescript
Sidekiq.defaultConfiguration.redis = {
  url: process.env.REDIS_URL,
  // Connection pool settings are handled by the redis package
};
```

### Monitoring

- Use the Stats API to build dashboards
- Set up alerts on the dead queue size
- Monitor process count and job latency
- Track processed/failed rates over time

## License

See LICENSE for details.

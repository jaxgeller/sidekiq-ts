/**
 * Job Options Example
 *
 * This example demonstrates job customization:
 * - Setting queue and retry options via sidekiqOptions
 * - Custom retry delay logic with retryIn()
 * - Custom exhaustion handling with retriesExhausted()
 *
 * Run with: npx tsx examples/job-options.ts
 */

import { Job } from "../src/job.js";
import { Sidekiq } from "../src/sidekiq.js";

Sidekiq.defaultConfiguration.redis = { url: "redis://localhost:6379" };

// Configure worker to process multiple queues with priority weights
// Higher weight = more jobs processed from that queue
Sidekiq.configureServer((config) => {
  config.queues = [
    ["critical", 3], // Process 3x more often
    ["default", 1],
  ];
});

// A job with custom options set via static property
class PaymentJob extends Job<[string, number]> {
  // Configure queue, retry count, and backtrace depth
  static sidekiqOptions = {
    queue: "critical",
    retry: 5,
    backtrace: 10,
  };

  perform(orderId: string, amount: number) {
    console.log(`Processing payment for order ${orderId}: $${amount}`);

    // Simulate occasional failure
    if (Math.random() < 0.3) {
      throw new Error("Payment gateway timeout");
    }
  }
}

// Custom retry delay: exponential backoff
// The handler receives the retry count, error, and job payload
PaymentJob.retryIn((count, error, _payload) => {
  // Exponential backoff: 10s, 20s, 40s, 80s, 160s
  const delay = 10 * 2 ** (count - 1);
  console.log(
    `Retry #${count} for error "${error.message}" in ${delay} seconds`
  );
  return delay;
});

// Custom exhaustion handler: called when all retries are exhausted
// Return "discard" to skip the dead queue, undefined to send to dead queue
PaymentJob.retriesExhausted((payload, error) => {
  console.log(
    `Job ${payload.jid} exhausted all retries. Error: ${error.message}`
  );
  // Could notify an external service, send alerts, etc.
  // Return "discard" to skip dead queue, or undefined to add to dead queue
  return undefined;
});

// A simple job using the default queue
class NotificationJob extends Job<[number, string]> {
  perform(userId: number, message: string) {
    console.log(`Notifying user ${userId}: ${message}`);
  }
}

Sidekiq.registerJob(PaymentJob);
Sidekiq.registerJob(NotificationJob);

async function main() {
  // Enqueue to the critical queue
  await PaymentJob.performAsync("order-123", 99.99);
  console.log("Enqueued payment job to critical queue");

  // Enqueue to the default queue
  await NotificationJob.performAsync(1, "Your order has been placed");
  console.log("Enqueued notification job to default queue");

  // You can also override options per-job using set()
  await NotificationJob.set({ queue: "critical" }).performAsync(
    2,
    "Urgent notification"
  );
  console.log("Enqueued notification to critical queue via set()");

  // Start worker
  console.log("\nStarting worker...");
  const runner = await Sidekiq.run();

  await new Promise((resolve) => setTimeout(resolve, 2000));
  await runner.stop();

  console.log("\nDone!");
  process.exit(0);
}

main().catch(console.error);

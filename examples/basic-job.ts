/**
 * Basic Job Example
 *
 * This example demonstrates the core workflow:
 * 1. Configure Redis connection
 * 2. Define a job class
 * 3. Enqueue jobs
 * 4. Run a worker to process them
 *
 * Run with: npx tsx examples/basic-job.ts
 */

import { Job } from "../src/job.js";
import { Sidekiq } from "../src/sidekiq.js";

// Configure Redis connection
Sidekiq.defaultConfiguration.redis = { url: "redis://localhost:6379" };

// Define a job by extending the Job class.
// The type parameter specifies the argument types for perform().
class WelcomeEmailJob extends Job<[string, string]> {
  async perform(email: string, name: string) {
    console.log(`Sending welcome email to ${name} <${email}>`);
    await fetch("http://httpbin.org/delay/1");
    // In a real app, you'd send an actual email here
  }
}

// Register the job class so the worker can find it
Sidekiq.registerJob(WelcomeEmailJob);

async function main() {
  // Enqueue some jobs
  console.log("Enqueueing jobs...");

  const jid1 = await WelcomeEmailJob.performAsync("alice@example.com", "Alice");
  console.log(`Enqueued job ${jid1}`);

  const jid2 = await WelcomeEmailJob.performAsync("bob@example.com", "Bob");
  console.log(`Enqueued job ${jid2}`);

  // Start the worker to process jobs
  // The `signals: true` option auto-registers SIGINT/SIGTERM handlers
  // for graceful shutdown (Ctrl+C will wait for in-flight jobs to complete)
  console.log("\nStarting worker...");
  await Sidekiq.run({ signals: true });
}

main().catch(console.error);

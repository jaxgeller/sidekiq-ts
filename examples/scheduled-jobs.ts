/**
 * Scheduled Jobs Example
 *
 * This example demonstrates scheduling jobs for later execution:
 * - performIn: Execute after a delay (in seconds)
 * - performAt: Execute at a specific Unix timestamp
 * - performBulk: Enqueue many jobs efficiently
 *
 * Run with: npx tsx examples/scheduled-jobs.ts
 */

import { Job, Sidekiq } from "../src/index.js";

Sidekiq.defaultConfiguration.redis = { url: "redis://localhost:6379" };

class ReportJob extends Job<[string]> {
  perform(reportName: string) {
    console.log(`Generating report: ${reportName}`);
  }
}

class ReminderJob extends Job<[number, string]> {
  async perform(userId: number, message: string) {
    console.log(`Reminder for user ${userId}: ${message}`);
  }
}

Sidekiq.registerJob(ReportJob);
Sidekiq.registerJob(ReminderJob);

async function main() {
  // Schedule a job to run 30 seconds from now
  const jid1 = await ReportJob.performIn(30, "daily-sales");
  console.log(`Scheduled report job ${jid1} to run in 30 seconds`);

  // Schedule a job to run at a specific time (Unix timestamp)
  const tomorrow = Math.floor(Date.now() / 1000) + 86_400;
  const jid2 = await ReportJob.performAt(tomorrow, "weekly-summary");
  console.log(`Scheduled report job ${jid2} to run tomorrow`);

  // Enqueue many jobs at once with performBulk
  // Each inner array contains the arguments for one job
  const jids = await ReminderJob.performBulk([
    [1, "Meeting at 3pm"],
    [2, "Review PR #123"],
    [3, "Deploy to staging"],
  ]);
  console.log(`Enqueued ${jids.length} reminder jobs: ${jids.join(", ")}`);

  // Start worker
  console.log("\nStarting worker...");
  const runner = await Sidekiq.run();

  // Process for a bit then stop
  await new Promise((resolve) => setTimeout(resolve, 2000));
  await runner.stop();

  console.log("\nDone!");
  process.exit(0);
}

main().catch(console.error);

import { Client } from "../src/client.js";
import { Config } from "../src/config.js";
import { Job } from "../src/job.js";
import type { Logger } from "../src/logger.js";
import { Sidekiq } from "../src/sidekiq.js";

const hrNowMs = (): number => Number(process.hrtime.bigint()) / 1_000_000;

interface BenchmarkOptions {
  jobs: number;
  concurrency: number;
  batchSize: number;
  queue: string;
  redisUrl: string;
}

const parsePositiveInt = (value: string, flag: string): number => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} expects a positive integer`);
  }
  return Math.floor(parsed);
};

const parseArgs = (
  argv: string[]
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: CLI argument parsing is inherently complex
): BenchmarkOptions & { showHelp: boolean } => {
  const args = argv.slice(2);
  const options: BenchmarkOptions & { showHelp: boolean } = {
    jobs: 10_000,
    concurrency: 10,
    batchSize: 1000,
    queue: "default",
    redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379/0",
    showHelp: false,
  };

  for (let i = 0; i < args.length; i += 1) {
    const raw = args[i];
    const [arg, inlineValue] =
      raw.startsWith("--") && raw.includes("=")
        ? raw.split("=", 2)
        : [raw, undefined];
    const value = inlineValue ?? args[i + 1];

    switch (arg) {
      case "-h":
      case "--help":
        options.showHelp = true;
        break;
      case "--jobs":
        if (!value) {
          throw new Error(`${arg} expects a number`);
        }
        options.jobs = parsePositiveInt(value, arg);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "--concurrency":
        if (!value) {
          throw new Error(`${arg} expects a number`);
        }
        options.concurrency = parsePositiveInt(value, arg);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "--batch":
        if (!value) {
          throw new Error(`${arg} expects a number`);
        }
        options.batchSize = parsePositiveInt(value, arg);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "--queue":
        if (!value) {
          throw new Error(`${arg} expects a queue name`);
        }
        options.queue = value;
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "--redis":
        if (!value) {
          throw new Error(`${arg} expects a Redis URL`);
        }
        options.redisUrl = value;
        if (!inlineValue) {
          i += 1;
        }
        break;
      default:
        if (arg.startsWith("-")) {
          throw new Error(`Unknown option ${arg}`);
        }
        break;
    }
  }

  return options;
};

const printHelp = () => {
  console.log(`Usage: npm run bench -- [options]

Options:
  --jobs NUM         Number of jobs to enqueue (default: 10000)
  --concurrency NUM  Runner concurrency (default: 10)
  --batch NUM        Bulk enqueue batch size (default: 1000)
  --queue NAME       Queue name (default: default)
  --redis URL        Redis URL (default: redis://localhost:6379/0)
  -h, --help         Show this help message
`);
};

const nullLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const formatRate = (count: number, durationMs: number): string => {
  const seconds = Math.max(durationMs / 1000, 0.0001);
  return (count / seconds).toFixed(1);
};

const main = async () => {
  const options = parseArgs(process.argv);
  if (options.showHelp) {
    printHelp();
    return;
  }

  const config = new Config({
    redis: { url: options.redisUrl },
    concurrency: options.concurrency,
    queues: [options.queue],
  });
  config.skipDefaultJobLogging = true;
  config.logger = nullLogger;

  let completed = 0;
  let resolveDone: (() => void) | null = null;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  class BenchmarkJob extends Job<[]> {
    perform() {
      completed += 1;
      if (completed === options.jobs) {
        resolveDone?.();
      }
    }
  }

  Sidekiq.registerJob(BenchmarkJob);

  const redis = await config.getRedisClient();
  await redis.flushDb();

  const client = new Client({ config });
  const enqueueStart = hrNowMs();
  for (let i = 0; i < options.jobs; i += options.batchSize) {
    const count = Math.min(options.batchSize, options.jobs - i);
    const args = Array.from({ length: count }, () => []);
    await client.pushBulk({
      class: BenchmarkJob,
      queue: options.queue,
      args,
      batch_size: options.batchSize,
    });
  }
  const enqueueMs = hrNowMs() - enqueueStart;

  const runner = await Sidekiq.run({ config });
  const processStart = hrNowMs();
  await done;
  const processMs = hrNowMs() - processStart;
  await runner.stop();
  await config.close();

  const totalMs = enqueueMs + processMs;

  console.log("sidekiq-ts benchmark");
  console.log(
    `jobs=${options.jobs} concurrency=${options.concurrency} batch=${options.batchSize} queue=${options.queue}`
  );
  console.log(`redis=${options.redisUrl}`);
  console.log(
    `enqueue: ${enqueueMs.toFixed(1)}ms (${formatRate(options.jobs, enqueueMs)} jobs/s)`
  );
  console.log(
    `process: ${processMs.toFixed(1)}ms (${formatRate(options.jobs, processMs)} jobs/s)`
  );
  console.log(
    `total:   ${totalMs.toFixed(1)}ms (${formatRate(options.jobs, totalMs)} jobs/s)`
  );
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

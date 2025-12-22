import type { Config } from "./config.js";
import { IterableInterrupted, JobSkipError } from "./iterable-errors.js";
import type { JobPayload } from "./types.js";

export class InterruptHandler {
  config?: Config;

  async call(
    _instance: unknown,
    payload: JobPayload,
    _queue: string,
    next: () => Promise<unknown> | unknown
  ): Promise<unknown> {
    try {
      return await next();
    } catch (error) {
      if (error instanceof IterableInterrupted) {
        this.config?.logger.debug("Interrupted, re-queueing...");
        const { Client } = await import("./client.js");
        const client = new Client({ config: this.config });
        await client.push(payload);
        throw new JobSkipError();
      }
      throw error;
    }
  }
}

export const ensureInterruptHandler = (config: Config): void => {
  if (!config.serverMiddleware.exists(InterruptHandler)) {
    config.serverMiddleware.add(InterruptHandler);
  }
};

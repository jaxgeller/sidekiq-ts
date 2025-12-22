import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Config, Context, DefaultJobLogger } from "../src/index.js";
import { createLogger } from "../src/logger.js";
import type { JobPayload } from "../src/types.js";

const clearContext = () => {
  const current = Context.current();
  for (const key of Object.keys(current)) {
    delete current[key];
  }
};

describe("Context", () => {
  beforeEach(clearContext);
  afterEach(clearContext);

  it("merges and restores context", () => {
    expect(Context.current()).toEqual({});

    Context.with({ a: 1 }, () => {
      expect(Context.current()).toEqual({ a: 1 });
    });

    expect(Context.current()).toEqual({});
  });

  it("overrides overlapping context values", () => {
    Context.current().foo = "bar";
    expect(Context.current()).toEqual({ foo: "bar" });

    Context.with({ foo: "bingo" }, () => {
      expect(Context.current()).toEqual({ foo: "bingo" });
    });

    expect(Context.current()).toEqual({ foo: "bar" });
  });

  it("supports nested contexts", () => {
    expect(Context.current()).toEqual({});

    Context.with({ a: 1 }, () => {
      expect(Context.current()).toEqual({ a: 1 });

      Context.with({ b: 2, c: 3 }, () => {
        expect(Context.current()).toEqual({ a: 1, b: 2, c: 3 });
      });

      expect(Context.current()).toEqual({ a: 1 });
    });

    expect(Context.current()).toEqual({});
  });

  it("appends context to logger output", () => {
    const messages: string[] = [];
    const base = {
      debug: (message: string) => messages.push(message),
      info: (message: string) => messages.push(message),
      warn: (message: string) => messages.push(message),
      error: (message: string) => messages.push(message),
    } as unknown as Console;
    const logger = createLogger(base);

    Context.with(
      { class: "HaikuJob", jid: "1234abc", tags: ["alpha", "beta"] },
      () => {
        logger.info("hello context");
      }
    );

    expect(messages).toHaveLength(1);
    expect(messages[0]).toContain("hello context");
    expect(messages[0]).toContain("class=HaikuJob");
    expect(messages[0]).toContain("jid=1234abc");
    expect(messages[0]).toContain("tags=alpha,beta");
  });

  it("sets job context for the default job logger", async () => {
    const messages: string[] = [];
    const base = {
      debug: (message: string) => messages.push(message),
      info: (message: string) => messages.push(message),
      warn: (message: string) => messages.push(message),
      error: (message: string) => messages.push(message),
    } as unknown as Console;
    const config = new Config({
      logger: createLogger(base),
      loggedJobAttributes: ["tags", "custom"],
    });
    const jobLogger = new DefaultJobLogger(config);
    const payload: JobPayload = {
      class: "ContextJob",
      args: [],
      jid: "jid123",
      tags: ["alpha"],
      custom: "value",
    };
    let captured: Record<string, unknown> | null = null;

    await jobLogger.prepare(payload, async () => {
      await jobLogger.call(payload, "default", async () => {
        captured = { ...Context.current() };
      });
    });

    expect(captured).toMatchObject({
      jid: "jid123",
      class: "ContextJob",
      tags: ["alpha"],
      custom: "value",
      queue: "default",
    });
    expect(messages.some((message) => message.includes("start"))).toBe(true);
  });
});

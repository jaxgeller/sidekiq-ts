import { afterEach, describe, expect, it } from "vitest";
import {
  Context,
  SidekiqLogger,
} from "../src/index.js";
import {
  createLogger,
  Formatters,
  JsonFormatter,
  PlainFormatter,
} from "../src/logger.js";

const fakeConsole = (messages: string[]) =>
  ({
    debug: (message: string) => messages.push(message),
    info: (message: string) => messages.push(message),
    warn: (message: string) => messages.push(message),
    error: (message: string) => messages.push(message),
  }) as unknown as Console;

const clearContext = () => {
  const current = Context.current();
  for (const key of Object.keys(current)) {
    delete current[key];
  }
};

describe("Logger formatters", () => {
  afterEach(clearContext);

  it("formats pretty/plain output with context", () => {
    const messages: string[] = [];
    const logger = new SidekiqLogger(fakeConsole(messages), new PlainFormatter());

    Context.with({ class: "LogJob", jid: "jid-1", tags: ["alpha", "beta"] }, () => {
      logger.info("hello");
    });

    expect(messages).toHaveLength(1);
    const line = messages[0];
    expect(line).toContain("INFO");
    expect(line).toContain("pid=");
    expect(line).toContain("tid=");
    expect(line).toContain("class=LogJob");
    expect(line).toContain("jid=jid-1");
    expect(line).toContain("tags=alpha,beta");
    expect(line).toContain(": hello");
  });

  it("emits JSON logs with optional context", () => {
    const messages: string[] = [];
    const logger = new SidekiqLogger(fakeConsole(messages), new JsonFormatter());

    logger.debug("boom");
    Context.with({ jid: "1234abc" }, () => {
      logger.info("json format");
    });

    const [first, second] = messages.map((line) => JSON.parse(line));
    expect(Object.keys(first).sort()).toEqual(["lvl", "msg", "pid", "tid", "ts"]);
    expect(first.ctx).toBeUndefined();
    expect(first.lvl).toBe("DEBUG");
    expect(Object.keys(second).sort()).toEqual(["ctx", "lvl", "msg", "pid", "tid", "ts"]);
    expect(second.ctx.jid).toBe("1234abc");
    expect(second.lvl).toBe("INFO");
  });

  it("uses the without-timestamp formatter when DYNO is set", () => {
    const previous = process.env.DYNO;
    process.env.DYNO = "dyno-1";
    try {
      const messages: string[] = [];
      const logger = createLogger(fakeConsole(messages)) as SidekiqLogger;
      logger.info("hello");
      expect(logger.formatter).toBeInstanceOf(Formatters.WithoutTimestamp);
      expect(messages[0]).toContain("pid=");
      expect(messages[0]).not.toContain("T");
    } finally {
      if (previous === undefined) {
        delete process.env.DYNO;
      } else {
        process.env.DYNO = previous;
      }
    }
  });
});

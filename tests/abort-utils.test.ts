import { describe, expect, it } from "vitest";
import { sleepWithAbort } from "../src/abort-utils.js";

describe("sleepWithAbort", () => {
  it("resolves after the specified time when no signal provided", async () => {
    const start = Date.now();
    await sleepWithAbort(100, undefined);
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(95);
    expect(elapsed).toBeLessThan(200);
  });

  it("resolves immediately when signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();

    const start = Date.now();
    await sleepWithAbort(1000, controller.signal);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(50);
  });

  it("resolves early when signal is aborted during sleep", async () => {
    const controller = new AbortController();

    const start = Date.now();
    const sleepPromise = sleepWithAbort(1000, controller.signal);

    // Abort after 50ms
    setTimeout(() => controller.abort(), 50);

    await sleepPromise;
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(200);
    expect(elapsed).toBeGreaterThanOrEqual(45);
  });

  it("does not leak event listeners after normal completion", async () => {
    const controller = new AbortController();

    await sleepWithAbort(10, controller.signal);

    // Listener should be cleaned up
    // The signal should not have any active listeners after completion
    expect(controller.signal.aborted).toBe(false);
  });

  it("handles multiple sequential sleeps with the same controller", async () => {
    const controller = new AbortController();

    const start = Date.now();
    await sleepWithAbort(20, controller.signal);
    await sleepWithAbort(20, controller.signal);
    await sleepWithAbort(20, controller.signal);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeGreaterThanOrEqual(55);
    expect(elapsed).toBeLessThan(200);
  });

  it("handles very short sleep durations", async () => {
    const controller = new AbortController();

    const start = Date.now();
    await sleepWithAbort(1, controller.signal);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(100);
  });

  it("handles zero duration sleep", async () => {
    const controller = new AbortController();

    const start = Date.now();
    await sleepWithAbort(0, controller.signal);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(50);
  });
});

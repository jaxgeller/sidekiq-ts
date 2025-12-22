import { describe, expect, it } from "vitest";
import { nextRun, parseCron, shouldRunAt } from "../src/cron.js";

describe("parseCron", () => {
  it("parses * for all values", () => {
    const schedule = parseCron("* * * * *");
    expect(schedule.minute).toHaveLength(60);
    expect(schedule.hour).toHaveLength(24);
    expect(schedule.dayOfMonth).toHaveLength(31);
    expect(schedule.month).toHaveLength(12);
    expect(schedule.dayOfWeek).toHaveLength(7);
  });

  it("parses single values", () => {
    const schedule = parseCron("5 10 15 6 3");
    expect(schedule.minute).toEqual([5]);
    expect(schedule.hour).toEqual([10]);
    expect(schedule.dayOfMonth).toEqual([15]);
    expect(schedule.month).toEqual([6]);
    expect(schedule.dayOfWeek).toEqual([3]);
  });

  it("parses ranges", () => {
    const schedule = parseCron("0-5 * * * *");
    expect(schedule.minute).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it("parses lists", () => {
    const schedule = parseCron("0,15,30,45 * * * *");
    expect(schedule.minute).toEqual([0, 15, 30, 45]);
  });

  it("parses step values with *", () => {
    const schedule = parseCron("*/15 * * * *");
    expect(schedule.minute).toEqual([0, 15, 30, 45]);
  });

  it("parses step values with range", () => {
    const schedule = parseCron("0-30/10 * * * *");
    expect(schedule.minute).toEqual([0, 10, 20, 30]);
  });

  it("parses month names", () => {
    const schedule = parseCron("* * * jan,jun,dec *");
    expect(schedule.month).toEqual([1, 6, 12]);
  });

  it("parses day names", () => {
    const schedule = parseCron("* * * * mon,wed,fri");
    expect(schedule.dayOfWeek).toEqual([1, 3, 5]);
  });

  it("throws on invalid field count", () => {
    expect(() => parseCron("* * *")).toThrow("expected 5 fields");
  });

  it("throws on invalid value", () => {
    expect(() => parseCron("60 * * * *")).toThrow("Invalid cron value");
  });
});

describe("shouldRunAt", () => {
  it("returns true when schedule matches", () => {
    const schedule = parseCron("30 10 * * *");
    const date = new Date("2024-06-15T10:30:00");
    expect(shouldRunAt(schedule, date)).toBe(true);
  });

  it("returns false when minute does not match", () => {
    const schedule = parseCron("30 10 * * *");
    const date = new Date("2024-06-15T10:31:00");
    expect(shouldRunAt(schedule, date)).toBe(false);
  });

  it("matches every 5 minutes", () => {
    const schedule = parseCron("*/5 * * * *");
    expect(shouldRunAt(schedule, new Date("2024-06-15T10:00:00"))).toBe(true);
    expect(shouldRunAt(schedule, new Date("2024-06-15T10:05:00"))).toBe(true);
    expect(shouldRunAt(schedule, new Date("2024-06-15T10:03:00"))).toBe(false);
  });
});

describe("nextRun", () => {
  it("finds the next minute for * * * * *", () => {
    const schedule = parseCron("* * * * *");
    const now = new Date("2024-06-15T10:30:45");
    const next = nextRun(schedule, now);
    expect(next.getMinutes()).toBe(31);
    expect(next.getSeconds()).toBe(0);
  });

  it("finds the next matching minute", () => {
    const schedule = parseCron("0 * * * *");
    const now = new Date("2024-06-15T10:30:00");
    const next = nextRun(schedule, now);
    expect(next.getHours()).toBe(11);
    expect(next.getMinutes()).toBe(0);
  });

  it("finds the next matching hour", () => {
    const schedule = parseCron("0 9 * * *");
    const now = new Date("2024-06-15T10:00:00");
    const next = nextRun(schedule, now);
    expect(next.getDate()).toBe(16);
    expect(next.getHours()).toBe(9);
    expect(next.getMinutes()).toBe(0);
  });
});

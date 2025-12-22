/**
 * Cron expression parser for standard 5-field cron syntax.
 *
 * Format: minute hour day-of-month month day-of-week
 *
 * Supported syntax:
 * - `*` - any value
 * - `N` - specific value
 * - `N-M` - range from N to M (inclusive)
 * - `N,M,O` - list of values
 * - `*\/N` or `N/M` - step values (every N starting at 0, or every M starting at N)
 */

export interface CronSchedule {
  minute: number[];
  hour: number[];
  dayOfMonth: number[];
  month: number[];
  dayOfWeek: number[];
}

interface FieldRange {
  min: number;
  max: number;
}

const FIELD_RANGES: Record<keyof CronSchedule, FieldRange> = {
  minute: { min: 0, max: 59 },
  hour: { min: 0, max: 23 },
  dayOfMonth: { min: 1, max: 31 },
  month: { min: 1, max: 12 },
  dayOfWeek: { min: 0, max: 6 },
};

const MONTH_NAMES: Record<string, number> = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
};

const DAY_NAMES: Record<string, number> = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6,
};

const WHITESPACE_REGEX = /\s+/;

function parseValue(
  value: string,
  range: FieldRange,
  names?: Record<string, number>
): number {
  const lower = value.toLowerCase();
  if (names && lower in names) {
    return names[lower];
  }
  const num = Number.parseInt(value, 10);
  if (Number.isNaN(num) || num < range.min || num > range.max) {
    throw new Error(
      `Invalid cron value: ${value} (expected ${range.min}-${range.max})`
    );
  }
  return num;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: cron parsing requires handling multiple syntax forms
function parseField(
  field: string,
  range: FieldRange,
  names?: Record<string, number>
): number[] {
  const values: Set<number> = new Set();

  for (const part of field.split(",")) {
    if (part.includes("/")) {
      // Step value: */N or N/M or N-M/S
      const [rangeStr, stepStr] = part.split("/");
      const step = Number.parseInt(stepStr, 10);
      if (Number.isNaN(step) || step <= 0) {
        throw new Error(`Invalid cron step: ${stepStr}`);
      }

      let start: number;
      let end: number;

      if (rangeStr === "*") {
        start = range.min;
        end = range.max;
      } else if (rangeStr.includes("-")) {
        const [startStr, endStr] = rangeStr.split("-");
        start = parseValue(startStr, range, names);
        end = parseValue(endStr, range, names);
      } else {
        start = parseValue(rangeStr, range, names);
        end = range.max;
      }

      for (let i = start; i <= end; i += step) {
        values.add(i);
      }
    } else if (part.includes("-")) {
      // Range: N-M
      const [startStr, endStr] = part.split("-");
      const start = parseValue(startStr, range, names);
      const end = parseValue(endStr, range, names);
      if (start > end) {
        throw new Error(`Invalid cron range: ${part}`);
      }
      for (let i = start; i <= end; i += 1) {
        values.add(i);
      }
    } else if (part === "*") {
      // Any value
      for (let i = range.min; i <= range.max; i += 1) {
        values.add(i);
      }
    } else {
      // Single value
      values.add(parseValue(part, range, names));
    }
  }

  return Array.from(values).sort((a, b) => a - b);
}

/**
 * Parse a cron expression into a schedule object.
 *
 * @param expression - Standard 5-field cron expression (minute hour day month weekday)
 * @returns Parsed schedule with arrays of valid values for each field
 * @throws Error if the expression is invalid
 */
export function parseCron(expression: string): CronSchedule {
  const parts = expression.trim().split(WHITESPACE_REGEX);
  if (parts.length !== 5) {
    throw new Error(
      `Invalid cron expression: expected 5 fields, got ${parts.length}`
    );
  }

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

  return {
    minute: parseField(minute, FIELD_RANGES.minute),
    hour: parseField(hour, FIELD_RANGES.hour),
    dayOfMonth: parseField(dayOfMonth, FIELD_RANGES.dayOfMonth),
    month: parseField(month, FIELD_RANGES.month, MONTH_NAMES),
    dayOfWeek: parseField(dayOfWeek, FIELD_RANGES.dayOfWeek, DAY_NAMES),
  };
}

/**
 * Check if a schedule should run at the given date/time.
 *
 * @param schedule - Parsed cron schedule
 * @param at - Date to check (defaults to now)
 * @returns True if the schedule matches the given time
 */
export function shouldRunAt(
  schedule: CronSchedule,
  at: Date = new Date()
): boolean {
  const minute = at.getMinutes();
  const hour = at.getHours();
  const dayOfMonth = at.getDate();
  const month = at.getMonth() + 1; // JS months are 0-indexed
  const dayOfWeek = at.getDay();

  return (
    schedule.minute.includes(minute) &&
    schedule.hour.includes(hour) &&
    schedule.dayOfMonth.includes(dayOfMonth) &&
    schedule.month.includes(month) &&
    schedule.dayOfWeek.includes(dayOfWeek)
  );
}

/**
 * Calculate the next run time after a given date.
 *
 * @param schedule - Parsed cron schedule
 * @param after - Start searching from this date (defaults to now)
 * @returns The next date/time when the schedule will run
 */
export function nextRun(
  schedule: CronSchedule,
  after: Date = new Date()
): Date {
  // Start from the next minute
  const next = new Date(after);
  next.setSeconds(0);
  next.setMilliseconds(0);
  next.setMinutes(next.getMinutes() + 1);

  // Search up to 4 years ahead (handles leap years and edge cases)
  const maxIterations = 4 * 366 * 24 * 60;

  for (let i = 0; i < maxIterations; i += 1) {
    if (shouldRunAt(schedule, next)) {
      return next;
    }

    // Increment by 1 minute
    next.setMinutes(next.getMinutes() + 1);
  }

  throw new Error("Could not find next run time within 4 years");
}

import { parseCron, cronMatches } from './cron.ts'
import type { CronExpression } from './cron.ts'

export type TaskHandler = () => void | Promise<void>

export enum TimeUnit {
  Minutes = 'minutes',
  Hours = 'hours',
  Days = 'days',
}

const DAY_NAMES: Record<string, number> = {
  sunday: 0,
  sun: 0,
  monday: 1,
  mon: 1,
  tuesday: 2,
  tue: 2,
  wednesday: 3,
  wed: 3,
  thursday: 4,
  thu: 4,
  friday: 5,
  fri: 5,
  saturday: 6,
  sat: 6,
}

/**
 * A scheduled task definition with a fluent configuration API.
 *
 * @example
 * new Schedule('cleanup', handler).dailyAt('02:30').withoutOverlapping()
 */
export class Schedule {
  readonly name: string
  readonly handler: TaskHandler

  private _cron: CronExpression | null = null
  private _noOverlap = false

  private _sporadicMin: number | null = null
  private _sporadicMax: number | null = null
  private _sporadicUnit: TimeUnit | null = null
  private _nextRunAt: Date | null = null

  constructor(name: string, handler: TaskHandler) {
    this.name = name
    this.handler = handler
  }

  // ── Raw cron ──────────────────────────────────────────────────────────────

  /** Set a raw 5-field cron expression. */
  cron(expression: string): this {
    this._cron = parseCron(expression)
    return this
  }

  // ── Minute-based ──────────────────────────────────────────────────────────

  /** Run every minute. */
  everyMinute(): this {
    return this.cron('* * * * *')
  }

  /** Run every 2 minutes. */
  everyTwoMinutes(): this {
    return this.cron('*/2 * * * *')
  }

  /** Run every 5 minutes. */
  everyFiveMinutes(): this {
    return this.cron('*/5 * * * *')
  }

  /** Run every 10 minutes. */
  everyTenMinutes(): this {
    return this.cron('*/10 * * * *')
  }

  /** Run every 15 minutes. */
  everyFifteenMinutes(): this {
    return this.cron('*/15 * * * *')
  }

  /** Run every 30 minutes. */
  everyThirtyMinutes(): this {
    return this.cron('*/30 * * * *')
  }

  // ── Hourly ────────────────────────────────────────────────────────────────

  /** Run once per hour at minute 0. */
  hourly(): this {
    return this.cron('0 * * * *')
  }

  /** Run once per hour at the given minute. */
  hourlyAt(minute: number): this {
    return this.cron(`${minute} * * * *`)
  }

  // ── Daily ─────────────────────────────────────────────────────────────────

  /** Run once per day at midnight. */
  daily(): this {
    return this.cron('0 0 * * *')
  }

  /** Run once per day at the given time (HH:MM). */
  dailyAt(time: string): this {
    const [hour, minute] = parseTime(time)
    return this.cron(`${minute} ${hour} * * *`)
  }

  /** Run twice per day at the given hours (minute 0). */
  twiceDaily(hour1: number, hour2: number): this {
    return this.cron(`0 ${hour1},${hour2} * * *`)
  }

  // ── Weekly ────────────────────────────────────────────────────────────────

  /** Run once per week on Sunday at midnight. */
  weekly(): this {
    return this.cron('0 0 * * 0')
  }

  /** Run once per week on the given day and optional time. */
  weeklyOn(day: string | number, time?: string): this {
    const dow = typeof day === 'string' ? dayToNumber(day) : day
    const [hour, minute] = time ? parseTime(time) : [0, 0]
    return this.cron(`${minute} ${hour} * * ${dow}`)
  }

  // ── Monthly ───────────────────────────────────────────────────────────────

  /** Run once per month on the 1st at midnight. */
  monthly(): this {
    return this.cron('0 0 1 * *')
  }

  /** Run once per month on the given day and optional time. */
  monthlyOn(day: number, time?: string): this {
    const [hour, minute] = time ? parseTime(time) : [0, 0]
    return this.cron(`${minute} ${hour} ${day} * *`)
  }

  // ── Sporadic ──────────────────────────────────────────────────────────────

  /**
   * Run at random intervals between `min` and `max` in the given unit.
   * Simulates human-like, non-periodic scheduling.
   *
   * @example
   * Scheduler.task('scrape', handler).sporadically(5, 30, TimeUnit.Minutes)
   */
  sporadically(min: number, max: number, unit: TimeUnit): this {
    if (min < 0 || max < 0) {
      throw new Error('sporadically: min and max must be non-negative')
    }
    if (min >= max) {
      throw new Error('sporadically: min must be less than max')
    }

    this._sporadicMin = min
    this._sporadicMax = max
    this._sporadicUnit = unit
    this._cron = null
    this._nextRunAt = this.computeNextRun(new Date())
    return this
  }

  // ── Options ───────────────────────────────────────────────────────────────

  /** Prevent overlapping runs within this process. */
  withoutOverlapping(): this {
    this._noOverlap = true
    return this
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  /** Check if this task is due at the given Date (evaluated in UTC). */
  isDue(now: Date): boolean {
    if (this._sporadicMin !== null) {
      if (!this._nextRunAt) return false
      if (now >= this._nextRunAt) {
        this._nextRunAt = this.computeNextRun(now)
        return true
      }
      return false
    }

    if (!this._cron) return false
    return cronMatches(this._cron, now)
  }

  /** Whether overlap prevention is enabled. */
  get preventsOverlap(): boolean {
    return this._noOverlap
  }

  /** The parsed cron expression (for testing/debugging). */
  get expression(): CronExpression | null {
    return this._cron
  }

  /** The next scheduled run time (for sporadic schedules). */
  get nextRunAt(): Date | null {
    return this._nextRunAt
  }

  /** Compute the next random run time from a reference point. */
  private computeNextRun(from: Date): Date {
    const ms = unitToMs(this._sporadicUnit!)
    const minMs = this._sporadicMin! * ms
    const maxMs = this._sporadicMax! * ms
    const delay = minMs + Math.random() * (maxMs - minMs)
    return new Date(from.getTime() + delay)
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseTime(time: string): [number, number] {
  const parts = time.split(':')
  if (parts.length !== 2) {
    throw new Error(`Invalid time format "${time}": expected HH:MM`)
  }
  const hour = parseInt(parts[0]!, 10)
  const minute = parseInt(parts[1]!, 10)
  if (isNaN(hour) || isNaN(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    throw new Error(`Invalid time "${time}": hour must be 0–23, minute must be 0–59`)
  }
  return [hour, minute]
}

function unitToMs(unit: TimeUnit): number {
  switch (unit) {
    case TimeUnit.Minutes:
      return 60_000
    case TimeUnit.Hours:
      return 3_600_000
    case TimeUnit.Days:
      return 86_400_000
  }
}

function dayToNumber(day: string): number {
  const n = DAY_NAMES[day.toLowerCase()]
  if (n === undefined) {
    throw new Error(
      `Invalid day name "${day}": expected one of ${Object.keys(DAY_NAMES)
        .filter((_, i) => i % 2 === 0)
        .join(', ')}`
    )
  }
  return n
}

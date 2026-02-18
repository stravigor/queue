/**
 * Minimal 5-field cron expression parser.
 *
 * Supports: `*`, exact (`5`), range (`1-5`), list (`1,3,5`),
 * step (`*​/10`), and range+step (`1-30/5`).
 *
 * Fields: minute (0–59), hour (0–23), day-of-month (1–31),
 * month (1–12), day-of-week (0–6, 0 = Sunday).
 */

export interface CronExpression {
  minute: number[]
  hour: number[]
  dayOfMonth: number[]
  month: number[]
  dayOfWeek: number[]
}

const FIELD_RANGES: [number, number][] = [
  [0, 59], // minute
  [0, 23], // hour
  [1, 31], // day of month
  [1, 12], // month
  [0, 6], // day of week
]

// Parse a 5-field cron string into expanded numeric arrays.
export function parseCron(expression: string): CronExpression {
  const parts = expression.trim().split(/\s+/)
  if (parts.length !== 5) {
    throw new Error(
      `Invalid cron expression "${expression}": expected 5 fields, got ${parts.length}`
    )
  }

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts.map((part, i) =>
    parseField(part, FIELD_RANGES[i]![0], FIELD_RANGES[i]![1])
  )

  return {
    minute: minute!,
    hour: hour!,
    dayOfMonth: dayOfMonth!,
    month: month!,
    dayOfWeek: dayOfWeek!,
  }
}

/**
 * Check if a Date matches a parsed cron expression.
 *
 * Standard cron rule: if both day-of-month and day-of-week are restricted
 * (not wildcards), either match satisfies the condition.
 */
export function cronMatches(cron: CronExpression, date: Date): boolean {
  const minute = date.getUTCMinutes()
  const hour = date.getUTCHours()
  const dayOfMonth = date.getUTCDate()
  const month = date.getUTCMonth() + 1
  const dayOfWeek = date.getUTCDay()

  if (!cron.minute.includes(minute)) return false
  if (!cron.hour.includes(hour)) return false
  if (!cron.month.includes(month)) return false

  // Standard cron: day-of-month and day-of-week are OR'd when both are restricted
  const domRestricted = cron.dayOfMonth.length < 31
  const dowRestricted = cron.dayOfWeek.length < 7

  if (domRestricted && dowRestricted) {
    return cron.dayOfMonth.includes(dayOfMonth) || cron.dayOfWeek.includes(dayOfWeek)
  }

  if (!cron.dayOfMonth.includes(dayOfMonth)) return false
  if (!cron.dayOfWeek.includes(dayOfWeek)) return false

  return true
}

/**
 * Compute the next Date (UTC) after `after` that matches the expression.
 * Searches up to 2 years ahead to prevent infinite loops.
 */
export function nextCronDate(cron: CronExpression, after: Date): Date {
  const date = new Date(after.getTime())
  // Advance to the next whole minute
  date.setUTCSeconds(0, 0)
  date.setUTCMinutes(date.getUTCMinutes() + 1)

  const limit = after.getTime() + 2 * 365 * 24 * 60 * 60 * 1000

  while (date.getTime() <= limit) {
    if (cronMatches(cron, date)) return date
    date.setUTCMinutes(date.getUTCMinutes() + 1)
  }

  throw new Error('Could not find next matching date within 2 years')
}

// ---------------------------------------------------------------------------
// Field parsing
// ---------------------------------------------------------------------------

function parseField(field: string, min: number, max: number): number[] {
  const values = new Set<number>()

  for (const part of field.split(',')) {
    const stepMatch = part.match(/^(.+)\/(\d+)$/)

    if (stepMatch) {
      const [, rangePart, stepStr] = stepMatch
      const step = parseInt(stepStr!, 10)
      if (step <= 0) throw new Error(`Invalid step value: ${stepStr}`)

      const [start, end] = rangePart === '*' ? [min, max] : parseRange(rangePart!, min, max)
      for (let i = start; i <= end; i += step) {
        values.add(i)
      }
    } else if (part === '*') {
      for (let i = min; i <= max; i++) values.add(i)
    } else if (part.includes('-')) {
      const [start, end] = parseRange(part, min, max)
      for (let i = start; i <= end; i++) values.add(i)
    } else {
      const n = parseInt(part, 10)
      if (isNaN(n) || n < min || n > max) {
        throw new Error(`Invalid cron value "${part}": must be ${min}–${max}`)
      }
      values.add(n)
    }
  }

  return [...values].sort((a, b) => a - b)
}

function parseRange(part: string, min: number, max: number): [number, number] {
  const [startStr, endStr] = part.split('-')
  const start = parseInt(startStr!, 10)
  const end = parseInt(endStr!, 10)

  if (isNaN(start) || isNaN(end) || start < min || end > max || start > end) {
    throw new Error(`Invalid cron range "${part}": must be within ${min}–${max}`)
  }

  return [start, end]
}

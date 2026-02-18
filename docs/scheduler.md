# Scheduler

Cron-like periodic task execution — cleanup jobs, report generation, cache pruning, digest emails. Tasks are defined in code with a fluent API and run by a long-lived scheduler process that checks every minute.

## Setup

Define your tasks in `app/schedules.ts`:

```typescript
import { Scheduler } from '@stravigor/core/scheduler'

Scheduler.task('cleanup:sessions', async () => {
  await db.sql`DELETE FROM "_stravigor_sessions" WHERE "expires_at" < NOW()`
}).hourly()

Scheduler.task('reports:daily', async () => {
  const report = await generateDailyReport()
  await saveReport(report)
}).dailyAt('02:00')
  .withoutOverlapping()
```

No database tables, no DI container — `Scheduler` is a static registry like `Emitter`.

## Defining tasks

Register a task with a name and handler. Chain a frequency method to set its schedule:

```typescript
import { Scheduler } from '@stravigor/core/scheduler'

Scheduler.task('task-name', async () => {
  // your logic
}).everyFiveMinutes()
```

The handler can be sync or async. The name is used for logging and overlap tracking.

## Frequency methods

### Minute-based

```typescript
.everyMinute()            // * * * * *
.everyTwoMinutes()        // */2 * * * *
.everyFiveMinutes()       // */5 * * * *
.everyTenMinutes()        // */10 * * * *
.everyFifteenMinutes()    // */15 * * * *
.everyThirtyMinutes()     // */30 * * * *
```

### Hourly

```typescript
.hourly()                 // at minute 0
.hourlyAt(45)             // at minute 45
```

### Daily

```typescript
.daily()                  // midnight
.dailyAt('02:30')         // at 02:30 UTC
.twiceDaily(8, 20)        // at 08:00 and 20:00
```

### Weekly

```typescript
.weekly()                 // Sunday at midnight
.weeklyOn('monday', '08:00')
.weeklyOn(5)              // Friday at midnight (0=Sun, 1=Mon, ..., 6=Sat)
```

Day names are case-insensitive and accept full names or abbreviations: `monday`/`mon`, `tuesday`/`tue`, etc.

### Monthly

```typescript
.monthly()                // 1st at midnight
.monthlyOn(15, '09:30')   // 15th at 09:30
```

### Raw cron

For complex schedules, use a standard 5-field cron expression:

```typescript
.cron('*/10 8-17 * * 1-5')   // every 10 min, 8am–5pm, weekdays
.cron('0 0 1,15 * *')        // 1st and 15th at midnight
```

Supported syntax: `*`, exact (`5`), range (`1-5`), list (`1,3,5`), step (`*/10`), range+step (`1-30/5`).

### Sporadic (human-like)

Run a task at random intervals within a range, simulating non-periodic, human-like behavior — useful for web scraping, polling external APIs, or any task where predictable timing is undesirable:

```typescript
import { Scheduler, TimeUnit } from '@stravigor/core/scheduler'

// Run every 5–30 minutes at random
Scheduler.task('scrape:prices', async () => {
  await scrapePrices()
}).sporadically(5, 30, TimeUnit.Minutes)

// Run every 1–4 hours at random, prevent overlap
Scheduler.task('poll:feed', async () => {
  await pollExternalFeed()
}).sporadically(1, 4, TimeUnit.Hours)
  .withoutOverlapping()
```

Available units: `TimeUnit.Minutes`, `TimeUnit.Hours`, `TimeUnit.Days`.

Each time the task fires, the next run is scheduled at a new random delay within the range. The `nextRunAt` getter exposes the next scheduled time for debugging:

```typescript
const schedule = Scheduler.task('poll', handler).sporadically(10, 60, TimeUnit.Minutes)
console.log(schedule.nextRunAt)  // Date
```

## Options

### Overlap prevention

Prevent a task from running if the previous run hasn't finished yet (in-memory, single-process):

```typescript
Scheduler.task('heavy-report', async () => {
  await generateLargeReport()  // takes 10+ minutes
}).everyFiveMinutes()
  .withoutOverlapping()
```

If the task is still running at the next tick, it is skipped.

## Running the scheduler

### CLI

```bash
bun strav schedule
```

Press Ctrl+C to stop gracefully — the scheduler finishes active tasks before exiting.

### How it works

1. The runner sleeps until the next minute boundary (`XX:XX:00`).
2. Checks which tasks are due using `Scheduler.due(now)`.
3. Executes all due tasks concurrently via `Promise.allSettled` — one failing task doesn't block others.
4. Repeats until stopped.

Errors are logged to stderr but never crash the scheduler process.

## Cron parser

The built-in parser is also available standalone:

```typescript
import { parseCron, cronMatches, nextCronDate } from '@stravigor/core/scheduler'

const cron = parseCron('0 2 * * 1')           // Mondays at 2am
cronMatches(cron, new Date())                  // true/false
const next = nextCronDate(cron, new Date())    // next matching Date (UTC)
```

Standard cron rule: when both day-of-month and day-of-week are restricted, either match satisfies (OR logic).

## Full example

```typescript
// app/schedules.ts
import { Scheduler, TimeUnit } from '@stravigor/core/scheduler'
import { cache } from '@stravigor/core/cache'
import { Queue } from '@stravigor/core/queue'

// Cleanup expired sessions every hour
Scheduler.task('cleanup:sessions', async () => {
  const result = await db.sql`
    DELETE FROM "_stravigor_sessions" WHERE "expires_at" < NOW()
  `
  console.log(`Cleaned ${result.count} expired sessions`)
}).hourly()

// Generate reports at 2am, prevent overlap
Scheduler.task('reports:daily', async () => {
  const report = await generateDailyReport()
  await saveReport(report)
}).dailyAt('02:00')
  .withoutOverlapping()

// Prune cache every 6 hours
Scheduler.task('cache:prune', async () => {
  await cache.flush()
}).cron('0 */6 * * *')

// Send weekly digest on Monday mornings
Scheduler.task('emails:digest', async () => {
  const users = await User.where('digest_enabled', true).all()
  for (const user of users) {
    await Queue.push('send-digest', { userId: user.id })
  }
}).weeklyOn('monday', '08:00')

// Scrape competitor prices at random intervals (human-like)
Scheduler.task('scrape:competitors', async () => {
  await scrapeCompetitorPrices()
}).sporadically(10, 45, TimeUnit.Minutes)
  .withoutOverlapping()
```

## Testing

Use `Scheduler.reset()` in your test teardown:

```typescript
import { afterEach } from 'bun:test'
import { Scheduler } from '@stravigor/core/scheduler'

afterEach(() => {
  Scheduler.reset()
})
```

Test task scheduling without running the scheduler loop:

```typescript
import { Scheduler } from '@stravigor/core/scheduler'

Scheduler.task('test', handler).dailyAt('02:00')

// Check if due at a specific time
const due = Scheduler.due(new Date('2024-06-15T02:00:00Z'))
expect(due).toHaveLength(1)
expect(due[0].name).toBe('test')
```

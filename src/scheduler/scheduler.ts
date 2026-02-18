import { Schedule } from './schedule.ts'
import type { TaskHandler } from './schedule.ts'

/**
 * Static task registry for periodic jobs.
 *
 * No DI, no database — tasks are registered in code and evaluated
 * in-memory by the {@link SchedulerRunner}.
 *
 * @example
 * Scheduler.task('cleanup:sessions', async () => {
 *   await db.sql`DELETE FROM "_strav_sessions" WHERE "expires_at" < NOW()`
 * }).hourly()
 *
 * Scheduler.task('reports:daily', () => generateDailyReport()).dailyAt('02:00')
 */
export default class Scheduler {
  private static _tasks: Schedule[] = []

  /**
   * Register a periodic task. Returns the {@link Schedule} for fluent configuration.
   *
   * @example
   * Scheduler.task('prune-cache', () => cache.flush()).everyFifteenMinutes()
   */
  static task(name: string, handler: TaskHandler): Schedule {
    const schedule = new Schedule(name, handler)
    Scheduler._tasks.push(schedule)
    return schedule
  }

  /** All registered tasks. */
  static get tasks(): readonly Schedule[] {
    return Scheduler._tasks
  }

  /** Return tasks that are due at the given time (defaults to now, UTC). */
  static due(now?: Date): Schedule[] {
    const date = now ?? new Date()
    return Scheduler._tasks.filter(t => t.isDue(date))
  }

  /** Clear all registered tasks. For testing. */
  static reset(): void {
    Scheduler._tasks = []
  }
}

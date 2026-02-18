import Scheduler from './scheduler.ts'
import type { Schedule } from './schedule.ts'

export interface RunnerOptions {
  /** Timezone for schedule evaluation. @default 'UTC' */
  timezone?: string
}

/**
 * Long-running scheduler loop.
 *
 * Aligns to minute boundaries (standard cron behaviour), checks which
 * tasks are due, and executes them concurrently. Supports graceful
 * shutdown via SIGINT/SIGTERM and per-task overlap prevention.
 *
 * @example
 * const runner = new SchedulerRunner()
 * await runner.start() // blocks until runner.stop()
 */
export default class SchedulerRunner {
  private running = false
  private active = new Set<string>()

  /** Start the scheduler loop. Blocks until stop() is called. */
  async start(): Promise<void> {
    this.running = true

    const onSignal = () => this.stop()
    process.on('SIGINT', onSignal)
    process.on('SIGTERM', onSignal)

    try {
      while (this.running) {
        await this.sleepUntilNextMinute()
        if (!this.running) break

        const now = new Date()
        const due = Scheduler.due(now)

        if (due.length > 0) {
          await this.runAll(due)
        }
      }

      // Wait for active tasks to finish before exiting
      if (this.active.size > 0) {
        await this.waitForActive()
      }
    } finally {
      process.off('SIGINT', onSignal)
      process.off('SIGTERM', onSignal)
    }
  }

  /** Signal the runner to stop after the current tick completes. */
  stop(): void {
    this.running = false
  }

  /** Number of tasks currently executing. */
  get activeCount(): number {
    return this.active.size
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  /** Sleep until the start of the next minute (XX:XX:00.000). */
  private async sleepUntilNextMinute(): Promise<void> {
    const now = Date.now()
    const nextMinute = Math.ceil(now / 60_000) * 60_000
    const delay = nextMinute - now

    // Minimum 1s, maximum 60s — avoid sleeping 0ms on exact boundaries
    const ms = Math.max(1_000, Math.min(delay, 60_000))
    await Bun.sleep(ms)
  }

  /** Execute all due tasks concurrently. */
  private async runAll(tasks: Schedule[]): Promise<void> {
    const promises: Promise<void>[] = []

    for (const task of tasks) {
      if (task.preventsOverlap && this.active.has(task.name)) {
        continue
      }

      promises.push(this.runTask(task))
    }

    await Promise.allSettled(promises)
  }

  /** Execute a single task with overlap tracking and error handling. */
  private async runTask(task: Schedule): Promise<void> {
    this.active.add(task.name)
    try {
      await task.handler()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      console.error(`[scheduler] Task "${task.name}" failed: ${message}`)
    } finally {
      this.active.delete(task.name)
    }
  }

  /** Wait for all active tasks to complete (for graceful shutdown). */
  private async waitForActive(): Promise<void> {
    const maxWait = 30_000
    const start = Date.now()
    while (this.active.size > 0 && Date.now() - start < maxWait) {
      await Bun.sleep(100)
    }
  }
}

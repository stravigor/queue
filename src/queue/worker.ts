import Queue, { hydrateJob } from './queue.ts'
import Emitter from '@stravigor/kernel/events/emitter'
import type { JobRecord, JobMeta } from './queue.ts'

export interface WorkerOptions {
  queue?: string
  sleep?: number
}

/**
 * Processes jobs from the queue.
 *
 * Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent polling.
 * Supports job timeouts, exponential/linear backoff for retries,
 * and graceful shutdown on SIGINT/SIGTERM.
 *
 * @example
 * const worker = new Worker({ queue: 'emails', sleep: 500 })
 * await worker.start() // blocks until worker.stop() is called
 */
export default class Worker {
  private running = false
  private processing = false
  private queue: string
  private sleep: number

  constructor(options: WorkerOptions = {}) {
    this.queue = options.queue ?? Queue.config.default
    this.sleep = options.sleep ?? Queue.config.sleep
  }

  /** Start the worker loop. Blocks until stop() is called. */
  async start(): Promise<void> {
    this.running = true

    const onSignal = () => this.stop()
    process.on('SIGINT', onSignal)
    process.on('SIGTERM', onSignal)

    let pollCount = 0

    try {
      while (this.running) {
        // Periodically release stale jobs (every 60 cycles)
        if (pollCount % 60 === 0) {
          await this.releaseStaleJobs()
        }
        pollCount++

        const job = await this.fetchNext()
        if (job) {
          this.processing = true
          await this.process(job)
          this.processing = false
        } else {
          await Bun.sleep(this.sleep)
        }
      }
    } finally {
      process.off('SIGINT', onSignal)
      process.off('SIGTERM', onSignal)
    }
  }

  /** Signal the worker to stop after the current job completes. */
  stop(): void {
    this.running = false
  }

  /** Whether the worker is currently processing a job. */
  get busy(): boolean {
    return this.processing
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  /**
   * Fetch the next available job using FOR UPDATE SKIP LOCKED.
   * Atomically reserves it by setting reserved_at and incrementing attempts.
   */
  private async fetchNext(): Promise<JobRecord | null> {
    const sql = Queue.db.sql

    const rows = await sql.begin(async (tx: any) => {
      const result = await tx`
        SELECT * FROM "_strav_jobs"
        WHERE "queue" = ${this.queue}
          AND "available_at" <= NOW()
          AND "reserved_at" IS NULL
        ORDER BY "available_at" ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      `
      if (result.length === 0) return []

      const job = result[0] as Record<string, unknown>
      await tx`
        UPDATE "_strav_jobs"
        SET "reserved_at" = NOW(), "attempts" = "attempts" + 1
        WHERE "id" = ${job.id}
      `
      return [{ ...job, attempts: (job.attempts as number) + 1 }]
    })

    return rows.length > 0 ? hydrateJob(rows[0] as Record<string, unknown>) : null
  }

  /** Process a single job: run handler, handle success/failure. */
  private async process(job: JobRecord): Promise<void> {
    const handler = Queue.handlers.get(job.job)

    if (!handler) {
      await this.fail(job, new Error(`No handler registered for job "${job.job}"`))
      return
    }

    const meta: JobMeta = {
      id: job.id,
      queue: job.queue,
      job: job.job,
      attempts: job.attempts,
      maxAttempts: job.maxAttempts,
    }

    const start = performance.now()

    try {
      await Promise.race([
        Promise.resolve(handler(job.payload, meta)),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error(`Job "${job.job}" timed out after ${job.timeout}ms`)),
            job.timeout
          )
        ),
      ])
      await this.complete(job)

      if (Emitter.listenerCount('queue:processed') > 0) {
        const duration = performance.now() - start
        Emitter.emit('queue:processed', {
          job: job.job,
          id: job.id,
          queue: job.queue,
          duration,
        }).catch(() => {})
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      if (job.attempts >= job.maxAttempts) {
        await this.fail(job, err)

        if (Emitter.listenerCount('queue:failed') > 0) {
          const duration = performance.now() - start
          Emitter.emit('queue:failed', {
            job: job.job,
            id: job.id,
            queue: job.queue,
            error: err.message,
            duration,
          }).catch(() => {})
        }
      } else {
        await this.release(job)
      }
    }
  }

  /** Delete a completed job. */
  private async complete(job: JobRecord): Promise<void> {
    await Queue.db.sql`DELETE FROM "_strav_jobs" WHERE "id" = ${job.id}`
  }

  /** Move a job to the failed_jobs table and delete from jobs. */
  private async fail(job: JobRecord, error: Error): Promise<void> {
    const sql = Queue.db.sql
    await sql.begin(async (tx: any) => {
      await tx`
        INSERT INTO "_strav_failed_jobs" ("queue", "job", "payload", "error")
        VALUES (${job.queue}, ${job.job}, ${JSON.stringify(job.payload)}, ${error.message})
      `
      await tx`DELETE FROM "_strav_jobs" WHERE "id" = ${job.id}`
    })
  }

  /** Release a job back to the queue with incremented backoff delay. */
  private async release(job: JobRecord): Promise<void> {
    const delay = this.backoffDelay(job.attempts)
    const availableAt = new Date(Date.now() + delay)

    await Queue.db.sql`
      UPDATE "_strav_jobs"
      SET "reserved_at" = NULL, "available_at" = ${availableAt}
      WHERE "id" = ${job.id}
    `
  }

  /** Calculate backoff delay in ms based on attempt number. */
  backoffDelay(attempts: number): number {
    if (Queue.config.retryBackoff === 'linear') {
      return attempts * 5_000
    }
    // Exponential: 2^attempts * 1000, with jitter
    const base = Math.pow(2, attempts) * 1000
    const jitter = Math.random() * 1000
    return base + jitter
  }

  /** Release jobs that have been reserved for too long (crashed workers). */
  private async releaseStaleJobs(): Promise<void> {
    await Queue.db.sql`
      UPDATE "_strav_jobs"
      SET "reserved_at" = NULL
      WHERE "reserved_at" IS NOT NULL
        AND "queue" = ${this.queue}
        AND "reserved_at" < NOW() - MAKE_INTERVAL(secs => "timeout" * 2.0 / 1000)
    `
  }
}

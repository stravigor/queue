import { inject } from '@stravigor/kernel/core/inject'
import Configuration from '@stravigor/kernel/config/configuration'
import Database from '@stravigor/database/database/database'
import Emitter from '@stravigor/kernel/events/emitter'
import { ConfigurationError } from '@stravigor/kernel/exceptions/errors'

export interface JobOptions {
  queue?: string
  delay?: number
  attempts?: number
  timeout?: number
}

export interface QueueConfig {
  default: string
  maxAttempts: number
  timeout: number
  retryBackoff: 'exponential' | 'linear'
  sleep: number
}

/** Metadata passed to job handlers alongside the payload. */
export interface JobMeta {
  id: number
  queue: string
  job: string
  attempts: number
  maxAttempts: number
}

/** A raw job row from the _strav_jobs table. */
export interface JobRecord {
  id: number
  queue: string
  job: string
  payload: unknown
  attempts: number
  maxAttempts: number
  timeout: number
  availableAt: Date
  reservedAt: Date | null
  createdAt: Date
}

/** A raw row from the _strav_failed_jobs table. */
export interface FailedJobRecord {
  id: number
  queue: string
  job: string
  payload: unknown
  error: string
  failedAt: Date
}

export type JobHandler<T = any> = (payload: T, meta: JobMeta) => void | Promise<void>

/**
 * PostgreSQL-backed job queue.
 *
 * Resolved once via the DI container — stores the database reference
 * and parsed config for Worker and all static methods.
 *
 * @example
 * app.singleton(Queue)
 * app.resolve(Queue)
 * await Queue.ensureTables()
 *
 * Queue.handle('send-email', async (payload) => { ... })
 * await Queue.push('send-email', { to: 'user@example.com' })
 */
@inject
export default class Queue {
  private static _db: Database
  private static _config: QueueConfig
  private static _handlers = new Map<string, JobHandler>()

  constructor(db: Database, config: Configuration) {
    Queue._db = db
    Queue._config = {
      default: config.get('queue.default', 'default') as string,
      maxAttempts: config.get('queue.maxAttempts', 3) as number,
      timeout: config.get('queue.timeout', 60_000) as number,
      retryBackoff: config.get('queue.retryBackoff', 'exponential') as 'exponential' | 'linear',
      sleep: config.get('queue.sleep', 1000) as number,
    }
  }

  static get db(): Database {
    if (!Queue._db)
      throw new ConfigurationError(
        'Queue not configured. Resolve Queue through the container first.'
      )
    return Queue._db
  }

  static get config(): QueueConfig {
    return Queue._config
  }

  static get handlers(): Map<string, JobHandler> {
    return Queue._handlers
  }

  /** Create the internal jobs and failed_jobs tables if they don't exist. */
  static async ensureTables(): Promise<void> {
    const sql = Queue.db.sql

    await sql`
      CREATE TABLE IF NOT EXISTS "_strav_jobs" (
        "id" BIGSERIAL PRIMARY KEY,
        "queue" VARCHAR(255) NOT NULL DEFAULT 'default',
        "job" VARCHAR(255) NOT NULL,
        "payload" JSONB NOT NULL DEFAULT '{}',
        "attempts" INT NOT NULL DEFAULT 0,
        "max_attempts" INT NOT NULL DEFAULT 3,
        "timeout" INT NOT NULL DEFAULT 60000,
        "available_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        "reserved_at" TIMESTAMPTZ,
        "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `

    await sql`
      CREATE INDEX IF NOT EXISTS "idx_strav_jobs_queue_available"
        ON "_strav_jobs" ("queue", "available_at")
        WHERE "reserved_at" IS NULL
    `

    await sql`
      CREATE TABLE IF NOT EXISTS "_strav_failed_jobs" (
        "id" BIGSERIAL PRIMARY KEY,
        "queue" VARCHAR(255) NOT NULL,
        "job" VARCHAR(255) NOT NULL,
        "payload" JSONB NOT NULL DEFAULT '{}',
        "error" TEXT NOT NULL,
        "failed_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `
  }

  /** Register a handler for a named job. */
  static handle<T = any>(name: string, handler: JobHandler<T>): void {
    Queue._handlers.set(name, handler)
  }

  /**
   * Push a job onto the queue. Returns the job ID.
   */
  static async push<T = any>(name: string, payload: T, options?: JobOptions): Promise<number> {
    const sql = Queue.db.sql
    const queue = options?.queue ?? Queue._config.default
    const maxAttempts = options?.attempts ?? Queue._config.maxAttempts
    const timeout = options?.timeout ?? Queue._config.timeout
    const availableAt = options?.delay ? new Date(Date.now() + options.delay) : new Date()

    const rows = await sql`
      INSERT INTO "_strav_jobs" ("queue", "job", "payload", "max_attempts", "timeout", "available_at")
      VALUES (${queue}, ${name}, ${JSON.stringify(payload)}, ${maxAttempts}, ${timeout}, ${availableAt})
      RETURNING "id"
    `

    const id = Number((rows[0] as Record<string, unknown>).id)

    if (Emitter.listenerCount('queue:dispatched') > 0) {
      Emitter.emit('queue:dispatched', { id, name, queue, payload }).catch(() => {})
    }

    return id
  }

  /**
   * Create a listener function suitable for Emitter.on().
   * When the event fires, the payload is pushed onto the queue.
   *
   * @example
   * Emitter.on('user.registered', Queue.listener('send-welcome-email'))
   */
  static listener(jobName: string, options?: JobOptions): (payload: any) => Promise<void> {
    return async (payload: any) => {
      await Queue.push(jobName, payload, options)
    }
  }

  // ---------------------------------------------------------------------------
  // Introspection / Management
  // ---------------------------------------------------------------------------

  /** Count of pending (unreserved) jobs in a queue. */
  static async size(queue?: string): Promise<number> {
    const sql = Queue.db.sql
    const q = queue ?? Queue._config.default

    const rows = await sql`
      SELECT COUNT(*)::int AS count FROM "_strav_jobs"
      WHERE "queue" = ${q} AND "reserved_at" IS NULL
    `
    return (rows[0] as Record<string, unknown>).count as number
  }

  /** List pending jobs, most recent first. */
  static async pending(queue?: string, limit = 25): Promise<JobRecord[]> {
    const sql = Queue.db.sql
    const q = queue ?? Queue._config.default

    const rows = await sql`
      SELECT * FROM "_strav_jobs"
      WHERE "queue" = ${q}
      ORDER BY "available_at" ASC
      LIMIT ${limit}
    `
    return rows.map(hydrateJob)
  }

  /** List failed jobs, most recent first. */
  static async failed(queue?: string, limit = 25): Promise<FailedJobRecord[]> {
    const sql = Queue.db.sql

    if (queue) {
      const rows = await sql`
        SELECT * FROM "_strav_failed_jobs"
        WHERE "queue" = ${queue}
        ORDER BY "failed_at" DESC
        LIMIT ${limit}
      `
      return rows.map(hydrateFailedJob)
    }

    const rows = await sql`
      SELECT * FROM "_strav_failed_jobs"
      ORDER BY "failed_at" DESC
      LIMIT ${limit}
    `
    return rows.map(hydrateFailedJob)
  }

  /** Delete all pending jobs in a queue. Returns the number deleted. */
  static async clear(queue?: string): Promise<number> {
    const sql = Queue.db.sql
    const q = queue ?? Queue._config.default

    const rows = await sql`
      DELETE FROM "_strav_jobs" WHERE "queue" = ${q}
    `
    return rows.count
  }

  /** Move all failed jobs back to the jobs table. Returns the number retried. */
  static async retryFailed(queue?: string): Promise<number> {
    const sql = Queue.db.sql

    if (queue) {
      const failed = await sql`
        DELETE FROM "_strav_failed_jobs" WHERE "queue" = ${queue} RETURNING *
      `
      for (const row of failed) {
        const r = row as Record<string, unknown>
        await sql`
          INSERT INTO "_strav_jobs" ("queue", "job", "payload", "max_attempts")
          VALUES (${r.queue}, ${r.job}, ${typeof r.payload === 'string' ? r.payload : JSON.stringify(r.payload)}, ${Queue._config.maxAttempts})
        `
      }
      return failed.length
    }

    const failed = await sql`
      DELETE FROM "_strav_failed_jobs" RETURNING *
    `
    for (const row of failed) {
      const r = row as Record<string, unknown>
      await sql`
        INSERT INTO "_strav_jobs" ("queue", "job", "payload", "max_attempts")
        VALUES (${r.queue}, ${r.job}, ${typeof r.payload === 'string' ? r.payload : JSON.stringify(r.payload)}, ${Queue._config.maxAttempts})
      `
    }
    return failed.length
  }

  /** Delete all failed jobs for a queue (or all queues). Returns the number deleted. */
  static async clearFailed(queue?: string): Promise<number> {
    const sql = Queue.db.sql

    if (queue) {
      const rows = await sql`
        DELETE FROM "_strav_failed_jobs" WHERE "queue" = ${queue}
      `
      return rows.count
    }

    const rows = await sql`
      DELETE FROM "_strav_failed_jobs"
    `
    return rows.count
  }

  /** Delete all jobs and failed jobs across all queues. For dev/test only. */
  static async flush(): Promise<void> {
    const sql = Queue.db.sql
    await sql`DELETE FROM "_strav_jobs"`
    await sql`DELETE FROM "_strav_failed_jobs"`
  }

  /** Reset static state. For testing only. */
  static reset(): void {
    Queue._handlers.clear()
  }
}

// ---------------------------------------------------------------------------
// Hydration helpers
// ---------------------------------------------------------------------------

function parsePayload(raw: unknown): unknown {
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw)
    } catch {
      return raw
    }
  }
  return raw
}

export function hydrateJob(row: Record<string, unknown>): JobRecord {
  return {
    id: Number(row.id),
    queue: row.queue as string,
    job: row.job as string,
    payload: parsePayload(row.payload),
    attempts: row.attempts as number,
    maxAttempts: row.max_attempts as number,
    timeout: row.timeout as number,
    availableAt: row.available_at as Date,
    reservedAt: (row.reserved_at as Date) ?? null,
    createdAt: row.created_at as Date,
  }
}

function hydrateFailedJob(row: Record<string, unknown>): FailedJobRecord {
  return {
    id: Number(row.id),
    queue: row.queue as string,
    job: row.job as string,
    payload: parsePayload(row.payload),
    error: row.error as string,
    failedAt: row.failed_at as Date,
  }
}

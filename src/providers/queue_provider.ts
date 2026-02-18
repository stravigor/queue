import ServiceProvider from '@stravigor/kernel/core/service_provider'
import type Application from '@stravigor/kernel/core/application'
import Queue from '../queue/queue.ts'

export interface QueueProviderOptions {
  /** Whether to auto-create the jobs tables. Default: `true` */
  ensureTables?: boolean
}

export default class QueueProvider extends ServiceProvider {
  readonly name = 'queue'
  override readonly dependencies = ['database']

  constructor(private options?: QueueProviderOptions) {
    super()
  }

  override register(app: Application): void {
    app.singleton(Queue)
  }

  override async boot(app: Application): Promise<void> {
    app.resolve(Queue)

    if (this.options?.ensureTables !== false) {
      await Queue.ensureTables()
    }
  }
}

import { IBaseComponent } from '@well-known-components/interfaces'
import future from 'fp-future'
import PQueue from 'p-queue'

/**
 * Abstract job queue
 * @public
 */
export type IJobQueue = {
  /**
   * Schedules a job.
   */
  scheduleJob<T>(fn: () => Promise<T>): Promise<T>
  /**
   * Schedules a job with retries. If it fails (throws), then the job goes back to the end of the queue to be processed later.
   */
  scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T>
}

export function createJobQueue(options: createJobQueue.Options): IJobQueue & IBaseComponent {
  const realQueue = new PQueue({
    concurrency: options.concurrency,
    autoStart: options.autoStart ?? true,
    timeout: options.timeout,
  })

  return {
    scheduleJob<T>(fn: () => Promise<T>): Promise<T> {
      return realQueue.add(fn)
    },
    scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T> {
      if (!(retries | 0)) {
        throw new Error('At least one retry is required')
      }
      const resultFuture = future<T>()
      let retry = retries | 0

      function schedule() {
        realQueue.add(async () => {
          retry--

          try {
            resultFuture.resolve(await fn())
          } catch (e: any) {
            if (!retry) {
              resultFuture.reject(e)
            } else {
              schedule()
            }
          }
        })
      }

      schedule()

      return resultFuture
    },
    async stop() {
      // wait until the jobs are finished at stop()
      await realQueue.onIdle()
    },
  }
}

export namespace createJobQueue {
  export type Options = {
    autoStart?: boolean
    concurrency?: number
    timeout?: number
  }
}

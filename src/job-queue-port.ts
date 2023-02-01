import { IBaseComponent } from '@well-known-components/interfaces'
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
  /*
   * Returns a promise that settles when the queue size is less than the given limit:
   * queue.size < limit.
   */
  onSizeLessThan(limit: number): Promise<void>
  /**
   * Schedules a job with retries. If it fails (throws), then the job goes back to the end of the queue to be processed later.
   */
  scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T>
  /**
   * Schedules a job with priority.
   * Operations with greater priority will be scheduled first.
   */
  scheduleJobWithPriority<T>(fn: () => Promise<T>, priority: number): Promise<T>
  /**
   * All finished
   */
  onIdle(): Promise<void>
}

export function createJobQueue(options: createJobQueue.Options): IJobQueue & IBaseComponent {
  const realQueue = new PQueue({
    concurrency: options.concurrency,
    autoStart: options.autoStart ?? true,
    timeout: options.timeout
  })

  return {
    onIdle() {
      return realQueue.onIdle()
    },
    scheduleJob<T>(fn: () => Promise<T>): Promise<T> {
      return realQueue.add(fn)
    },
    async onSizeLessThan(limit: number): Promise<void> {
      // Instantly resolve if the queue is empty.
      if (realQueue.size < limit) {
        return
      }

      return new Promise((resolve) => {
        const listener = () => {
          if (realQueue.size < limit) {
            realQueue.off('next', listener)
            resolve()
          }
        }

        realQueue.on('next', listener)
      })
    },
    scheduleJobWithPriority<T>(fn: () => Promise<T>, priority: number): Promise<T> {
      return realQueue.add(fn, {
        priority
      })
    },
    scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T> {
      if (!(retries | 0)) {
        throw new Error('At least one retry is required')
      }
      return new Promise<T>((resolve, reject) => {
        function schedule(retries: number) {
          realQueue
            .add(async () => {
              try {
                resolve(await fn())
              } catch (e: any) {
                if (retries <= 0) {
                  reject(e)
                } else {
                  schedule(retries - 1)
                }
              }
            })
            .catch(reject)
        }

        schedule(retries)
      })
    },
    async stop() {
      // wait until the jobs are finished at stop()
      await realQueue.onIdle()
    }
  }
}

export namespace createJobQueue {
  export type Options = {
    autoStart?: boolean
    concurrency?: number
    timeout?: number
  }
}

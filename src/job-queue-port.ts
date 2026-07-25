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
    timeout: options.timeout,
    // p-queue defaults throwOnTimeout to false, which makes a timed-out job *resolve* as if it had
    // succeeded while its function keeps running uncancelled. That hid timeouts from
    // scheduleJobWithRetries and let stop() report the queue drained while jobs were still in flight.
    throwOnTimeout: options.timeout !== undefined
  })

  // All onSizeLessThan waiters share a single 'next' subscription. One listener per call would grow
  // with every pending waiter (tripping Node's max-listeners warning) and would stay attached for as
  // long as a waiter's limit is unmet; here the subscription exists only while someone is waiting.
  const sizeWaiters = new Set<{ limit: number; resolve: () => void }>()
  let sizeListenerAttached = false

  function notifySizeWaiters() {
    for (const waiter of Array.from(sizeWaiters)) {
      if (realQueue.size < waiter.limit) {
        sizeWaiters.delete(waiter)
        waiter.resolve()
      }
    }
    if (sizeWaiters.size === 0 && sizeListenerAttached) {
      realQueue.off('next', notifySizeWaiters)
      sizeListenerAttached = false
    }
  }

  return {
    onIdle() {
      return realQueue.onIdle()
    },
    scheduleJob<T>(fn: () => Promise<T>): Promise<T> {
      return realQueue.add(fn)
    },
    async onSizeLessThan(limit: number): Promise<void> {
      // Instantly resolve if the queue is already below the limit.
      if (realQueue.size < limit) {
        return
      }

      return new Promise<void>((resolve) => {
        sizeWaiters.add({ limit, resolve })
        if (!sizeListenerAttached) {
          realQueue.on('next', notifySizeWaiters)
          sizeListenerAttached = true
        }
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
        function schedule(remainingRetries: number) {
          // The job is added as-is so the queue owns its outcome. Settling inside the queued function
          // instead hid every queue-level rejection — a timeout in particular — from the retry logic,
          // because the function's own try/catch never sees it.
          realQueue
            .add(fn)
            .then(resolve)
            .catch((err: any) => {
              if (remainingRetries <= 0) {
                reject(err)
              } else {
                schedule(remainingRetries - 1)
              }
            })
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

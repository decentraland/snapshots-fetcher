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
   *
   * A configured `timeout` counts as a failure and is retried. Note that nothing can cancel a
   * JavaScript promise, so a timed-out attempt keeps running while its retry starts: **the scheduled
   * function must be idempotent and safe to run concurrently with itself.** `onIdle()` and `stop()` do
   * wait for those abandoned attempts, so quiescence is still accurate.
   */
  scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T>
  /**
   * Schedules a job with priority.
   * Operations with greater priority will be scheduled first.
   */
  scheduleJobWithPriority<T>(fn: () => Promise<T>, priority: number): Promise<T>
  /**
   * Resolves when nothing is queued and nothing is still executing.
   *
   * This waits for the scheduled functions themselves, not just for the queue's own bookkeeping: a job
   * that hit the configured `timeout` is no longer counted by the queue but is still running, since a
   * promise cannot be cancelled.
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

  // Every scheduled function that has actually started and not yet settled.
  //
  // p-queue cannot cancel a function it has timed out: with `throwOnTimeout` the queue promise rejects
  // and the queue stops counting the task, but the function keeps running — and may keep mutating
  // whatever it was mutating. Tracking executions ourselves is what makes onIdle()/stop() mean
  // "nothing is running" rather than "the queue has stopped waiting".
  const inFlightExecutions = new Set<Promise<void>>()

  function tracked<T>(fn: () => Promise<T>): () => Promise<T> {
    return () => {
      const running = fn()
      const settled = Promise.resolve(running).then(
        () => undefined,
        () => undefined
      )
      inFlightExecutions.add(settled)
      void settled.then(() => inFlightExecutions.delete(settled))
      return running
    }
  }

  async function waitUntilQuiescent(): Promise<void> {
    // Looped because draining either side can feed the other: a retry can be queued while we await the
    // executions, and an execution can outlive the queue's own idea of idle.
    do {
      await realQueue.onIdle()
      await Promise.all(Array.from(inFlightExecutions))
    } while (realQueue.size > 0 || realQueue.pending > 0 || inFlightExecutions.size > 0)
  }

  return {
    onIdle() {
      return waitUntilQuiescent()
    },
    scheduleJob<T>(fn: () => Promise<T>): Promise<T> {
      return realQueue.add(tracked(fn))
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
      return realQueue.add(tracked(fn), {
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
            .add(tracked(fn))
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
      await waitUntilQuiescent()
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

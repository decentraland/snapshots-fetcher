import { IBaseComponent } from '@well-known-components/interfaces'
import PQueue from 'p-queue'

/**
 * Abstract job queue.
 *
 * Lifecycle: `stop()` is terminal. It waits for everything already scheduled to finish, and from the
 * moment it is called the queue refuses new work — `scheduleJob`, `scheduleJobWithPriority` and
 * `scheduleJobWithRetries` throw, and a retry ladder already in flight stops re-enqueueing itself. A
 * stopped queue cannot be restarted; build another.
 *
 * A queue built with `autoStart: false` is started by `stop()` so its queued work can drain — otherwise
 * shutdown would wait on jobs that never begin. `onIdle()` deliberately does not do this: it is a query
 * about the current work, not a lifecycle action, so awaiting it on a queue that was never started waits
 * for something that will not happen. Use `onIdle()` when you want "wait for the current work to drain"
 * without ending the queue.
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

  // Terminal once stop() begins. The queue is exported from the package root, and a stop() that leaves
  // the queue accepting work means a consumer that called it can still have jobs start afterwards — the
  // exact "stop() does not stop" shape this package has been correcting elsewhere. Refusing new work also
  // makes stop() converge instead of chasing retries that keep re-enqueueing themselves.
  let stopped = false

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

  function assertNotStopped() {
    if (stopped) {
      throw new Error('The job queue was stopped and no longer accepts jobs')
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
      assertNotStopped()
      return realQueue.add(tracked(fn))
    },
    async onSizeLessThan(limit: number): Promise<void> {
      // A limit of 0 or below can never be satisfied — the queue size is never negative — so the caller
      // would wait forever with no indication why. Fail instead of hanging.
      if (!Number.isInteger(limit) || limit < 1) {
        throw new Error(`limit must be an integer >= 1, got ${limit}`)
      }

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
      assertNotStopped()
      return realQueue.add(tracked(fn), {
        priority
      })
    },
    scheduleJobWithRetries<T>(fn: () => Promise<T>, retries: number): Promise<T> {
      // Plain integer validation, not the bitwise `!(retries | 0)` this used to do. `| 0` coerces to
      // int32, so it let 2.5 through as 2 and -1 through as -1 (which then rejected on the first failure,
      // silently behaving like no retries at all), while wrongly refusing anything at or above 2^32
      // because the coercion wrapped it to 0. Now that the queue is exported from the package root, a
      // caller passing a bad value deserves to be told rather than to get quiet nonsense.
      if (!Number.isInteger(retries) || retries < 1) {
        throw new Error(`retries must be an integer >= 1, got ${retries}`)
      }
      assertNotStopped()
      return new Promise<T>((resolve, reject) => {
        function schedule(remainingRetries: number) {
          // Checked per attempt, not only on entry: a ladder already in flight when stop() lands must
          // not keep re-enqueueing itself, or stop() would wait for work it is trying to end.
          if (stopped) {
            reject(new Error('The job queue was stopped before this job could be retried'))
            return
          }
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
      stopped = true
      // Started before draining, or a paused queue never drains at all: onIdle() resolves on
      // `pending === 0 && size === 0`, and an autoStart:false queue holds its jobs in `size` without ever
      // starting them — so stop() waited forever and every scheduled promise stayed pending. IJobQueue
      // exposes no way to start a paused queue, so a consumer had no way out of that either. Starting it
      // here is what makes the documented "waits for everything already scheduled to finish" true
      // regardless of how the queue was constructed, rather than only for an auto-starting one.
      realQueue.start()
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

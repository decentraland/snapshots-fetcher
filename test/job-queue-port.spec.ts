import future from 'fp-future'
import { createJobQueue, IJobQueue } from '../src/job-queue-port'
import { sleep } from '../src/utils'

describe('createJobQueue', () => {
  describe('onSizeLessThan', () => {
    let queue: IJobQueue

    beforeEach(() => {
      queue = createJobQueue({ autoStart: true, concurrency: 1 })
    })

    describe('when the queue is already below the limit', () => {
      it('should resolve without waiting for a queue event', async () => {
        await expect(queue.onSizeLessThan(1)).resolves.toBeUndefined()
      })
    })

    describe('when several waiters are pending on different limits', () => {
      let releaseJobs: ReturnType<typeof future<void>>
      let resolvedWaiters: number[]

      beforeEach(async () => {
        releaseJobs = future<void>()
        resolvedWaiters = []

        // One running job plus three queued, so queue.size is 3 while the first job is blocked.
        for (let index = 0; index < 4; index++) {
          void queue.scheduleJob(async () => {
            await releaseJobs
          })
        }

        void queue.onSizeLessThan(3).then(() => resolvedWaiters.push(3))
        void queue.onSizeLessThan(2).then(() => resolvedWaiters.push(2))
        void queue.onSizeLessThan(1).then(() => resolvedWaiters.push(1))

        await sleep(10)
      })

      it('should keep every waiter pending until the queue drains past its own limit', () => {
        expect(resolvedWaiters).toEqual([])
      })

      it('should resolve the waiters in the order their limits are met', async () => {
        releaseJobs.resolve()
        await queue.onIdle()
        await sleep(10)

        expect(resolvedWaiters).toEqual([3, 2, 1])
      })
    })

    describe('and a new waiter arrives after every previous waiter was resolved', () => {
      let secondRoundResolved: boolean

      beforeEach(async () => {
        secondRoundResolved = false

        // First round: block the queue, wait for it to drain, which detaches the shared listener.
        const releaseFirstRound = future<void>()
        for (let index = 0; index < 3; index++) {
          void queue.scheduleJob(async () => {
            await releaseFirstRound
          })
        }
        const firstRoundWaiter = queue.onSizeLessThan(1)
        releaseFirstRound.resolve()
        await firstRoundWaiter

        // Second round: the shared subscription must be re-attached for these new waiters.
        const releaseSecondRound = future<void>()
        for (let index = 0; index < 3; index++) {
          void queue.scheduleJob(async () => {
            await releaseSecondRound
          })
        }
        void queue.onSizeLessThan(1).then(() => {
          secondRoundResolved = true
        })

        await sleep(10)
        releaseSecondRound.resolve()
        await queue.onIdle()
        await sleep(10)
      })

      it('should resolve the new waiter, having re-attached the shared subscription', () => {
        expect(secondRoundResolved).toBe(true)
      })
    })
  })

  describe('scheduleJobWithPriority', () => {
    let queue: IJobQueue
    let completionOrder: string[]
    let releaseBlocker: ReturnType<typeof future<void>>

    beforeEach(() => {
      completionOrder = []
      releaseBlocker = future<void>()
      queue = createJobQueue({ autoStart: true, concurrency: 1 })
    })

    it('should run the queued jobs in descending priority order', async () => {
      // The blocker occupies the single concurrency slot, so the rest queue up and their relative
      // priority decides who runs first once it is released.
      const blocker = queue.scheduleJob(async () => {
        completionOrder.push('blocker')
        await releaseBlocker
      })
      const queued = [
        queue.scheduleJobWithPriority(async () => completionOrder.push('low'), 0),
        queue.scheduleJobWithPriority(async () => completionOrder.push('high'), 10),
        queue.scheduleJobWithPriority(async () => completionOrder.push('medium'), 5)
      ]

      await sleep(10)
      releaseBlocker.resolve()
      await Promise.all([blocker, ...queued])

      expect(completionOrder).toEqual(['blocker', 'high', 'medium', 'low'])
    })
  })

  describe('scheduleJob', () => {
    let queue: IJobQueue

    beforeEach(() => {
      queue = createJobQueue({ autoStart: true, concurrency: 1 })
    })

    it('should resolve with the value the job returns', async () => {
      await expect(queue.scheduleJob(async () => 'the-value')).resolves.toBe('the-value')
    })

    it('should reject with the error the job throws', async () => {
      await expect(
        queue.scheduleJob(async () => {
          throw new Error('the job failed')
        })
      ).rejects.toThrow('the job failed')
    })
  })

  describe('scheduleJobWithRetries', () => {
    let queue: IJobQueue

    beforeEach(() => {
      queue = createJobQueue({ autoStart: true, concurrency: 1 })
    })

    describe('when the job fails fewer times than the allowed retries', () => {
      let attempts: number

      beforeEach(() => {
        attempts = 0
      })

      it('should resolve with the value of the first successful attempt', async () => {
        const result = await queue.scheduleJobWithRetries(async () => {
          attempts++
          if (attempts < 3) {
            throw new Error('transient failure')
          }
          return 'the-value'
        }, 5)

        expect(result).toBe('the-value')
      })
    })

    describe('when the job keeps failing past the allowed retries', () => {
      it('should reject with the last error', async () => {
        await expect(
          queue.scheduleJobWithRetries(async () => {
            throw new Error('permanent failure')
          }, 2)
        ).rejects.toThrow('permanent failure')
      })
    })

    describe('when a job exceeds the configured queue timeout', () => {
      let timedQueue: IJobQueue

      beforeEach(() => {
        timedQueue = createJobQueue({ autoStart: true, concurrency: 1, timeout: 30 })
      })

      it('should surface the timeout as a failure and retry, rather than resolving as a success', async () => {
        let attempts = 0

        const result = await timedQueue.scheduleJobWithRetries(async () => {
          attempts++
          if (attempts === 1) {
            // Exceeds the queue timeout. p-queue's default (throwOnTimeout: false) would resolve this
            // job as if it had succeeded while it kept running in the background.
            await sleep(200)
          }
          return `succeeded on attempt ${attempts}`
        }, 3)

        expect(result).toBe('succeeded on attempt 2')
      })
    })

    describe('when a timed-out job keeps running after the queue gave up on it', () => {
      let timedQueue: IJobQueue & { stop?: () => Promise<void> }
      let stillRunning: boolean

      beforeEach(() => {
        stillRunning = false
        timedQueue = createJobQueue({ autoStart: true, concurrency: 1, timeout: 40 }) as any
      })

      it('should not report idle until the abandoned execution has actually finished', async () => {
        // Nothing can cancel a promise, so p-queue's timeout only stops it *counting* the task — the
        // function carries on. onIdle() must still wait for it, or a consumer treats "idle" as
        // "nothing is mutating state".
        void timedQueue
          .scheduleJob(async () => {
            stillRunning = true
            await sleep(300)
            stillRunning = false
          })
          .catch(() => undefined)

        await sleep(80) // past the 40ms timeout: the queue has already given up
        await timedQueue.onIdle()

        expect(stillRunning).toBe(false)
      })

      it('should not let stop() resolve while the abandoned execution is still running', async () => {
        void timedQueue
          .scheduleJob(async () => {
            stillRunning = true
            await sleep(300)
            stillRunning = false
          })
          .catch(() => undefined)

        await sleep(80)
        await timedQueue.stop!()

        expect(stillRunning).toBe(false)
      })
    })

    describe.each([
      ['zero', 0],
      // The previous bitwise guard coerced with `| 0`, so it let these through: 2.5 became 2, and -1
      // stayed truthy and then behaved like no retries at all on the first failure.
      ['a fraction', 2.5],
      ['a negative', -1],
      ['not a number', Number.NaN]
    ])('when retries is %s', (_label: string, retries: number) => {
      it('should throw naming the expected range', () => {
        expect(() => queue.scheduleJobWithRetries(async () => 'unused', retries)).toThrow(
          `retries must be an integer >= 1, got ${retries}`
        )
      })
    })

    describe('when retries is a valid integer above the int32 range', () => {
      it('should be accepted rather than refused by an overflowing coercion', () => {
        // The previous guard computed `retries | 0`, which wraps this to 0 and rejected it.
        expect(() => queue.scheduleJobWithRetries(async () => 'unused', 2 ** 32)).not.toThrow()
      })
    })

    describe.each([
      ['zero', 0],
      ['a negative', -1]
    ])('when onSizeLessThan is given %s as a limit', (_label: string, limit: number) => {
      it('should reject rather than waiting for a size the queue can never reach', async () => {
        await expect(queue.onSizeLessThan(limit)).rejects.toThrow(`limit must be an integer >= 1, got ${limit}`)
      })
    })
    describe('when the queue has been stopped', () => {
      let stoppedQueue: ReturnType<typeof createJobQueue>

      beforeEach(async () => {
        stoppedQueue = createJobQueue({ concurrency: 1 })
        await stoppedQueue.stop!()
      })

      it('should refuse a new job rather than running it after the caller stopped the queue', () => {
        expect(() => stoppedQueue.scheduleJob(async () => 'unused')).toThrow('no longer accepts jobs')
      })

      it('should refuse a new prioritised job too', () => {
        expect(() => stoppedQueue.scheduleJobWithPriority(async () => 'unused', 1)).toThrow('no longer accepts jobs')
      })

      it('should refuse a new job with retries too', () => {
        expect(() => stoppedQueue.scheduleJobWithRetries(async () => 'unused', 2)).toThrow('no longer accepts jobs')
      })
    })

    describe('when a retry ladder is in flight as the queue is stopped', () => {
      it('should stop re-enqueueing rather than making stop() chase its own retries', async () => {
        const retryingQueue = createJobQueue({ concurrency: 1 })
        let attempts = 0
        const job = retryingQueue.scheduleJobWithRetries(async () => {
          attempts++
          // Each attempt takes a moment, so the ladder is genuinely still in flight when stop() lands.
          // With an instant failure all 50 retries burn through before stop() is even called.
          await sleep(15)
          throw new Error('always fails')
        }, 50)

        // Let the ladder get going, then stop underneath it.
        await sleep(40)
        const attemptsAtStop = attempts
        await retryingQueue.stop!()

        await expect(job).rejects.toThrow()
        // A ladder that kept re-enqueueing would have burned through all 50 attempts.
        expect(attempts).toBeLessThan(50)
        expect(attempts).toBeGreaterThanOrEqual(attemptsAtStop)
      })
    })
  })
})

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

    describe('when no retries are allowed', () => {
      it('should throw indicating that at least one retry is required', () => {
        expect(() => queue.scheduleJobWithRetries(async () => 'unused', 0)).toThrow(
          'At least one retry is required'
        )
      })
    })
  })
})

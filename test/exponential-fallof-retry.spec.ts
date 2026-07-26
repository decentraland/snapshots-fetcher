import { createLogComponent } from '@well-known-components/logger'
import future from 'fp-future'
import { createExponentialFallofRetry } from '../src/exponential-fallof-retry'
import { sleep } from '../src/utils'
import {createConfigComponent} from "@well-known-components/env-config-provider";

describe('createExponentialFallofRetry', () => {
  it('iterates ten times', async () => {
    const config = createConfigComponent({})
    const logs = await createLogComponent({config})
    const logger = logs.getLogger('logger')

    let totalCount = 0

    let finishedFuture = future<void>()

    const component = createExponentialFallofRetry(logger, {
      async action() {
        totalCount++

        if (totalCount == 10) finishedFuture.resolve()
        if (totalCount % 2) throw new Error('Synthetic error')
      },
      retryTime: 10,
      retryTimeExponent: 1.1,
    })

    expect(component.isStopped()).toEqual(true)

    const startPromise = component.start()
    // wait until it is started
    while (component.isStopped()) {
      await sleep(1)
    }

    // once it e
    await finishedFuture
    expect(component.getRetryCount()).toEqual(10)
    expect(totalCount).toEqual(10)
    await component.stop()
    await startPromise
    // stop() lands while the loop is in its retry sleep. It used to wake up and run the action one
    // more time before noticing, because `started` was only re-checked after the action; it is now
    // re-checked straight after the sleep, so the count stays where it was.
    expect(component.getRetryCount()).toEqual(10)
    expect(totalCount).toEqual(10)
  })

  describe('when stop() is called while the component is sleeping between retries', () => {
    let logger: any

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
    })

    it('should stop promptly instead of waiting out the full retry interval', async () => {
      const firstActionRun = future<void>()
      const component = createExponentialFallofRetry(logger, {
        async action() {
          firstActionRun.resolve()
        },
        // Large enough that the test would time out if the sleep were not interrupted by stop().
        retryTime: 1_000_000
      })

      const startPromise = component.start()
      await firstActionRun
      // give the loop a moment to enter the retry sleep
      await sleep(50)
      await component.stop()
      await startPromise

      expect(component.isStopped()).toEqual(true)
    })
  })

  describe('the retry interval', () => {
    let logger: any
    let scheduledIntervals: number[]

    beforeEach(() => {
      scheduledIntervals = []
      // The computed interval is only observable through the "Retrying in Xms" line. Reading it is
      // deterministic, unlike timing the gaps between attempts.
      logger = {
        log: jest.fn(),
        debug: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        info: jest.fn((message: string) => {
          const match = typeof message === 'string' && message.match(/^Retrying in ([\d.]+)ms/)
          if (match) {
            scheduledIntervals.push(Number(match[1]))
          }
        })
      }
    })

    afterEach(() => {
      jest.resetAllMocks()
    })

    describe('when the action fails repeatedly and then completes successfully', () => {
      let component: ReturnType<typeof createExponentialFallofRetry>
      let reachedFourthAttempt: ReturnType<typeof future<void>>

      beforeEach(async () => {
        reachedFourthAttempt = future<void>()
        let attempt = 0

        component = createExponentialFallofRetry(logger, {
          async action() {
            attempt++
            if (attempt === 4) {
              reachedFourthAttempt.resolve()
              // Hold the loop here so the assertions read a settled list of intervals.
              await sleep(10_000)
              return
            }
            throw new Error('synthetic failure')
          },
          retryTime: 20,
          retryTimeExponent: 3,
          maxInterval: 100_000
        })

        void component.start()
        await reachedFourthAttempt
        await component.stop()
      })

      it('should grow while the action keeps failing', () => {
        expect(scheduledIntervals.slice(0, 3)).toEqual([60, 180, 540])
      })
    })

    describe('and a successful run follows the failures', () => {
      let component: ReturnType<typeof createExponentialFallofRetry>
      let reachedFifthAttempt: ReturnType<typeof future<void>>

      beforeEach(async () => {
        reachedFifthAttempt = future<void>()
        let attempt = 0

        component = createExponentialFallofRetry(logger, {
          async action() {
            attempt++
            // fail, fail, succeed, then fail again
            if (attempt === 3) return
            if (attempt >= 5) {
              reachedFifthAttempt.resolve()
              await sleep(10_000)
              return
            }
            throw new Error('synthetic failure')
          },
          retryTime: 20,
          retryTimeExponent: 3,
          maxInterval: 100_000
        })

        void component.start()
        await reachedFifthAttempt
        await component.stop()
      })

      it('should drop back to the base interval instead of staying at the grown one', () => {
        // 60 and 180 from the two failures, then back to 20 after the success, then 60 again.
        expect(scheduledIntervals.slice(0, 4)).toEqual([60, 180, 20, 60])
      })
    })

    describe('and the action fails but had been running for longer than healthyRunTime', () => {
      let component: ReturnType<typeof createExponentialFallofRetry>
      let reachedThirdAttempt: ReturnType<typeof future<void>>

      beforeEach(async () => {
        reachedThirdAttempt = future<void>()
        let attempt = 0

        component = createExponentialFallofRetry(logger, {
          async action() {
            attempt++
            if (attempt >= 3) {
              reachedThirdAttempt.resolve()
              await sleep(10_000)
              return
            }
            // Stays up well past healthyRunTime before failing, like a stream that only ends on error.
            await sleep(60)
            throw new Error('synthetic failure after a healthy run')
          },
          retryTime: 20,
          retryTimeExponent: 3,
          maxInterval: 100_000,
          healthyRunTime: 40
        })

        void component.start()
        await reachedThirdAttempt
        await component.stop()
      })

      it('should keep the base interval, so isolated failures after healthy runs do not compound', () => {
        expect(scheduledIntervals.slice(0, 2)).toEqual([20, 20])
      })
    })
  })

  describe('when start() is called after stop()', () => {
    let logger: any
    let attempts: number

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
      attempts = 0
    })

    it('should refuse to start, rather than run a loop that can never be stopped again', async () => {
      const component = createExponentialFallofRetry(logger, {
        async action() {
          attempts++
        },
        retryTime: 5
      })

      await component.stop()
      // Without a terminal stopped flag this resolves never: `started` is false, so the guard lets the
      // loop in, and its only exit is `if (!started) return` — which start() has just set back to true.
      await component.start()
      await sleep(50)

      expect(attempts).toBe(0)
    })
  })

  describe('when retryTime is zero', () => {
    let logger: any
    let attempts: number

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
      attempts = 0

      const component = createExponentialFallofRetry(logger, {
        async action() {
          attempts++
          throw new Error('synthetic failure')
        },
        retryTime: 0
      })

      await component.start()
    })

    it('should run the action once and stop iterating instead of spinning', () => {
      expect(attempts).toBe(1)
    })
  })

  describe('when maxInterval is negative', () => {
    let logger: any

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
    })

    it('should throw at construction time', () => {
      expect(() =>
        createExponentialFallofRetry(logger, {
          async action() {},
          retryTime: 10,
          maxInterval: -1
        })
      ).toThrow('options.maxInterval must be >= 0')
    })
  })

  describe('when retryTime is negative', () => {
    let logger: any

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
    })

    it('should throw at construction time rather than busy-spinning on zero-length sleeps', () => {
      expect(() =>
        createExponentialFallofRetry(logger, {
          async action() {},
          retryTime: -1
        })
      ).toThrow('options.retryTime must be >= 0')
    })
  })

  describe('when healthyRunTime is negative', () => {
    let logger: any

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
    })

    it('should throw at construction time', () => {
      expect(() =>
        createExponentialFallofRetry(logger, {
          async action() {},
          retryTime: 10,
          healthyRunTime: -1
        })
      ).toThrow('options.healthyRunTime must be >= 0')
    })
  })

  describe('when start() is called while the component is already running', () => {
    let logger: any
    let attempts: number

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
      attempts = 0

      const component = createExponentialFallofRetry(logger, {
        async action() {
          attempts++
          await sleep(50)
        },
        retryTime: 10_000,
        exitOnSuccess: true
      })

      const firstStart = component.start()
      // A second start() must not begin a parallel loop.
      const secondStart = component.start()
      await Promise.all([firstStart, secondStart])
    })

    it('should not start a second loop', () => {
      expect(attempts).toBe(1)
    })
  })

  describe('when the action succeeds and exitOnSuccess is enabled', () => {
    let logger: any

    beforeEach(async () => {
      const config = createConfigComponent({})
      const logs = await createLogComponent({ config })
      logger = logs.getLogger('logger')
    })

    it('should report the component as stopped once the loop exits', async () => {
      const component = createExponentialFallofRetry(logger, {
        async action() {
          // succeeds immediately
        },
        retryTime: 1000,
        exitOnSuccess: true
      })

      await component.start()

      expect(component.isStopped()).toEqual(true)
    })
  })
})

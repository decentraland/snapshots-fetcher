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
    expect(component.getRetryCount()).toEqual(11)
    expect(totalCount).toEqual(11)
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

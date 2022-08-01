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
})

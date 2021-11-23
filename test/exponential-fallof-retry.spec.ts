import { createLogComponent } from '@well-known-components/logger'
import future from 'fp-future'
import { createExponentialFallofRetry } from '../src/exponential-fallof-retry'

describe('createExponentialFallofRetry', () => {
  it('iterates ten times', async () => {
    const logs = createLogComponent().getLogger('logger')

    let totalCount = 0

    let finishedFuture = future<void>()

    const component = createExponentialFallofRetry(logs, {
      async action() {
        totalCount++

        if (totalCount == 10) finishedFuture.resolve()
        if (totalCount % 2) throw new Error('Synthetic error')
      },
      retryTime: 10,
      retryTimeExponent: 1.1,
    })

    expect(component.isStopped()).toEqual(true)

    await component.start()
    expect(component.isStopped()).toEqual(false)
    await finishedFuture
    await component.stop()
    expect(component.getRetryCount()).toEqual(10)
    expect(totalCount).toEqual(10)
  })
})

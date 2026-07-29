import { hashV1 } from '@dcl/hashing'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from '../src/types'
import { sleep } from '../src/utils'
import { test } from './components'

// Ten entities, so the stream is still feeding the deployer when the server is dropped mid-phase.
const snapshotBody = Buffer.from(
  [
    '### Decentraland json snapshot',
    ...Array.from({ length: 10 }, (_unused, index) =>
      JSON.stringify({
        entityId: `ba${String(index).padStart(57, '0')}`,
        entityType: 'profile',
        pointers: ['0x1'],
        entityTimestamp: index + 1,
        authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
      })
    )
  ].join('\n')
)

test('synchronizer when a server is dropped while its snapshot is still deploying', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let scheduled: number
  let snapshotHash: string
  let baseUrl: string

  it('prepares the endpoints', async () => {
    scheduled = 0
    snapshotHash = await hashV1(snapshotBody)
    baseUrl = await components.getBaseUrl()

    components.router.get('/snapshots', async () => ({
      body: [{ hash: snapshotHash, timeRange: { initTimestamp: 0, endTimestamp: 60_000 } }]
    }))
    components.router.get('/contents/:file', async () => ({ body: snapshotBody.toString() }))
    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))
  })

  it('drops the server midway through the snapshot phase', async () => {
    synchronizer = await buildSynchronizer(components, {
      async scheduleEntityDeployment(entity) {
        scheduled++
        // Slow enough that the phase is still running when syncWithServers narrows the desired set.
        await sleep(60)
        if (entity.markAsDeployed) await entity.markAsDeployed()
      },
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    })

    await synchronizer.syncWithServers(new Set([baseUrl]))
    while (scheduled === 0) {
      await sleep(10)
    }

    // The server is no longer wanted. Its snapshot work should stop rather than run the phase out.
    await synchronizer.syncWithServers(new Set())
    const scheduledWhenDropped = scheduled
    await sleep(400)

    // A couple more may land: the stop signal is consulted between entities, not mid-entity.
    expect(scheduled).toBeLessThan(scheduledWhenDropped + 4)
  })

  it('should not have deployed all ten entities of a snapshot nobody wants any more', () => {
    expect(scheduled).toBeLessThan(10)
  })

  afterAll(async () => {
    if (synchronizer?.stop) await synchronizer.stop()
  })
})

function buildSynchronizer(
  components: SnapshotsFetcherComponents,
  deployer: IDeployerComponent
): Promise<SynchronizerComponent> {
  const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
  return createSynchronizer(
    { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage, deployer },
    {
      bootstrapReconnection: { reconnectTime: 60_000 },
      syncingReconnection: { reconnectTime: 60_000 },
      tmpDownloadFolder: resolve('downloads'),
      requestMaxRetries: 1,
      requestRetryWaitTime: 0,
      pointerChangesWaitTime: 0
    }
  )
}

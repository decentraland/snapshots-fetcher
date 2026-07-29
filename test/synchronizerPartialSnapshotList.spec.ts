import { hashV1 } from '@dcl/hashing'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from '../src/types'
import { test } from './components'

const snapshotBody = Buffer.from(
  [
    '### Decentraland json snapshot',
    JSON.stringify({
      entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
      entityType: 'profile',
      pointers: ['0x1'],
      entityTimestamp: 1,
      authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
    })
  ].join('\n')
)

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

test('synchronizer when a server serves one unreadable snapshot entry alongside a valid one', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let pointerChangesRequested: number
  let contentsRequested: number
  let validHash: string

  it('prepares the endpoints', async () => {
    pointerChangesRequested = 0
    contentsRequested = 0
    validHash = await hashV1(snapshotBody)

    components.router.get('/snapshots', async () => ({
      body: [
        // Newer, and perfectly readable: on its own this would advance the server to its endTimestamp.
        { hash: validHash, timeRange: { initTimestamp: 20 * 60_000, endTimestamp: 40 * 60_000 } },
        // Older, and unreadable — a time range nothing in the surviving list covers.
        { hash: '../../etc/passwd', timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 } }
      ]
    }))
    components.router.get(`/contents/${validHash}`, async () => {
      contentsRequested++
      return { body: snapshotBody.toString() }
    })
    components.router.get('/pointer-changes', async () => {
      pointerChangesRequested++
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('attempts the bootstrap', async () => {
    synchronizer = await buildSynchronizer(components, {
      async scheduleEntityDeployment(entity) {
        if (entity.markAsDeployed) await entity.markAsDeployed()
      },
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    })

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))

    const deadline = Date.now() + 10_000
    while (contentsRequested === 0) {
      if (Date.now() > deadline) {
        throw new Error('Timed out waiting for the readable snapshot to be deployed; the path under test never ran')
      }
      await new Promise((ok) => setTimeout(ok, 10))
    }
    await new Promise((ok) => setTimeout(ok, 300))
  })

  it('should still deploy the snapshot it could read', () => {
    expect(contentsRequested).toBeGreaterThan(0)
  })

  it('should not promote the server past the range it could not read', () => {
    expect(pointerChangesRequested).toBe(0)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

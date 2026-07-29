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

test('synchronizer when a server advertises far more replaced hashes than one query should carry', ({ components }) => {
  const CHUNK_SIZE = 1000
  let synchronizer: SynchronizerComponent
  let lookupSizes: number[]
  let contentsRequested: number
  let validHash: string
  let realFilter: (hashes: string[]) => Promise<Set<string>>

  it('prepares the endpoints', async () => {
    lookupSizes = []
    contentsRequested = 0
    validHash = await hashV1(snapshotBody)

    // Ten entries at the per-entry cap of 1000. Each is individually legal, which is the point: the
    // aggregate the synchronizer batches is bounded by nothing but the response size limit.
    const snapshots = Array.from({ length: 10 }, (_unused, entry) => ({
      hash: `ba${String(entry).padStart(57, '0')}`,
      timeRange: { initTimestamp: entry * 60_000, endTimestamp: (entry + 1) * 60_000 },
      replacedSnapshotHashes: Array.from({ length: CHUNK_SIZE }, (_ignored, index) => `ba${entry}${String(index).padStart(56, '0')}`)
    }))

    components.router.get('/snapshots', async () => ({
      body: [
        ...snapshots,
        { hash: validHash, timeRange: { initTimestamp: 20 * 60_000, endTimestamp: 40 * 60_000 } }
      ]
    }))
    components.router.get(`/contents/${validHash}`, async () => {
      contentsRequested++
      return { body: snapshotBody.toString() }
    })
    components.router.get('/contents/:file', async () => ({ body: snapshotBody.toString() }))
    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))

    // Records what each lookup is actually asked for. Chunking that exists in a helper but is bypassed at
    // the call site would leave this holding one 10,001-hash entry.
    //
    // A plain wrapper rather than jest.spyOn: the test runner calls jest.resetAllMocks() before every step
    // and jest.restoreAllMocks() after, so a spy installed in this step is gone by the time the next one
    // runs the bootstrap. That silently produced an empty recording and an assertion that passed on
    // Math.max() of nothing.
    realFilter = components.processedSnapshotStorage.filterProcessedSnapshotsFrom.bind(
      components.processedSnapshotStorage
    )
    components.processedSnapshotStorage.filterProcessedSnapshotsFrom = async (hashes: string[]) => {
      lookupSizes.push(hashes.length)
      return realFilter(hashes)
    }
  })

  it('bootstraps the server', async () => {
    synchronizer = await buildSynchronizer(components, {
      async scheduleEntityDeployment(entity) {
        if (entity.markAsDeployed) await entity.markAsDeployed()
      },
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    })

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))

    const deadline = Date.now() + 20_000
    while (contentsRequested === 0) {
      if (Date.now() > deadline) {
        throw new Error('the snapshot was never downloaded')
      }
      await new Promise((ok) => setTimeout(ok, 20))
    }
  })

  it('should have looked up more than 10,000 hashes in total', () => {
    // Guards the test itself: if the fixture stopped producing a large aggregate, the assertion below
    // would pass trivially.
    expect(lookupSizes.reduce((total, size) => total + size, 0)).toBeGreaterThan(10_000)
  })

  it('should never have asked the storage for more than one chunk at a time', () => {
    expect(Math.max(...lookupSizes)).toBeLessThanOrEqual(CHUNK_SIZE)
  })

  afterAll(async () => {
    components.processedSnapshotStorage.filterProcessedSnapshotsFrom = realFilter
    if (synchronizer?.stop) {
      await synchronizer.stop()
    }
  })
})

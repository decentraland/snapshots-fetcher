import { hashV1 } from '@dcl/hashing'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from '../src/types'
import { test } from './components'

const snapshotEndTimestamp = 20 * 60_000 + 5000

// One valid deployment, then a truncated line. The valid entity deploys; the truncated line is an entity
// that never will, which is what must stop the server advancing past this snapshot's time range.
const partlyUnusableSnapshot = Buffer.from(
  [
    '### Decentraland json snapshot',
    JSON.stringify({
      entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
      entityType: 'profile',
      pointers: ['0x1'],
      entityTimestamp: 1,
      authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
    }),
    '{"entityType":"pro'
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

test('synchronizer when a server serves a snapshot with unreadable lines', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let pointerChangesRequested: number
  let markedAsProcessed: string[]
  let unusableSnapshotHash: string
  let contentsRequested: number

  it('prepares the endpoints', async () => {
    pointerChangesRequested = 0
    markedAsProcessed = []
    contentsRequested = 0
    // The real content hash, so the download passes verification and the snapshot actually reaches the
    // parser. A fabricated hash would fail hash verification first and mask what is under test.
    unusableSnapshotHash = await hashV1(partlyUnusableSnapshot)

    components.router.get('/snapshots', async () => ({
      body: [{ hash: unusableSnapshotHash, timeRange: { initTimestamp: 0, endTimestamp: snapshotEndTimestamp } }]
    }))
    components.router.get(`/contents/${unusableSnapshotHash}`, async () => {
      contentsRequested++
      return { body: partlyUnusableSnapshot.toString() }
    })
    components.router.get('/pointer-changes', async () => {
      pointerChangesRequested++
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('attempts the bootstrap', async () => {
    jest
      .spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      .mockImplementation(async (hash: string) => {
        markedAsProcessed.push(hash)
      })

    synchronizer = await buildSynchronizer(components, {
      async scheduleEntityDeployment(entity) {
        if (entity.markAsDeployed) await entity.markAsDeployed()
      },
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    })

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))

    // The bootstrap cannot finish — the server stays in snapshot bootstrap and the job keeps retrying —
    // so completion is not the signal to wait for. Wait for the snapshot to have actually been fetched
    // and parsed, and fail if that never happens: a race against a timeout that ignores which side won
    // would let the assertions below pass vacuously if this code path were never reached.
    const deadline = Date.now() + 10_000
    while (contentsRequested === 0) {
      if (Date.now() > deadline) {
        throw new Error('Timed out waiting for the unusable snapshot to be requested; the path under test never ran')
      }
      await new Promise((ok) => setTimeout(ok, 10))
    }
  })

  it('should not mark the unusable snapshot as processed', () => {
    expect(markedAsProcessed).not.toContain(unusableSnapshotHash)
  })

  it('should not promote the server to pointer-changes, which would skip the undeployed entities', () => {
    expect(pointerChangesRequested).toBe(0)
  })

  afterAll(async () => {
    jest.restoreAllMocks()
    await synchronizer.stop!()
  })
})

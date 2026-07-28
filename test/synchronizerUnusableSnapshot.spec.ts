import { hashV1 } from '@dcl/hashing'
import future from 'fp-future'
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

  it('prepares the endpoints', async () => {
    pointerChangesRequested = 0
    markedAsProcessed = []
    // The real content hash, so the download passes verification and the snapshot actually reaches the
    // parser. A fabricated hash would fail hash verification first and mask what is under test.
    unusableSnapshotHash = await hashV1(partlyUnusableSnapshot)

    components.router.get('/snapshots', async () => ({
      body: [{ hash: unusableSnapshotHash, timeRange: { initTimestamp: 0, endTimestamp: snapshotEndTimestamp } }]
    }))
    components.router.get(`/contents/${unusableSnapshotHash}`, async () => ({
      body: partlyUnusableSnapshot.toString()
    }))
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

    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    // The bootstrap cannot finish: the server stays in snapshot bootstrap and the sync job keeps
    // retrying, so wait for the first attempt to have been made rather than for completion.
    const firstAttemptSettled = future<void>()
    void syncJob.onInitialBootstrapFinished(async () => firstAttemptSettled.resolve())
    await Promise.race([firstAttemptSettled, new Promise((ok) => setTimeout(ok, 1500))])
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

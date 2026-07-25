import future from 'fp-future'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { SynchronizerComponent } from '../src/types'
import { test } from './components'

test('synchronizer when a snapshot advertised by a server cannot be deployed', ({ components }) => {
  const contentFolder = resolve('downloads')
  // A hash that is deliberately absent from test/fixtures, so it is not preloaded in the in-memory
  // storage and the download is actually attempted (and fails) instead of short-circuiting.
  const undownloadableSnapshot = 'bafkreisnapshotthatisnevergoingtobeserved0000000000000000000000'
  const snapshotEndTimestamp = 20 * 60_000 + 1

  let synchronizer: SynchronizerComponent
  let pointerChangesRequests: string[]

  it('prepares the endpoints', () => {
    pointerChangesRequests = []

    components.router.get('/snapshots', async () => ({
      body: [
        {
          hash: undownloadableSnapshot,
          timeRange: { initTimestamp: 0, endTimestamp: snapshotEndTimestamp },
          replacedSnapshotHashes: []
        }
      ]
    }))

    // The snapshot file is never served, so deployEntitiesFromSnapshot fails for this server.
    components.router.get(`/contents/${undownloadableSnapshot}`, async () => ({
      status: 500,
      body: 'the snapshot file is unavailable'
    }))

    components.router.get('/pointer-changes', async (ctx) => {
      pointerChangesRequests.push(ctx.url.searchParams.get('from') ?? '')
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('attempts the bootstrap', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        processedSnapshotStorage,
        snapshotStorage,
        metrics,
        deployer: {
          scheduleEntityDeployment: jest.fn(),
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      {
        // High enough that the failed bootstrap is not retried within the test.
        bootstrapReconnection: { reconnectTime: 60_000 },
        syncingReconnection: { reconnectTime: 60_000 },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 1,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 0
      }
    )

    const bootstrapTryFinished = future<void>()
    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await syncJob.onInitialBootstrapFinished(async () => {
      bootstrapTryFinished.resolve()
    })
    await bootstrapTryFinished
    await synchronizer.stop!()
  })

  it('should not start the pointer-changes stream for the server that failed', () => {
    expect(pointerChangesRequests).toEqual([])
  })

  it('should leave the snapshot unmarked so a later bootstrap retries it', async () => {
    const processed = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([undownloadableSnapshot])

    expect(processed.has(undownloadableSnapshot)).toBe(false)
  })
})

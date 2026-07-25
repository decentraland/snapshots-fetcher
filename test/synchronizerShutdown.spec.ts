import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { SynchronizerComponent, SyncJob } from '../src/types'
import { test } from './components'

test('synchronizer when it is stopped before the bootstrap could finish', ({ components }) => {
  const contentFolder = resolve('downloads')

  async function createStoppableSynchronizer(): Promise<SynchronizerComponent> {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    return createSynchronizer(
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
        bootstrapReconnection: { reconnectTime: 60_000 },
        syncingReconnection: { reconnectTime: 60_000 },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 1,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 0
      }
    )
  }

  it('prepares the endpoints', () => {
    // A server that never serves its snapshots keeps the job in the bootstrapping state, so the
    // sync-finished future stays pending until the synchronizer is stopped.
    components.router.get('/snapshots', async () => ({ status: 500, body: 'no snapshots for you' }))
  })

  describe('when the sync job is the one currently running', () => {
    let synchronizer: SynchronizerComponent
    let syncJob: SyncJob

    beforeEach(async () => {
      synchronizer = await createStoppableSynchronizer()
      syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
      await synchronizer.stop!()
    })

    it('should reject onSyncFinished() instead of leaving the caller waiting forever', async () => {
      await expect(syncJob.onSyncFinished()).rejects.toThrow('stopped before it finished')
    })
  })

  describe('and another sync job is still queued behind it', () => {
    let synchronizer: SynchronizerComponent
    let queuedSyncJob: SyncJob

    beforeEach(async () => {
      synchronizer = await createStoppableSynchronizer()
      const baseUrl = await components.getBaseUrl()
      await synchronizer.syncWithServers(new Set([baseUrl]))
      // The serial runner keeps this second job queued while the first one is still bootstrapping.
      queuedSyncJob = await synchronizer.syncWithServers(new Set([baseUrl]))
      await synchronizer.stop!()
    })

    it('should reject onSyncFinished() for the queued job that never started', async () => {
      await expect(queuedSyncJob.onSyncFinished()).rejects.toThrow('stopped before it finished')
    })
  })
})

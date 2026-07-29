import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { SynchronizerComponent, SyncJob } from '../src/types'
import { test } from './components'

test('synchronizer when syncWithServers is called repeatedly while a job is running', ({ components }) => {
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
    // A server that never serves its snapshots keeps the first job bootstrapping, so everything after
    // it stays queued — the situation a periodic DAO refresh used to pile jobs up in.
    components.router.get('/snapshots', async () => ({ status: 500, body: 'no snapshots for you' }))
  })

  describe('when a job is already running and several more calls arrive', () => {
    let synchronizer: SynchronizerComponent
    let runningJob: SyncJob
    let firstQueuedJob: SyncJob
    let laterJobs: SyncJob[]

    beforeEach(async () => {
      synchronizer = await createStoppableSynchronizer()
      const baseUrl = await components.getBaseUrl()
      // The runner starts this one immediately, so it is the running job, not a queued one.
      runningJob = await synchronizer.syncWithServers(new Set([baseUrl]))
      firstQueuedJob = await synchronizer.syncWithServers(new Set([baseUrl]))
      laterJobs = []
      for (let call = 0; call < 5; call++) {
        laterJobs.push(await synchronizer.syncWithServers(new Set([baseUrl])))
      }
    })

    afterEach(async () => {
      await synchronizer.stop!()
    })

    it('should queue a job behind the running one rather than reusing the running job', () => {
      expect(firstQueuedJob).not.toBe(runningJob)
    })

    it('should hand every later caller the already-queued job instead of stacking more', () => {
      expect(laterJobs.every((job) => job === firstQueuedJob)).toEqual(true)
    })
  })

  describe('and the desired server set changes on a call that coalesces', () => {
    let synchronizer: SynchronizerComponent
    let firstQueuedJob: SyncJob
    let coalescedJob: SyncJob

    beforeEach(async () => {
      synchronizer = await createStoppableSynchronizer()
      const baseUrl = await components.getBaseUrl()
      await synchronizer.syncWithServers(new Set([baseUrl]))
      firstQueuedJob = await synchronizer.syncWithServers(new Set([baseUrl]))
      // The queued job reads the live desired-server state when it starts, so a changed set does not
      // need a job of its own.
      coalescedJob = await synchronizer.syncWithServers(new Set([baseUrl, 'http://another-server.com']))
    })

    afterEach(async () => {
      await synchronizer.stop!()
    })

    it('should still reuse the queued job', () => {
      expect(coalescedJob).toBe(firstQueuedJob)
    })
  })
})

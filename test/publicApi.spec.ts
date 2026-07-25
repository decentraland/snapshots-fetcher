import { createInMemoryStorage } from '@dcl/catalyst-storage'
import { createTestMetricsComponent } from '@dcl/metrics'
import { createLogComponent } from '@well-known-components/logger'
import { createConfigComponent } from '@well-known-components/env-config-provider'
// Everything below must be reachable from the package root: this is exactly what a consumer can
// import. A deep import into dist/ would compile here but breaks the published API contract.
import {
  createJobQueue,
  createSynchronizer,
  decideSnapshotDeploymentFromProcessedSet,
  downloadEntityAndContentFiles,
  shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded,
  getDeployedEntitiesStreamFromPointerChanges,
  getDeployedEntitiesStreamFromSnapshot,
  metricsDefinitions,
  IDeployerComponent,
  IDownloadQueue,
  IProcessedSnapshotStorageComponent,
  ISnapshotStorageComponent,
  SnapshotsFetcherComponents,
  SynchronizerComponent,
  SynchronizerOptions
} from '../src'

describe('the package root export', () => {
  describe('when a consumer assembles the components createSynchronizer requires', () => {
    let components: SnapshotsFetcherComponents & { deployer: IDeployerComponent }
    let options: SynchronizerOptions

    beforeEach(async () => {
      const config = createConfigComponent({ LOG_LEVEL: 'ERROR' })
      const downloadQueue: IDownloadQueue = createJobQueue({ autoStart: true, concurrency: 1 })
      const processedSnapshotStorage: IProcessedSnapshotStorageComponent = {
        filterProcessedSnapshotsFrom: jest.fn().mockResolvedValue(new Set<string>()),
        markSnapshotAsProcessed: jest.fn()
      }
      const snapshotStorage: ISnapshotStorageComponent = { has: jest.fn().mockResolvedValue(false) }

      components = {
        metrics: createTestMetricsComponent(metricsDefinitions),
        fetcher: { fetch: jest.fn() },
        downloadQueue,
        logs: await createLogComponent({ config }),
        storage: createInMemoryStorage(),
        processedSnapshotStorage,
        snapshotStorage,
        deployer: {
          scheduleEntityDeployment: jest.fn(),
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      }

      options = {
        bootstrapReconnection: { reconnectTime: 1000 },
        syncingReconnection: { reconnectTime: 1000 },
        tmpDownloadFolder: 'downloads',
        requestMaxRetries: 1,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 0
      }
    })

    afterEach(() => {
      jest.resetAllMocks()
    })

    it('should build a synchronizer without any deep import into dist', async () => {
      const synchronizer: SynchronizerComponent = await createSynchronizer(components, options)

      await synchronizer.stop!()

      expect(synchronizer.syncWithServers).toBeInstanceOf(Function)
    })

    it('should expose the entity and stream helpers', () => {
      expect([
        downloadEntityAndContentFiles,
        getDeployedEntitiesStreamFromSnapshot,
        getDeployedEntitiesStreamFromPointerChanges
      ]).toEqual([expect.any(Function), expect.any(Function), expect.any(Function)])
    })

    it('should expose the snapshot-deployment decision helpers', () => {
      expect([decideSnapshotDeploymentFromProcessedSet, shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded]).toEqual(
        [expect.any(Function), expect.any(Function)]
      )
    })

    describe('and requestMaxRetries is zero', () => {
      it('should reject up front instead of failing every snapshot request silently', async () => {
        await expect(createSynchronizer(components, { ...options, requestMaxRetries: 0 })).rejects.toThrow(
          'options.requestMaxRetries must be an integer >= 1'
        )
      })
    })

    describe('and requestMaxRetries is not an integer', () => {
      it('should reject up front naming the offending value', async () => {
        await expect(createSynchronizer(components, { ...options, requestMaxRetries: 1.5 })).rejects.toThrow(
          'got 1.5'
        )
      })
    })
  })
})

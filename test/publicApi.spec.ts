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
import * as packageRoot from '../src'

describe('the package root export', () => {
  describe('when enumerating what the package publishes', () => {
    // The API guard for this repo. `make build` used to invoke api-extractor, but it was never declared or
    // installed and no config or report file ever existed, so that step always failed — the check never ran
    // on any branch, and removing it fixed a broken target rather than dropping a safeguard. Reinstating it
    // properly is not straightforward either: CI delegates to a shared reusable workflow this repository
    // cannot add steps to, so the enforceable place for an API check is the suite CI already runs.
    //
    // Only value exports appear here — types are erased at runtime, so a type-level surface change still
    // needs review. Adding or removing a runtime export should be a deliberate edit to this list.
    const published = [
      'DEFAULT_TRANSFER_LIMITS',
      'createJobQueue',
      'createSynchronizer',
      'decideSnapshotDeploymentFromProcessedSet',
      'downloadEntityAndContentFiles',
      'getDeployedEntitiesStreamFromPointerChanges',
      'getDeployedEntitiesStreamFromSnapshot',
      'metricsDefinitions',
      'resolveTransferLimits',
      'shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded'
    ]

    it('should export exactly the documented set of values, no more and no less', () => {
      expect(Object.keys(packageRoot).sort()).toEqual(published)
    })
  })

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

    describe.each([
      ['bootstrapReconnection', 'reconnectTime', Number.NaN, 'finite number >= 0'],
      ['bootstrapReconnection', 'reconnectRetryTimeExponent', 0, 'finite number >= 1'],
      ['bootstrapReconnection', 'maxReconnectionTime', Number.POSITIVE_INFINITY, 'finite number >= 0'],
      ['syncingReconnection', 'reconnectTime', -1, 'finite number >= 0'],
      ['syncingReconnection', 'reconnectRetryTimeExponent', Number.NaN, 'finite number >= 1'],
      ['syncingReconnection', 'maxReconnectionTime', -1, 'finite number >= 0']
    ])(
      'and %s.%s is invalid',
      (group: string, field: string, value: number, expectedRequirement: string) => {
        let invalidOptions: SynchronizerOptions

        beforeEach(() => {
          invalidOptions = {
            ...options,
            [group]: {
              ...(options as any)[group],
              [field]: value
            }
          } as SynchronizerOptions
        })

        afterEach(() => {
          invalidOptions = undefined as any
        })

        it('should reject it during construction instead of starting a broken retry job later', async () => {
          await expect(createSynchronizer(components, invalidOptions)).rejects.toThrow(
            `options.${group}.${field} must be a ${expectedRequirement}`
          )
        })
      }
    )
  })
})

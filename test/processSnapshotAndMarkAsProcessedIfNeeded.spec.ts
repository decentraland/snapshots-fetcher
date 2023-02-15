import { IDeployerComponent } from '../src/types'
import { processSnapshotAndMarkAsProcessedIfNeeded } from '../src/synchronizer'
import * as deployEntities from '../src/deploy-entities'
import { test, TestComponents } from './components'

describe('processSnapshotAndMarkAsProcessedIfNeeded', () => {

  const processedSnapshotHash = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'

  let deployEntitiesFromSnapshotSpy
  const streamOptions = {
    requestRetryWaitTime: 1,
    requestMaxRetries: 1,
    tmpDownloadFolder: 'mock'
  }
  const genesisTimestamp = 0

  beforeEach(() => {
    jest.restoreAllMocks()
    deployEntitiesFromSnapshotSpy = jest.spyOn(deployEntities, 'deployEntitiesFromSnapshot')
  })

  test('should not process an already processed snapshot', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(processedSnapshotHash)
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: genesisTimestamp + 1,
          replacedSnapshotHashes: [],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).not.toBeCalled()
    })
  })

  test('should not process snapshot with greatest timestamp smaller or equal than genesis timestamp', ({ components }) => {
    it('run test', async () => {
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 0,
          replacedSnapshotHashes: [],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).not.toBeCalled()
    })
  })

  test('should process snapshot with greatest timestamp bigger than genesis timestamp', ({ components }) => {
    it('run test', async () => {
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 1,
          replacedSnapshotHashes: [],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).toBeCalled()
    })
  })

  test('should not process own snapshot', ({ components }) => {
    it('run test', async () => {
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      jest.spyOn(components.snapshotStorage, 'has').mockImplementation(async (hash) => {
        return hash === processedSnapshotHash
      })
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 1,
          replacedSnapshotHashes: [],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).not.toBeCalled()
    })
  })

  test('should not process snapshot when the replaced hashes of some group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, {
          deployEntity: jest.fn(),
          onIdle: jest.fn()
        }),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 1,
          replacedSnapshotHashes: [[h1, h2, h3]],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).not.toBeCalled()
    })
  })

  test('should save the snapshot as processed when all the hashes of some replaced-hashes group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 1,
          replacedSnapshotHashes: [[h1, h2, h3], ['non-processed']],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).not.toBeCalled()
      const processed = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([processedSnapshotHash])
      expect(processed.has(processedSnapshotHash)).toBeTruthy()
    })
  })

  test('should process snapshot when not all the replaced hashes of any group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await processSnapshotAndMarkAsProcessedIfNeeded(
        componentsWithDeployer(components, deployerMock),
        {
          snapshotHash: processedSnapshotHash,
          greatestEndTimestamp: 1,
          replacedSnapshotHashes: [[h1, h2, 'non-processed'], [h3, 'non-processed']],
          servers: new Set([])
        },
        streamOptions,
        0,
        () => false
      )
      expect(deployEntitiesFromSnapshotSpy).toBeCalled()
      const processed = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([processedSnapshotHash, 'non-processed'])
      expect(processed.size).toEqual(0)
    })
  })
})

function componentsWithDeployer(components: TestComponents, deployer: IDeployerComponent):
  Parameters<typeof processSnapshotAndMarkAsProcessedIfNeeded>[0]
  & { deployer: IDeployerComponent } {
  return {
    metrics: components.metrics,
    logs: components.logs,
    processedSnapshotStorage: components.processedSnapshotStorage,
    snapshotStorage: components.snapshotStorage,
    storage: components.storage,
    deployer
  }
}

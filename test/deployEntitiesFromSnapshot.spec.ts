import { resolve } from 'path'
import { deployEntitiesFromSnapshot } from '../src/deploy-entities'
import * as streamEntities from '../src/stream-entities'
import { DeployableEntity, IDeployerComponent } from '../src/types'
import { TestComponents, test } from './components'

describe('deployEntitiesFromSnapshot', () => {

  const contentFolder = resolve('downloads')
  const snapshotHash = 'hash'
  const servers = ['http://aServer.com']
  const streamOptions = {
    requestRetryWaitTime: 0,
    requestMaxRetries: 10,
    tmpDownloadFolder: contentFolder
  }
  const deployerMock = {
    scheduleEntityDeployment: jest.fn(),
    onIdle: jest.fn(),
    prepareForDeploymentsIn: jest.fn()
  }

  beforeEach(() => {
    jest.restoreAllMocks()
  })

  test('when the snapshot is empty', ({ components }) => {
    it('does not stream entities but saves the snapshot as processed', async () => {
      mockDeployedEntitiesStreamWith([])
      const markSnapshotAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(servers),
        () => false
      )
      expect(deployerMock.scheduleEntityDeployment).not.toBeCalled()
      expect(markSnapshotAsProcessedSpy).toBeCalledWith(snapshotHash)
    })
  })

  test('when the snapshot is not empty', ({ components }) => {
    it('streams and deployes its entities', async () => {
      const entity1 = { entityId: 'id1', entityType: 't1', pointers: ['p1'], entityTimestamp: 0, authChain: [], snapshotHash, servers }
      const entity2 = { entityId: 'id2', entityType: 't2', pointers: ['p2'], entityTimestamp: 1, authChain: [], snapshotHash, servers }
      mockDeployedEntitiesStreamWith([entity1, entity2])
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')

      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(),
        () => false
      )
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(2)
      expect(scheduleEntityDeploymentSpy).toBeCalledWith(expect.objectContaining({
        ...entity1,
        snapshotHash
      }), servers)
      expect(scheduleEntityDeploymentSpy).toBeCalledWith(expect.objectContaining({
        ...entity2,
        snapshotHash
      }), servers)
    })
  })

  test('when the snapshot is not empty and the deployer is sync', ({ components }) => {
    it('streams its entities and saves the snapshot as processed after the entities are deployed', async () => {
      mockDeployedEntitiesStreamWith([
        { entityId: 'id1', entityType: 't1', pointers: ['p1'], entityTimestamp: 0, authChain: [], snapshotHash, servers },
        { entityId: 'id2', entityType: 't2', pointers: ['p2'], entityTimestamp: 1, authChain: [], snapshotHash, servers }
      ])
      const deployerMock = {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          if (entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      }
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')
      const markSnapshotAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')

      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(),
        () => false
      )
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(2)
      expect(markSnapshotAsProcessedSpy).toBeCalledWith(snapshotHash)
    })
  })

  test('when the snapshot is not empty and the deployer is async', ({ components }) => {
    it('streams its entities and saves the snapshot as processed after the entities are deployed', async () => {
      mockDeployedEntitiesStreamWith([
        { entityId: 'id1', entityType: 't1', pointers: ['p1'], entityTimestamp: 0, authChain: [], snapshotHash, servers },
        { entityId: 'id2', entityType: 't2', pointers: ['p2'], entityTimestamp: 1, authChain: [], snapshotHash, servers },
      ])

      const entitiesToDeploy = []
      const deployerMock = {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          if (entity.markAsDeployed) {
            entitiesToDeploy.push(entity.markAsDeployed)
          }
        },
        async onIdle() {
          await Promise.all(entitiesToDeploy.map(mark => mark()))
        },
        prepareForDeploymentsIn: jest.fn()
      }
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')
      const markSnapshotAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')

      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(),
        () => false
      )
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(2)
      expect(markSnapshotAsProcessedSpy).not.toBeCalledWith(snapshotHash)
      await deployerMock.onIdle()
      expect(markSnapshotAsProcessedSpy).toBeCalledWith(snapshotHash)
    })
  })

  test('when the snapshot is not empty', ({ components }) => {
    it('streams its entities and does not save the snapshot as processed before ALL the entities are deployed', async () => {
      mockDeployedEntitiesStreamWith([
        { entityId: 'id1', entityType: 't1', pointers: ['p1'], entityTimestamp: 0, authChain: [], snapshotHash, servers },
        { entityId: 'id2', entityType: 't2', pointers: ['p2'], entityTimestamp: 1, authChain: [], snapshotHash, servers },
      ])
      const deployerMock = {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          // only entity 1 is deployed
          if (entity.entityId == 'id1' && entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      }
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')
      const markSnapshotAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')

      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(),
        () => false
      )
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(2)
      expect(markSnapshotAsProcessedSpy).not.toBeCalledWith(snapshotHash)
    })
  })

  test('when the stream is cancelled', ({ components }) => {
    it('stops the deployments of the entities', async () => {
      mockDeployedEntitiesStreamWith([
        { entityId: 'id1', entityType: 't1', pointers: ['p1'], entityTimestamp: 0, authChain: [], snapshotHash, servers },
        { entityId: 'id2', entityType: 't2', pointers: ['p2'], entityTimestamp: 1, authChain: [], snapshotHash, servers },
      ])
      const deployerMock = {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          if (entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      }
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')
      const markSnapshotAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      let numberOfStreamedEntities = 0
      const shouldStopStream = () => {
        if (numberOfStreamedEntities == 1) {
          return true
        }
        numberOfStreamedEntities++
        return false
      }
      await deployEntitiesFromSnapshot(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        snapshotHash,
        new Set(),
        shouldStopStream
      )
      // only first entity is streamed
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(1)
      expect(markSnapshotAsProcessedSpy).not.toBeCalledWith(snapshotHash)
    })
  })
})

function mockDeployedEntitiesStreamWith(entities: any[]) {
  return jest.spyOn(streamEntities, 'getDeployedEntitiesStreamFromSnapshot')
    .mockImplementation(async function* gen() {
      for (const entity of entities) {
        yield entity
      }
      return
    })
}

function componentsWithDeployer(components: TestComponents, deployer: IDeployerComponent):
  Parameters<typeof deployEntitiesFromSnapshot>[0]
  & { deployer: IDeployerComponent } {
  return {
    metrics: components.metrics,
    logs: components.logs,
    storage: components.storage,
    snapshotStorage: components.snapshotStorage,
    processedSnapshotStorage: components.processedSnapshotStorage,
    deployer
  }
}

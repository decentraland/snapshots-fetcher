import { DeployableEntity, IDeployerComponent } from '../src/types'
import { deployEntitiesFromPointerChanges } from '../src/deploy-entities'
import * as streamEntities from '../src/stream-entities'
import { test, TestComponents } from './components'

describe('deployEntitiesFromPointerChanges', () => {

  const snapshotHash = 'hash'
  const server = 'http://aServer.com'
  const streamOptions = {
    pointerChangesWaitTime: 0,
    fromTimestamp: 0
  }

  beforeEach(() => {
    jest.restoreAllMocks()
  })

  test('when the stream is empty', ({ components }) => {
    it('does not deploy anything', async () => {
      mockDeployedEntitiesStreamWith([])
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        () => false,
        () => { }
      )
      expect(deployerMock.deployEntity).not.toBeCalled()
    })
  })

  test('when the stream is not empty', ({ components }) => {
    it('streams and deployes its entities', async () => {
      const entity1 = { entityId: 'id1', entityType: 't1', pointers: ['p1'], localTimestamp: 0, authChain: [] }
      const entity2 = { entityId: 'id2', entityType: 't2', pointers: ['p2'], localTimestamp: 1, authChain: [] }
      mockDeployedEntitiesStreamWith([entity1, entity2])
      const deployerMock = {
        deployEntity: jest.fn(),
        onIdle: jest.fn()
      }
      const deployEntitySpy = jest.spyOn(deployerMock, 'deployEntity')

      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        () => false,
        () => { }
      )
      expect(deployEntitySpy).toBeCalledTimes(2)
      expect(deployEntitySpy).toBeCalledWith(expect.objectContaining({
        ...entity1
      }), [server])
      expect(deployEntitySpy).toBeCalledWith(expect.objectContaining({
        ...entity2
      }), [server])
    })
  })

  test('when the stream is cancelled', ({ components }) => {
    it('stops the deployments of the entities', async () => {
      mockDeployedEntitiesStreamWith([
        { entityId: 'id1', entityType: 't1', pointers: ['p1'], localTimestamp: 0, authChain: [] },
        { entityId: 'id2', entityType: 't2', pointers: ['p2'], localTimestamp: 1, authChain: [] },
      ])
      const deployerMock = {
        async deployEntity(entity: DeployableEntity) {
          if (entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        onIdle: jest.fn()
      }
      const deployEntitySpy = jest.spyOn(deployerMock, 'deployEntity')
      const saveAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'saveAsProcessed')
      let numberOfStreamedEntities = 0
      const shouldStopStream = () => {
        if (numberOfStreamedEntities == 1) {
          return true
        }
        numberOfStreamedEntities++
        return false
      }
      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        shouldStopStream,
        () => { }
      )
      // only first entity is streamed
      expect(deployEntitySpy).toBeCalledTimes(1)
      expect(saveAsProcessedSpy).not.toBeCalledWith(snapshotHash)
    })
  })
})

function mockDeployedEntitiesStreamWith(entities: any[]) {
  return jest.spyOn(streamEntities, 'getDeployedEntitiesStreamFromPointerChanges')
    .mockImplementation(async function* gen() {
      for (const entity of entities) {
        yield entity
      }
      return
    })
}

function componentsWithDeployer(components: TestComponents, deployer: IDeployerComponent):
  Parameters<typeof deployEntitiesFromPointerChanges>[0]
  & { deployer: IDeployerComponent } {
  return {
    metrics: components.metrics,
    logs: components.logs,
    fetcher: components.fetcher,
    deployer
  }
}

import { deployEntitiesFromPointerChanges } from '../src/deploy-entities'
import * as streamEntities from '../src/stream-entities'
import { DeployableEntity, IDeployerComponent } from '../src/types'
import { TestComponents, test } from './components'

describe('deployEntitiesFromPointerChanges', () => {

  const snapshotHash = 'hash'
  const server = 'http://aServer.com'
  const streamOptions = {
    pointerChangesWaitTime: 0,
    fromTimestamp: 0
  }
  const deployerMock = {
    scheduleEntityDeployment: jest.fn(),
    onIdle: jest.fn(),
    prepareForDeploymentsIn: jest.fn()
  }

  beforeEach(() => {
    jest.restoreAllMocks()
  })

  test('when the stream is empty', ({ components }) => {
    it('does not deploy anything', async () => {
      mockDeployedEntitiesStreamWith([])
      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        () => false,
        () => { }
      )
      expect(deployerMock.scheduleEntityDeployment).not.toBeCalled()
    })
  })

  test('when the stream is not empty', ({ components }) => {
    it('streams and deployes its entities', async () => {
      const entity1 = { entityId: 'id1', entityType: 't1', pointers: ['p1'], localTimestamp: 0, authChain: [] }
      const entity2 = { entityId: 'id2', entityType: 't2', pointers: ['p2'], localTimestamp: 1, authChain: [] }
      mockDeployedEntitiesStreamWith([entity1, entity2])
      const scheduleEntityDeploymentSpy = jest.spyOn(deployerMock, 'scheduleEntityDeployment')

      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        () => false,
        () => { }
      )
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(2)
      expect(scheduleEntityDeploymentSpy).toBeCalledWith(expect.objectContaining({
        ...entity1
      }), [server])
      expect(scheduleEntityDeploymentSpy).toBeCalledWith(expect.objectContaining({
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
      await deployEntitiesFromPointerChanges(
        componentsWithDeployer(components, deployerMock),
        streamOptions,
        server,
        shouldStopStream,
        () => { }
      )
      // only first entity is streamed
      expect(scheduleEntityDeploymentSpy).toBeCalledTimes(1)
      expect(markSnapshotAsProcessedSpy).not.toBeCalledWith(snapshotHash)
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

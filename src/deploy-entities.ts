import { getDeployedEntitiesStreamFromPointerChanges, getDeployedEntitiesStreamFromSnapshot } from '.'
import {
  IDeployerComponent,
  PointerChangesDeployedEntityStreamOptions,
  SnapshotDeployedEntityStreamOptions,
  SnapshotsFetcherComponents
} from './types'

/**
 * This function streams and deploys the entities of pointer-changes of a server. It calls 'increaseLastTimestamp'
 * for each entity deployed.
 * @public
 */
export async function deployEntitiesFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'metrics' | 'fetcher'> & {
    deployer: IDeployerComponent
  },
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string,
  shouldStopStream: () => boolean,
  increaseLastTimestamp: (contentServer: string, ...newTimestamps: number[]) => void
) {
  const logger = components.logs.getLogger('deployEntitiesFromPointerChanges')
  const deployments = getDeployedEntitiesStreamFromPointerChanges(components, options, contentServer)

  for await (const deployment of deployments) {
    // if the stream is closed then we should not process more deployments
    if (shouldStopStream()) {
      logger.debug('Canceling running stream')
      return
    }

    await components.deployer.deployEntity(
      {
        ...deployment,
        markAsDeployed: async function () {
          components.metrics.increment('dcl_entities_deployments_processed_total', { source: 'pointer-changes' })
          // update greatest processed timestamp
          increaseLastTimestamp(contentServer, deployment.localTimestamp)
        }
      },
      [contentServer]
    )
  }
}

/**
 * This function streams and deploys the entities of a snapshot. When the deployer marks all the entities as deployed,
 * it saves the snapshot as processed.
 * @public
 */
export async function deployEntitiesFromSnapshot(
  components: Pick<
    SnapshotsFetcherComponents,
    'metrics' | 'logs' | 'storage' | 'processedSnapshotStorage' | 'snapshotStorage'
  > & {
    deployer: IDeployerComponent
  },
  options: SnapshotDeployedEntityStreamOptions,
  snapshotHash: string,
  servers: Set<string>,
  shouldStopStream: () => boolean
) {
  const logger = components.logs.getLogger('deployEntitiesFromSnapshot')
  const stream = getDeployedEntitiesStreamFromSnapshot(components, options, snapshotHash, servers)
  let snapshotWasCompletelyStreamed = false
  let numberOfStreamedEntities = 0
  let numberOfProcessedEntities = 0
  async function saveIfStreamEndedAndAllEntitiesWereProcessed() {
    if (snapshotWasCompletelyStreamed && numberOfStreamedEntities === numberOfProcessedEntities) {
      await components.processedSnapshotStorage.saveAsProcessed(snapshotHash)
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'saved' })
    }
  }
  for await (const entity of stream) {
    if (shouldStopStream()) {
      logger.debug('Canceling running sync snapshots stream')
      return
    }
    // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
    // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
    // Once the entity is truly deployed, it should call the method 'markAsDeployed'
    await components.deployer.deployEntity(
      {
        ...entity,
        markAsDeployed: async function () {
          components.metrics.increment('dcl_entities_deployments_processed_total', { source: 'snapshots' })
          numberOfProcessedEntities++
          await saveIfStreamEndedAndAllEntitiesWereProcessed()
        },
        snapshotHash
      },
      entity.servers
    )
    numberOfStreamedEntities++
  }
  snapshotWasCompletelyStreamed = true
  components.metrics.increment('dcl_processed_snapshots_total', { state: 'stream_end' })
  logger.info('Stream ended.', { snapshotHash })
  await saveIfStreamEndedAndAllEntitiesWereProcessed()
}

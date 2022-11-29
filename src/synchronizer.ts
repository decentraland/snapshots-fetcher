import { createCatalystPointerChangesDeploymentStream, getDeployedEntitiesStreamFromSnapshots } from "."
import { createJobLifecycleManagerComponent } from "./job-lifecycle-manager"
import { CatalystDeploymentStreamOptions, IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from "./types"

/**
 * @public
 */
export async function createSynchronizer(
  components: SnapshotsFetcherComponents & {
    deployer: IDeployerComponent
  },
  options: CatalystDeploymentStreamOptions,
): Promise<SynchronizerComponent> {
  const logger = components.logs.getLogger('synchronizer')
  let syncingServers: Set<string> = new Set()
  const lastEntityTimestampFromSnapshotsByServer: Map<string, number> = new Map()
  const deployPointerChangesjobManager = createJobLifecycleManagerComponent(
    components,
    {
      jobManagerName: 'SynchronizationJobManager',
      createJob(contentServer) {
        return createCatalystPointerChangesDeploymentStream(
          components,
          contentServer,
          {
            ...options,
            fromTimestamp: lastEntityTimestampFromSnapshotsByServer.get(contentServer) ? 0 - 20 * 60_000 : undefined
          }
        )
      }
    })

  return {
    async syncFromSnapshots(contentServers: Set<string>) {
      const genesisTimestamp = options.fromTimestamp || 0
      const stream = getDeployedEntitiesStreamFromSnapshots(components, options, Array.from(contentServers))
      logger.info('Starting to deploy entities from snapshots.', { servers: JSON.stringify(contentServers) })
      try {
        for await (const entity of stream) {
          // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely.
          await components.deployer.deployEntity(entity, entity.servers)
          for (const server of entity.servers) {
            const lastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(server) ?? genesisTimestamp
            const deploymentTimestamp = 'entityTimestamp' in entity ? entity.entityTimestamp : entity.localTimestamp
            lastEntityTimestampFromSnapshotsByServer.set(server, Math.max(lastTimestamp, deploymentTimestamp))
          }
        }
      } catch (error) {
        logger.error('There was an error while deploying entities from snapshots.', { error: JSON.stringify(error) })
      }
      logger.info('End deploying entities from snapshots.')
    },
    async syncFromPointerChanges(contentServers: Set<string>) {
      deployPointerChangesjobManager.setDesiredJobs(contentServers)
      syncingServers = contentServers
    },
    async syncWithServers(contentServers: Set<string>) {
      const newServersToBootstrapFromSnapshots = [...contentServers].filter(sv => !syncingServers.has(sv))
      const genesisTimestamp = options.fromTimestamp || 0
      const stream = getDeployedEntitiesStreamFromSnapshots(components, options, newServersToBootstrapFromSnapshots)
      logger.info('Starting to deploy entities from snapshots.', { servers: JSON.stringify(newServersToBootstrapFromSnapshots) })
      try {
        for await (const entity of stream) {
          // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely.
          await components.deployer.deployEntity(entity, entity.servers)
          for (const server of entity.servers) {
            const lastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(server) ?? genesisTimestamp
            const deploymentTimestamp = 'entityTimestamp' in entity ? entity.entityTimestamp : entity.localTimestamp
            lastEntityTimestampFromSnapshotsByServer.set(server, Math.max(lastTimestamp, deploymentTimestamp))
          }
        }
      } catch (error) {
        logger.error('There was an error while deploying entities from snapshots.', { error: JSON.stringify(error) })
      }
      logger.info('End deploying entities from snapshots.')
      deployPointerChangesjobManager.setDesiredJobs(contentServers)
      syncingServers = contentServers
    }
  }

}

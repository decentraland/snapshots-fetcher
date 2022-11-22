import { createCatalystPointerChangesDeploymentStream, getDeployedEntitiesStreamFromSnapshots } from "."
import { createJobLifecycleManagerComponent } from "./job-lifecycle-manager"
import { CatalystDeploymentStreamOptions, DeployedEntityStreamOptions, IDeployerComponent, SnapshotsFetcherComponents } from "./types"

export async function createSynchronizer(
  components: SnapshotsFetcherComponents & {
    deployer: IDeployerComponent
  },
  options: CatalystDeploymentStreamOptions,
) {
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
    async syncWithServers(contentServers: Set<string>) {
      const newServersToBootstrapFromSnapshots = [...contentServers].filter(sv => !syncingServers.has(sv))
      const genesisTimestamp = options.fromTimestamp || 0
      const stream = getDeployedEntitiesStreamFromSnapshots(components, options, newServersToBootstrapFromSnapshots)
      for await (const entity of stream) {
        // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely.
        await components.deployer.deployEntity(entity, entity.servers)
        for (const server of entity.servers) {
          const lastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(server) ?? genesisTimestamp
          const deploymentTimestamp = 'entityTimestamp' in entity ? entity.entityTimestamp : entity.localTimestamp
          lastEntityTimestampFromSnapshotsByServer.set(server, Math.max(lastTimestamp, deploymentTimestamp))
        }
      }
      deployPointerChangesjobManager.setDesiredJobs(contentServers)
      syncingServers = contentServers
    }
  }

}

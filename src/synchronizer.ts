import { createCatalystPointerChangesDeploymentStream, getDeployedEntitiesStreamFromSnapshots2 } from "."
import { getSnapshots } from "./client"
import { createExponentialFallofRetry } from "./exponential-fallof-retry"
import { createJobLifecycleManagerComponent } from "./job-lifecycle-manager"
import { CatalystDeploymentStreamOptions, IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from "./types"

type Snapshot = {
  snapshot: {
    hash: string
    lastIncludedDeploymentTimestamp: number
    replacedSnapshotHashes?: string[] | undefined
  }
  server: string
}

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
  const syncingServers: Set<string> = new Set()
  const bootstsrappingServers: Set<string> = new Set()
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

  async function bootstrapFromSnapshots() {
    const serversSnapshotsBySnapshotHash: Map<string, Snapshot[]> = new Map()
    const serversToDeployFromSnapshots: Set<string> = new Set()
    for (const server of bootstsrappingServers) {
      try {
        const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
        for (const snapshot of snapshots) {
          const snapshotsWithServer = serversSnapshotsBySnapshotHash.get(snapshot.hash) ?? []
          snapshotsWithServer.push({ snapshot, server })
          serversSnapshotsBySnapshotHash.set(snapshot.hash, snapshotsWithServer)
          serversToDeployFromSnapshots.add(server)
        }
      } catch (error) {
        logger.error(`Error getting snapshots from ${server}. Will try later`)
      }
    }

    const genesisTimestamp = options.fromTimestamp || 0
    const stream = getDeployedEntitiesStreamFromSnapshots2(components, options, serversSnapshotsBySnapshotHash)
    logger.info('Starting to deploy entities from snapshots.')
    for await (const entity of stream) {
      // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
      // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
      await components.deployer.deployEntity(entity, entity.servers)
      for (const server of entity.servers) {
        const lastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(server) ?? genesisTimestamp
        const deploymentTimestamp = 'entityTimestamp' in entity ? entity.entityTimestamp : entity.localTimestamp
        lastEntityTimestampFromSnapshotsByServer.set(server, Math.max(lastTimestamp, deploymentTimestamp))
      }
    }
    logger.info('End deploying entities from snapshots.')
    for (const bootstrappedServer of serversToDeployFromSnapshots) {
      syncingServers.add(bootstrappedServer)
      bootstsrappingServers.delete(bootstrappedServer)
    }
  }

  function createSyncJob() {
    return createExponentialFallofRetry(logger, {
      async action() {
        try {
          // metrics?
          await bootstrapFromSnapshots()
          // now we start syncing from pointer changes, it internally managers new servers to start syncing
          deployPointerChangesjobManager.setDesiredJobs(syncingServers)

        } catch (e: any) {
          // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
          throw e
        }
      },
      retryTime: options.reconnectTime,
      retryTimeExponent: options.reconnectRetryTimeExponent ?? 1.1,
      maxInterval: options.maxReconnectionTime,
    })
  }

  return {
    async syncWithServers(serversToSync: Set<string>) {
      for (const serverToSync of serversToSync) {
        if (!syncingServers.has(serverToSync)) {
          bootstsrappingServers.add(serverToSync)
        }
      }

      for (const syncingServer of syncingServers) {
        if (!serversToSync.has(syncingServer)) {
          syncingServers.delete(syncingServer)
        }
      }
      createSyncJob().start()
    }
  }

}

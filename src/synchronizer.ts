import { createCatalystPointerChangesDeploymentStream, getDeployedEntitiesStreamFromSnapshots2 } from "."
import { getSnapshots } from "./client"
import { createExponentialFallofRetry } from "./exponential-fallof-retry"
import { createJobLifecycleManagerComponent } from "./job-lifecycle-manager"
import { CatalystDeploymentStreamOptions, IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from "./types"
import future from 'fp-future'

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
  let initialBootstrapFinished = false
  const initialBootstrapFinishedEventCallbacks: Array<() => void> = []
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
        logger.error(`Error getting snapshots from ${server}.`)
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

    if (!initialBootstrapFinished) {
      initialBootstrapFinished = true
      for (const cb of initialBootstrapFinishedEventCallbacks) {
        await cb()
      }
    }
  }

  function createSyncJob() {
    return createExponentialFallofRetry(logger, {
      async action() {
        try {
          // metrics start?
          await bootstrapFromSnapshots()
          // now we start syncing from pointer changes, it internally managers new servers to start syncing
          deployPointerChangesjobManager.setDesiredJobs(syncingServers)
        } catch (e: any) {
          // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
          // increment metrics
          throw e
        }
        // If there are still some servers that didn't bootstrap, we throw an error so it runs later
        if (bootstsrappingServers.size > 0) {
          throw new Error(`There are servers that failed to bootstrap. Will try later. Servers: ${JSON.stringify(bootstsrappingServers)}`)
        }
      },
      retryTime: options.reconnectTime,
      retryTimeExponent: options.reconnectRetryTimeExponent ?? 1.1,
      maxInterval: options.maxReconnectionTime,
    })
  }

  return {
    async syncWithServers(serversToSync: Set<string>) {
      // 1. Add the new servers (not currently syncing) to the bootstrapping state
      for (const serverToSync of serversToSync) {
        if (!syncingServers.has(serverToSync)) {
          bootstsrappingServers.add(serverToSync)
        }
      }

      // 2. Remote the syncing servers that must not be syncing anymore
      for (const syncingServer of syncingServers) {
        if (!serversToSync.has(syncingServer)) {
          syncingServers.delete(syncingServer)
        }
      }
      createSyncJob().start()
    },
    onInitialBootstrapFinished(cb: () => void) {
      if (!initialBootstrapFinished) {
        initialBootstrapFinishedEventCallbacks.push(cb)
      } else {
        cb()
      }
    }
  }

}

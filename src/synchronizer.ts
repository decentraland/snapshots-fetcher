import { createCatalystPointerChangesDeploymentStream, getDeployedEntitiesStreamFromSnapshots } from "."
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
  let initialBootstrapFinished = false
  const initialBootstrapFinishedEventCallbacks: Array<() => Promise<void>> = []
  const syncingServers: Set<string> = new Set()
  const bootstrappingServers: Set<string> = new Set()
  const lastEntityTimestampFromSnapshotsByServer: Map<string, number> = new Map()
  const deployPointerChangesjobManager = createJobLifecycleManagerComponent(
    components,
    {
      jobManagerName: 'SynchronizationJobManager',
      createJob(contentServer) {
        const lastEntityTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer)
        if (lastEntityTimestamp == undefined) {
          throw new Error(`Can't start pointer changes stream without last entity timestamp for ${contentServer}. This should not happen.`)
        }
        const fromTimestamp = lastEntityTimestamp - 20 * 60_000
        return createCatalystPointerChangesDeploymentStream(
          components,
          contentServer,
          {
            ...options,
            fromTimestamp
          }
        )
      }
    })

  async function syncFromSnapshots(serversToSync: Set<string>) {
    const serversSnapshotsBySnapshotHash: Map<string, Snapshot[]> = new Map()
    const serversToSyncFromSnapshots: Set<string> = new Set()
    const genesisTimestamp = options.fromTimestamp || 0
    for (const server of serversToSync) {
      try {
        const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
        for (const snapshot of snapshots) {
          const snapshotsWithServer = serversSnapshotsBySnapshotHash.get(snapshot.hash) ?? []
          snapshotsWithServer.push({ snapshot, server })
          serversSnapshotsBySnapshotHash.set(snapshot.hash, snapshotsWithServer)
        }
        // If the server doesn't have snapshots yet (for example new servers), then we set to genesisTimestamp
        const lastEntityTimestampOfThisServer = Math.max(...snapshots.map(s => s.lastIncludedDeploymentTimestamp), genesisTimestamp)
        lastEntityTimestampFromSnapshotsByServer.set(server, lastEntityTimestampOfThisServer)
        serversToSyncFromSnapshots.add(server)
      } catch (error) {
        logger.info(`Error getting snapshots from ${server}.`)
      }
    }

    const stream = getDeployedEntitiesStreamFromSnapshots(components, options, serversSnapshotsBySnapshotHash)
    logger.info('Starting to deploy entities from snapshots.')
    for await (const entity of stream) {
      // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
      // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
      await components.deployer.deployEntity(entity, entity.servers)
    }
    logger.info('End deploying entities from snapshots.')
    return serversToSyncFromSnapshots
  }

  async function bootstrap() {
    const syncedFromSnapshotServers = await syncFromSnapshots(bootstrappingServers)
    for (const bootstrappedServer of syncedFromSnapshotServers) {
      syncingServers.add(bootstrappedServer)
      bootstrappingServers.delete(bootstrappedServer)
    }

    if (!initialBootstrapFinished) {
      initialBootstrapFinished = true
      const runningCallbacks = initialBootstrapFinishedEventCallbacks.map(cb => cb())
      await Promise.all(runningCallbacks)
    }
    logger.info(`Bootstrap finished successfully for: ${syncedFromSnapshotServers}`)
  }

  function createSyncJob() {
    return createExponentialFallofRetry(logger, {
      async action() {
        try {
          // metrics start?
          await bootstrap()
          // now we start syncing from pointer changes, it internally managers new servers to start syncing
          deployPointerChangesjobManager.setDesiredJobs(syncingServers)
        } catch (e: any) {
          // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
          // increment metrics
          throw e
        }
        // If there are still some servers that didn't bootstrap, we throw an error so it runs later
        if (bootstrappingServers.size > 0) {
          throw new Error(`There are servers that failed to bootstrap. Will try later. Servers: ${JSON.stringify([...bootstrappingServers])}`)
        }
      },
      retryTime: 5000,
      retryTimeExponent: 1.5,
      maxInterval: 3_600_000,
      exitOnSuccess: true
    })
  }

  return {
    async syncWithServers(serversToSync: Set<string>) {
      // 1. Add the new servers (not currently syncing) to the bootstrapping state
      for (const serverToSync of serversToSync) {
        if (!syncingServers.has(serverToSync)) {
          bootstrappingServers.add(serverToSync)
        }
      }

      // 2. a) Remove from bootstrapping servers that should stop syncing
      for (const bootstappingServer of bootstrappingServers) {
        if (!serversToSync.has(bootstappingServer)) {
          bootstrappingServers.delete(bootstappingServer)
        }
      }

      // 2. b) Remove from syncing servers that should stop syncing
      for (const syncingServer of syncingServers) {
        if (!serversToSync.has(syncingServer)) {
          syncingServers.delete(syncingServer)
        }
      }

      createSyncJob().start()
    },
    async syncSnapshotsForSyncingServers() {
      await syncFromSnapshots(syncingServers)
    },
    async onInitialBootstrapFinished(cb: () => Promise<void>) {
      if (!initialBootstrapFinished) {
        initialBootstrapFinishedEventCallbacks.push(cb)
      } else {
        await cb()
      }
    }
  }

}

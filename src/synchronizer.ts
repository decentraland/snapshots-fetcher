import { createProcessedSnapshotsComponent, getDeployedEntitiesStreamFromPointerChanges, getDeployedEntitiesStreamFromSnapshots } from "."
import { getSnapshots } from "./client"
import { createExponentialFallofRetry } from "./exponential-fallof-retry"
import { createJobLifecycleManagerComponent } from "./job-lifecycle-manager"
import { IDeployerComponent, PointerChangesDeployedEntityStreamOptions, Snapshot, SnapshotsFetcherComponents, SynchronizerComponent, SynchronizerOptions } from "./types"
import { contentServerMetricLabels } from "./utils"

/**
 * @public
 */
export async function createSynchronizer(
  components: SnapshotsFetcherComponents & {
    deployer: IDeployerComponent
  },
  options: SynchronizerOptions
): Promise<SynchronizerComponent> {
  const logger = components.logs.getLogger('synchronizer')
  let initialBootstrapFinished = false
  const genesisTimestamp = options.fromTimestamp || 0
  const initialBootstrapFinishedEventCallbacks: Array<() => Promise<void>> = []
  const bootstrappingServersFromSnapshots: Set<string> = new Set()
  const bootstrappingServersFromPointerChanges: Set<string> = new Set()
  const syncingServers: Set<string> = new Set()
  const lastEntityTimestampFromSnapshotsByServer: Map<string, number> = new Map()

  function increaseLastTimestamp(contentServer: string, ...newTimestamps: number[]) {
    // If the server doesn't have snapshots yet (for example new servers), then we set to genesisTimestamp
    const currentLastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer) || genesisTimestamp
    lastEntityTimestampFromSnapshotsByServer.set(contentServer, Math.max(currentLastTimestamp, ...newTimestamps))
  }

  async function syncFromPointerChanges(contentServer: string, options: PointerChangesDeployedEntityStreamOptions, syncIsStopped: () => boolean) {
    const deployments = getDeployedEntitiesStreamFromPointerChanges(components, options, contentServer)

    for await (const deployment of deployments) {
      // if the stream is closed then we should not process more deployments
      if (syncIsStopped()) {
        logger.debug('Canceling running stream')
        return
      }

      await components.deployer.deployEntity(deployment, [contentServer])

      // update greatest processed timestamp
      increaseLastTimestamp(contentServer, deployment.localTimestamp)
    }
  }

  async function syncFromSnapshots(serversToSync: Set<string>): Promise<Set<string>> {
    const snapshotsByServer: Map<string, Snapshot[]> = new Map()
    for (const server of serversToSync) {
      try {
        const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
        snapshotsByServer.set(server, snapshots)
      } catch (error) {
        logger.info(`Error getting snapshots from ${server}.`)
      }
    }

    // Each time a new processedSnapshots is created because it has data that need to be ephimeral between multiple
    // calls of this method, for example 'snapshotsBeingStreamed'. It's proper of an execution.
    const processedSnapshots = createProcessedSnapshotsComponent(components)

    const stream = getDeployedEntitiesStreamFromSnapshots({ ...components, processedSnapshots }, options, snapshotsByServer)
    logger.info('Starting to deploy entities from snapshots.')
    for await (const entity of stream) {
      // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
      // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
      // Once the entity is truly deployed, it should call the method 'markAsDeployed'
      await components.deployer.deployEntity({
        ...entity,
        markAsDeployed: () => processedSnapshots.entityProcessedFrom(entity.snapshotHash)
      }, entity.servers)
    }
    logger.info('End deploying entities from snapshots.')

    // Once the snapshots were correctly streamed, update the last entity timestamps
    for (const [server, snapshots] of snapshotsByServer) {
      increaseLastTimestamp(server, ...snapshots.map(s => s.lastIncludedDeploymentTimestamp))
    }
    // We only return servers that didn't fail to get its snapshots
    return new Set(snapshotsByServer.keys())
  }

  async function bootstrapFromSnapshots() {
    const syncedServersFromSnapshot = await syncFromSnapshots(bootstrappingServersFromSnapshots)

    for (const bootstrappedServer of syncedServersFromSnapshot) {
      bootstrappingServersFromPointerChanges.add(bootstrappedServer)
      bootstrappingServersFromSnapshots.delete(bootstrappedServer)
    }
  }

  async function bootstrapFromPointerChanges() {
    const pointerChangesBootstrappingJobs = []
    for (const bootstrappingServersFromPointerChange of bootstrappingServersFromPointerChanges) {
      pointerChangesBootstrappingJobs.push(async () => {
        try {
          const lastEntityTimestamp = lastEntityTimestampFromSnapshotsByServer.get(bootstrappingServersFromPointerChange)
          if (lastEntityTimestamp == undefined) {
            throw new Error(`Can't start pointer changes stream without last entity timestamp for ${bootstrappingServersFromPointerChange}. This should never happen.`)
          }
          const fromTimestamp = lastEntityTimestamp - 20 * 60_000
          await syncFromPointerChanges(bootstrappingServersFromPointerChange, { ...options, fromTimestamp }, () => false)
          syncingServers.add(bootstrappingServersFromPointerChange)
          bootstrappingServersFromPointerChanges.delete(bootstrappingServersFromPointerChange)
        } catch (error) {
          // If there's an error, the server doesn't pass to syncing state
          logger.info(`Error bootstrapping from pointer changes for server: ${bootstrappingServersFromPointerChange}`)
        }
      })
    }

    await Promise.all(pointerChangesBootstrappingJobs.map(job => job()))
  }

  async function bootstrap() {
    await bootstrapFromSnapshots()
    await bootstrapFromPointerChanges()
    if (!initialBootstrapFinished) {
      initialBootstrapFinished = true
      const runningCallbacks = initialBootstrapFinishedEventCallbacks.map(cb => cb())
      await Promise.all(runningCallbacks)
    }
    logger.info(`Bootstrap finished successfully`)
  }

  const deployPointerChangesAfterBootstrapJobManager = createJobLifecycleManagerComponent(
    components,
    {
      jobManagerName: 'SynchronizationJobManager',
      createJob(contentServer) {
        const fromTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer)
        if (fromTimestamp == undefined) {
          throw new Error(`Can't start pointer changes stream without last entity timestamp for ${contentServer}. This should never happen.`)
        }
        const metricsLabels = contentServerMetricLabels(contentServer)
        const exponentialFallofRetryComponent = createExponentialFallofRetry(logger, {
          async action() {
            try {
              components.metrics.increment('dcl_deployments_stream_reconnection_count', metricsLabels)
              await syncFromPointerChanges(contentServer, { ...options, fromTimestamp }, () => exponentialFallofRetryComponent.isStopped())
            } catch (e: any) {
              // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
              components.metrics.increment('dcl_deployments_stream_failure_count', metricsLabels)
              throw e
            }
          },
          retryTime: options.syncingReconnection.reconnectTime,
          retryTimeExponent: options.syncingReconnection.reconnectRetryTimeExponent ?? 1.1,
          maxInterval: options.syncingReconnection.maxReconnectionTime,
        })
        return exponentialFallofRetryComponent
      }
    }
  )

  function createSyncJob() {
    return createExponentialFallofRetry(logger, {
      async action() {
        try {
          // metrics start?
          await bootstrap()
          // now we start syncing from pointer changes, it internally managers new servers to start syncing
          deployPointerChangesAfterBootstrapJobManager.setDesiredJobs(syncingServers)
        } catch (e: any) {
          // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
          // increment metrics
          throw e
        }
        // If there are still some servers that didn't bootstrap, we throw an error so it runs later
        if (bootstrappingServersFromSnapshots.size > 0 || bootstrappingServersFromPointerChanges.size > 0) {
          throw new Error(
            `There are servers that failed to bootstrap. Will try later. Servers: ${JSON.stringify([
              ...bootstrappingServersFromSnapshots,
              ...bootstrappingServersFromPointerChanges])
            }`)
        }
      },
      retryTime: options.bootstrapReconnection.reconnectTime ?? 5000,
      retryTimeExponent: options.bootstrapReconnection.reconnectRetryTimeExponent ?? 1.5,
      maxInterval: options.bootstrapReconnection.maxReconnectionTime ?? 3_600_000,
      exitOnSuccess: true
    })
  }

  return {
    async syncWithServers(serversToSync: Set<string>) {
      // 1. Add the new servers (not currently syncing) to the bootstrapping state
      for (const serverToSync of serversToSync) {
        if (!syncingServers.has(serverToSync)) {
          bootstrappingServersFromSnapshots.add(serverToSync)
        }
      }

      // 2. a) Remove from bootstrapping servers that should stop syncing
      for (const bootstappingServer of bootstrappingServersFromSnapshots) {
        if (!serversToSync.has(bootstappingServer)) {
          bootstrappingServersFromSnapshots.delete(bootstappingServer)
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

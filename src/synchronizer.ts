import { getSnapshots } from './client'
import { createExponentialFallofRetry, ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { createJobLifecycleManagerComponent } from './job-lifecycle-manager'
import {
  IDeployerComponent,
  SnapshotInfo,
  SnapshotsFetcherComponents,
  SynchronizerComponent,
  SynchronizerOptions
} from './types'
import { contentServerMetricLabels } from './utils'
import future from 'fp-future'
import PQueue from 'p-queue'
import { deployEntitiesFromPointerChanges, deployEntitiesFromSnapshot } from './deploy-entities'

export async function shouldProcessSnapshotAndMarkAsProcessedIfNeeded(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage'>,
  snapshotHash: string,
  snapshotReplacedGroups: string[][]
) {
  const processedSnapshots = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([
    snapshotHash,
    ...snapshotReplacedGroups.flat()
  ])

  if (processedSnapshots.has(snapshotHash)) {
    return false
  }
  for (const replacedGroup of snapshotReplacedGroups) {
    if (replacedGroup.length > 0 && replacedGroup.every((s) => processedSnapshots.has(s))) {
      await components.processedSnapshotStorage.saveAsProcessed(snapshotHash)
      return false
    }
  }
  return true
}

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
  const genesisTimestamp = options.fromTimestamp || 0
  const bootstrappingServersFromSnapshots: Set<string> = new Set()
  const bootstrappingServersFromPointerChanges: Set<string> = new Set()
  const syncingServers: Set<string> = new Set()
  const lastEntityTimestampFromSnapshotsByServer: Map<string, number> = new Map()
  const syncJobs: ExponentialFallofRetryComponent[] = []
  const regularSyncFromSnapshotsAfterBootstrapJob = createExponentialFallofRetry(logger, {
    async action() {
      try {
        await syncFromSnapshots(syncingServers)
      } catch (e: any) {
        // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
        logger.error(`Error syncing snapshots: ${JSON.stringify(e)}`)
        throw e
      }
    },
    // every 14 days
    retryTime: 86_400_000 * 14,
    retryTimeExponent: 1
  })
  let firstSyncJobStarted = false
  let snapshotsSyncTimeout: NodeJS.Timeout | undefined
  let isStopped = false

  function increaseLastTimestamp(contentServer: string, ...newTimestamps: number[]) {
    // If the server doesn't have snapshots yet (for example new servers), then we set to genesisTimestamp
    const currentLastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer) || genesisTimestamp
    lastEntityTimestampFromSnapshotsByServer.set(contentServer, Math.max(currentLastTimestamp, ...newTimestamps))
  }

  function reportServerStateMetric() {
    components.metrics.observe(
      'dcl_bootstrapping_servers',
      { from: 'snapshots' },
      bootstrappingServersFromSnapshots.size
    )
    components.metrics.observe(
      'dcl_bootstrapping_servers',
      { from: 'pointer-changes' },
      bootstrappingServersFromPointerChanges.size
    )
    components.metrics.observe('dcl_syncing_servers', {}, syncingServers.size)
  }

  async function syncFromSnapshots(serversToSync: Set<string>): Promise<Set<string>> {
    const snapshotInfoByHash: Map<string, SnapshotInfo> = new Map()
    const snapshotLastTimestampByServer: Map<string, number> = new Map()
    for (const server of serversToSync) {
      try {
        const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
        snapshotLastTimestampByServer.set(server, Math.max(...snapshots.map((s) => s.lastIncludedDeploymentTimestamp)))
        for (const snapshot of snapshots) {
          const snapshotInfo = snapshotInfoByHash.get(snapshot.hash)

          const greatestEndTimestamp = snapshotInfo
            ? Math.max(snapshotInfo.greatestEndTimestamp, snapshot.lastIncludedDeploymentTimestamp)
            : snapshot.lastIncludedDeploymentTimestamp

          const replacedSnapshotHashes = snapshotInfo?.replacedSnapshotHashes ?? []
          if (snapshot.replacedSnapshotHashes) {
            replacedSnapshotHashes.push(snapshot.replacedSnapshotHashes)
          }
          const servers = snapshotInfo?.servers ?? new Set()
          servers.add(server)

          snapshotInfoByHash.set(snapshot.hash, {
            snapshotHash: snapshot.hash,
            greatestEndTimestamp,
            replacedSnapshotHashes,
            servers
          })
        }
      } catch (error) {
        logger.info(`Error getting snapshots from ${server}.`)
      }
    }

    logger.info('Starting to deploy entities from snapshots.')

    const deploymentsProcessorsQueue = new PQueue({
      concurrency: 10,
      autoStart: true
    })

    for (const snapshotInfo of snapshotInfoByHash.values()) {
      deploymentsProcessorsQueue
        .add(async () => {
          const { greatestEndTimestamp, replacedSnapshotHashes, snapshotHash } = snapshotInfo
          const shouldProcessSnapshot =
            greatestEndTimestamp > genesisTimestamp &&
            (await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, snapshotHash, replacedSnapshotHashes)) &&
            !(await components.snapshotStorage.has(snapshotHash))
          if (shouldProcessSnapshot) {
            await deployEntitiesFromSnapshot(
              components,
              options,
              snapshotInfo.snapshotHash,
              snapshotInfo.servers,
              () => isStopped
            )
          }
        })
        .catch(logger.error)
    }

    await deploymentsProcessorsQueue.onIdle()

    logger.info('End deploying entities from snapshots.')

    // Once the snapshots were correctly streamed, update the last entity timestamps
    for (const [server, lastTimestamp] of snapshotLastTimestampByServer) {
      increaseLastTimestamp(server, lastTimestamp)
    }
    // We only return servers that didn't fail to get its snapshots
    return new Set(snapshotLastTimestampByServer.keys())
  }

  async function bootstrapFromSnapshots() {
    logger.debug(`Bootstrapping servers (snapshots): ${Array.from(bootstrappingServersFromSnapshots)}`)
    const syncedServersFromSnapshot = await syncFromSnapshots(bootstrappingServersFromSnapshots)

    for (const bootstrappedServer of syncedServersFromSnapshot) {
      bootstrappingServersFromPointerChanges.add(bootstrappedServer)
      bootstrappingServersFromSnapshots.delete(bootstrappedServer)
    }
    reportServerStateMetric()
  }

  async function bootstrapFromPointerChanges() {
    logger.debug(`Bootstrapping servers (Pointer Changes): ${Array.from(bootstrappingServersFromPointerChanges)}`)
    const pointerChangesBootstrappingJobs: (() => Promise<void>)[] = []
    for (const bootstrappingServersFromPointerChange of bootstrappingServersFromPointerChanges) {
      pointerChangesBootstrappingJobs.push(async () => {
        try {
          const lastEntityTimestamp = lastEntityTimestampFromSnapshotsByServer.get(
            bootstrappingServersFromPointerChange
          )
          if (lastEntityTimestamp === undefined) {
            throw new Error(
              `Can't start pointer changes stream without last entity timestamp for ${bootstrappingServersFromPointerChange}. This should never happen.`
            )
          }
          const fromTimestamp = Math.max(lastEntityTimestamp - 20 * 60_000, 0)
          await deployEntitiesFromPointerChanges(
            components,
            { ...options, fromTimestamp, pointerChangesWaitTime: 0 },
            bootstrappingServersFromPointerChange,
            () => isStopped,
            increaseLastTimestamp
          )
          syncingServers.add(bootstrappingServersFromPointerChange)
          bootstrappingServersFromPointerChanges.delete(bootstrappingServersFromPointerChange)
        } catch (error) {
          // If there's an error, the server doesn't pass to syncing state
          logger.info(`Error bootstrapping from pointer changes for server: ${bootstrappingServersFromPointerChange}`)
        }
      })
    }

    if (pointerChangesBootstrappingJobs.length > 0) {
      await Promise.all(pointerChangesBootstrappingJobs.map((job) => job()))
    }

    reportServerStateMetric()
  }

  async function bootstrap() {
    logger.info(`Bootstrap (snapshots): ${Array.from(bootstrappingServersFromSnapshots)}`)
    await bootstrapFromSnapshots()
    logger.info(`Bootstrap (pointer-changes): ${Array.from(bootstrappingServersFromPointerChanges)}`)
    await bootstrapFromPointerChanges()
    logger.debug(`Bootstrap finished.`)
  }

  const deployPointerChangesAfterBootstrapJobManager = createJobLifecycleManagerComponent(components, {
    jobManagerName: 'SynchronizationJobManager',
    createJob(contentServer) {
      const fromTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer)
      if (fromTimestamp === undefined) {
        throw new Error(
          `Can't start pointer changes stream without last entity timestamp for ${contentServer}. This should never happen.`
        )
      }
      const metricsLabels = contentServerMetricLabels(contentServer)
      const exponentialFallofRetryComponent = createExponentialFallofRetry(logger, {
        async action() {
          try {
            components.metrics.increment('dcl_deployments_stream_reconnection_count', metricsLabels)
            await deployEntitiesFromPointerChanges(
              components,
              { ...options, fromTimestamp },
              contentServer,
              () => isStopped,
              increaseLastTimestamp
            )
          } catch (e: any) {
            // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
            components.metrics.increment('dcl_deployments_stream_failure_count', metricsLabels)
            throw e
          }
        },
        retryTime: options.syncingReconnection.reconnectTime,
        retryTimeExponent: options.syncingReconnection.reconnectRetryTimeExponent ?? 1.1,
        maxInterval: options.syncingReconnection.maxReconnectionTime
      })
      return exponentialFallofRetryComponent
    }
  })

  function createSyncJob() {
    const onFirstBootstrapFinishedCallbacks: Array<() => Promise<void>> = []
    let firstBootstrapTryFinished = false
    const syncFinished = future<void>()
    const syncRetry = createExponentialFallofRetry(logger, {
      async action() {
        await bootstrap()
        logger.info('Bootstrap finished')
        if (!isStopped && !firstBootstrapTryFinished) {
          firstBootstrapTryFinished = true
          if (onFirstBootstrapFinishedCallbacks.length > 0) {
            const runningCallbacks = onFirstBootstrapFinishedCallbacks.map((cb) => cb())
            await Promise.all(runningCallbacks)
          }
        }
        // now we start syncing from pointer changes, it internally managers new servers to start syncing
        deployPointerChangesAfterBootstrapJobManager.setDesiredJobs(syncingServers)
        logger.info(`Syncing servers: ${Array.from(syncingServers)}`)
        // If there are still some servers that didn't bootstrap, we throw an error so it runs later
        if (bootstrappingServersFromSnapshots.size > 0 || bootstrappingServersFromPointerChanges.size > 0) {
          throw new Error(
            `There are servers that failed to bootstrap. Will try later. Servers: ${JSON.stringify([
              ...bootstrappingServersFromSnapshots,
              ...bootstrappingServersFromPointerChanges
            ])}`
          )
        }
        syncFinished.resolve()
      },
      retryTime: options.bootstrapReconnection.reconnectTime ?? 5000,
      retryTimeExponent: options.bootstrapReconnection.reconnectRetryTimeExponent ?? 1.5,
      maxInterval: options.bootstrapReconnection.maxReconnectionTime ?? 3_600_000,
      exitOnSuccess: true
    })
    return {
      ...syncRetry,
      async onInitialBootstrapFinished(cb: () => Promise<void>) {
        if (!firstBootstrapTryFinished) {
          onFirstBootstrapFinishedCallbacks.push(cb)
        } else {
          await cb()
        }
      },
      async onSyncFinished() {
        await syncFinished
      }
    }
  }

  // Remove a server from sync list when it was removed from the Servers DAO
  function removeServersNotToSyncFromStateSet(serversToSync: Set<string>, syncStateSet: Set<string>) {
    for (const syncServerInSomeState of syncStateSet) {
      if (!serversToSync.has(syncServerInSomeState)) {
        syncStateSet.delete(syncServerInSomeState)
      }
    }
  }

  return {
    async syncWithServers(serversToSync: Set<string>) {
      if (isStopped) {
        throw new Error('synchronizer is stopped.')
      }
      // 1. Add the new servers (not currently syncing) to the bootstrapping state from snapshots
      for (const serverToSync of serversToSync) {
        if (!syncingServers.has(serverToSync) && !bootstrappingServersFromPointerChanges.has(serverToSync)) {
          bootstrappingServersFromSnapshots.add(serverToSync)
        }
      }

      // 2. a) Remove from bootstrapping servers (snapshots) that should stop syncing
      removeServersNotToSyncFromStateSet(serversToSync, bootstrappingServersFromSnapshots)

      // 2. b) Remove from bootstrapping servers (pointer-changes) that should stop syncing
      removeServersNotToSyncFromStateSet(serversToSync, bootstrappingServersFromPointerChanges)

      // 2. c) Remove from syncing servers that should stop syncing
      removeServersNotToSyncFromStateSet(serversToSync, syncingServers)

      reportServerStateMetric()

      const newSyncJob = createSyncJob()
      if (!firstSyncJobStarted) {
        firstSyncJobStarted = true
        await newSyncJob.onInitialBootstrapFinished(async () => {
          snapshotsSyncTimeout = setTimeout(
            async () => await regularSyncFromSnapshotsAfterBootstrapJob.start(),
            3_600_000
          )
        })
      }
      syncJobs.push(newSyncJob)
      if (syncJobs.length === 1) {
        syncJobs[0].start().finally(async () => {
          syncJobs.shift()
          if (syncJobs.length > 0) {
            syncJobs[0].start().catch(logger.error)
          }
        })
      }
      return newSyncJob
    },
    async stop() {
      // Component will not stop until the sync from snapshots is over.
      if (!isStopped) {
        isStopped = true

        if (syncJobs.length > 0) {
          await syncJobs[0].stop()
          while (syncJobs.length > 0) {
            syncJobs.shift()
          }
        }
        syncingServers.clear()
        if (deployPointerChangesAfterBootstrapJobManager.stop) {
          await deployPointerChangesAfterBootstrapJobManager.stop()
        }
        if (regularSyncFromSnapshotsAfterBootstrapJob.stop) {
          await regularSyncFromSnapshotsAfterBootstrapJob.stop()
        }
        if (snapshotsSyncTimeout) {
          clearTimeout(snapshotsSyncTimeout)
          await regularSyncFromSnapshotsAfterBootstrapJob.stop()
        }
      }
    }
  }
}

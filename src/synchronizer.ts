import future from 'fp-future'
import PQueue from 'p-queue'
import { getSnapshots } from './client'
import {
  decideSnapshotDeploymentFromProcessedSet,
  deployEntitiesFromPointerChanges,
  deployEntitiesFromSnapshot
} from './deploy-entities'
import { ExponentialFallofRetryComponent, createExponentialFallofRetry } from './exponential-fallof-retry'
import { createJobLifecycleManagerComponent } from './job-lifecycle-manager'
import {
  IDeployerComponent,
  SnapshotMetadata,
  SnapshotsFetcherComponents,
  SynchronizerComponent,
  SynchronizerOptions,
  TimeRange
} from './types'
import { contentServerMetricLabels } from './utils'

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
  const pointerChangesShiftFix = 20 * 60_000

  let isStopped = false
  const regularSyncFromSnapshotsAfterBootstrapJob = createExponentialFallofRetry(logger, {
    async action() {
      if (isStopped) {
        return
      }

      try {
        await syncFromSnapshots(syncingServers)
      } catch (e: any) {
        // The full error (with stack) is logged by createExponentialFallofRetry; here we add context.
        // Note: JSON.stringify(error) is "{}", so log the message explicitly.
        logger.error(`Error syncing snapshots: ${e?.message ?? JSON.stringify(e)}`)
        throw e
      }
    },
    // every 14 days
    retryTime: 86_400_000 * 14,
    retryTimeExponent: 1
  })
  let firstSyncJobStarted = false
  let snapshotsSyncTimeout: NodeJS.Timeout | undefined

  function pointerChangesStartingTimestamp(server: string): number {
    const lastTimestamp = lastEntityTimestampFromSnapshotsByServer.get(server)
    // Note: a last timestamp of 0 (genesis) is valid, so we must check for undefined explicitly.
    if (lastTimestamp === undefined) {
      throw new Error(
        `Can't start pointer changes stream without last entity timestamp for ${server}. This should never happen.`
      )
    }
    return Math.max(lastTimestamp - pointerChangesShiftFix, 0)
  }

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
    type Snapshot = SnapshotMetadata & { server: string }
    const snapshotsByHash: Map<string, Snapshot[]> = new Map()
    const snapshotLastTimestampByServer: Map<string, number> = new Map()
    // Fetch every server's snapshots concurrently. getSnapshots already runs through the
    // concurrency-limited downloadQueue, so this just feeds that queue instead of starving it with
    // one serial round-trip per server. The synchronous map mutations below run without interleaving.
    await Promise.all(
      Array.from(serversToSync).map(async (server) => {
        try {
          const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
          // A server may legitimately have no snapshots yet (e.g. brand new). Math.max() of an empty
          // list is -Infinity, so fall back to the genesis timestamp to keep a sane starting point.
          const lastTimestamp =
            snapshots.length > 0 ? Math.max(...snapshots.map((s) => s.timeRange.endTimestamp)) : genesisTimestamp
          snapshotLastTimestampByServer.set(server, lastTimestamp)
          for (const snapshot of snapshots) {
            const snapshotMetadatas = snapshotsByHash.get(snapshot.hash) ?? []
            snapshotMetadatas.push({ ...snapshot, server })
            snapshotsByHash.set(snapshot.hash, snapshotMetadatas)
          }
        } catch (error) {
          logger.info(`Error getting snapshots from ${server}.`)
        }
      })
    )

    const deploymentsProcessorsQueue = new PQueue({
      concurrency: 10,
      autoStart: false
    })

    // Resolve which snapshot hashes were already processed in a single storage call for the whole
    // set, instead of one round-trip per snapshot. The per-snapshot decisions below then read from
    // this set (and only hit snapshotStorage.has / markSnapshotAsProcessed in the branches that need it).
    const allSnapshotHashesToCheck = new Set<string>()
    for (const [snapshotHash, snapshots] of snapshotsByHash) {
      allSnapshotHashesToCheck.add(snapshotHash)
      for (const snapshot of snapshots) {
        for (const replacedHash of snapshot.replacedSnapshotHashes ?? []) {
          allSnapshotHashesToCheck.add(replacedHash)
        }
      }
    }
    const processedSnapshots =
      allSnapshotHashesToCheck.size > 0
        ? await components.processedSnapshotStorage.filterProcessedSnapshotsFrom(Array.from(allSnapshotHashesToCheck))
        : new Set<string>()

    const timeRangesOfEntitiesToDeploy: TimeRange[] = []
    // Each decision may still hit snapshotStorage; run them with bounded concurrency instead of a
    // serial chain. The synchronous push/enqueue after each await can't interleave.
    const shouldProcessChecksQueue = new PQueue({ concurrency: 10 })
    await Promise.all(
      Array.from(snapshotsByHash).map(([snapshotHash, snapshots]) =>
        shouldProcessChecksQueue.add(async () => {
          const replacedSnapshotHashes = snapshots.map((s) => s.replacedSnapshotHashes ?? [])
          const greatestEndTimestamp = Math.max(...snapshots.map((s) => s.timeRange.endTimestamp))
          const shouldProcessSnapshot = await decideSnapshotDeploymentFromProcessedSet(
            components,
            processedSnapshots,
            genesisTimestamp,
            snapshotHash,
            greatestEndTimestamp,
            replacedSnapshotHashes
          )
          if (shouldProcessSnapshot) {
            const servers = new Set(snapshots.map((s) => s.server))
            timeRangesOfEntitiesToDeploy.push(...snapshots.map((s) => s.timeRange))
            deploymentsProcessorsQueue
              .add(async () => {
                await deployEntitiesFromSnapshot(components, options, snapshotHash, servers, () => isStopped)
              })
              .catch((err) => logger.error(err))
          }
        })
      )
    )

    logger.info('Warming up deployer.')
    await components.deployer.prepareForDeploymentsIn(timeRangesOfEntitiesToDeploy)

    logger.info('Starting to deploy entities from snapshots.')
    deploymentsProcessorsQueue.start()

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
    let minStartingPoint: undefined | number
    for (const bootstrappingServersFromPointerChange of bootstrappingServersFromPointerChanges) {
      const fromTimestamp = pointerChangesStartingTimestamp(bootstrappingServersFromPointerChange)
      minStartingPoint = Math.min(fromTimestamp, minStartingPoint ?? fromTimestamp)
      pointerChangesBootstrappingJobs.push(async () => {
        try {
          const fromTimestamp = pointerChangesStartingTimestamp(bootstrappingServersFromPointerChange)
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

    if (minStartingPoint !== undefined) {
      await components.deployer.prepareForDeploymentsIn([
        {
          initTimestamp: minStartingPoint,
          endTimestamp: Date.now()
        }
      ])
    }

    if (pointerChangesBootstrappingJobs.length > 0) {
      await Promise.all(pointerChangesBootstrappingJobs.map((job) => job()))
    }

    reportServerStateMetric()
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
          if (isStopped) {
            return
          }

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
        if (isStopped) {
          return
        }

        logger.info(`Bootstrap (snapshots): ${Array.from(bootstrappingServersFromSnapshots)}`)
        await bootstrapFromSnapshots()

        logger.info(`Bootstrap (pointer-changes): ${Array.from(bootstrappingServersFromPointerChanges)}`)
        await bootstrapFromPointerChanges()

        logger.info('Bootstrap finished')

        if (isStopped) {
          return
        }

        if (!firstBootstrapTryFinished) {
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

  // Runs queued sync jobs one at a time, in FIFO order. Each job re-arms the chain in its finally,
  // so the queue keeps draining no matter how many jobs were enqueued while one was running.
  function startNextSyncJob() {
    if (syncJobs.length === 0) {
      return
    }
    syncJobs[0]
      .start()
      .catch((err) => logger.error(err))
      .finally(() => {
        syncJobs.shift()
        startNextSyncJob()
      })
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
      // Only kick off the chain when this is the sole queued job; otherwise the currently-running
      // chain will pick it up when it finishes (see startNextSyncJob).
      if (syncJobs.length === 1) {
        startNextSyncJob()
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

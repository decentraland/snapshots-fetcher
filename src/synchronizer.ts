import future from 'fp-future'
import PQueue from 'p-queue'
import { getSnapshots } from './client'
import {
  decideSnapshotDeploymentFromProcessedSet,
  deployEntitiesFromPointerChanges,
  deployEntitiesFromSnapshot
} from './deploy-entities'
import { createExponentialFallofRetry } from './exponential-fallof-retry'
import { createJobLifecycleManagerComponent } from './job-lifecycle-manager'
import { createSerialJobRunner } from './serial-job-runner'
import {
  IDeployerComponent,
  SnapshotMetadata,
  SnapshotsFetcherComponents,
  SynchronizerComponent,
  SynchronizerOptions,
  TimeRange
} from './types'
import { contentServerMetricLabels } from './utils'

// Preserved as the defaults these two queues have always used, so omitting `options.concurrency`
// keeps the previous behaviour exactly.
const DEFAULT_SNAPSHOT_DEPLOYMENTS_CONCURRENCY = 10
const DEFAULT_SNAPSHOT_CHECKS_CONCURRENCY = 10

function resolveConcurrency(name: string, value: number | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback
  }
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`options.concurrency.${name} must be an integer >= 1, got ${value}`)
  }
  return value
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
  // Fail fast on a value that would otherwise break sync silently: scheduleJobWithRetries throws
  // synchronously when given 0, and that throw is swallowed by the per-server catch in
  // syncFromSnapshots, so every server would just log "Error getting snapshots" forever.
  if (!Number.isInteger(options.requestMaxRetries) || options.requestMaxRetries < 1) {
    throw new Error(`options.requestMaxRetries must be an integer >= 1, got ${options.requestMaxRetries}`)
  }

  // Resolved up front so a bad value fails here with a clear message instead of inside p-queue's
  // constructor, several stack frames deep in a background sync job.
  const snapshotDeploymentsConcurrency = resolveConcurrency(
    'snapshotDeployments',
    options.concurrency?.snapshotDeployments,
    DEFAULT_SNAPSHOT_DEPLOYMENTS_CONCURRENCY
  )
  const snapshotChecksConcurrency = resolveConcurrency(
    'snapshotChecks',
    options.concurrency?.snapshotChecks,
    DEFAULT_SNAPSHOT_CHECKS_CONCURRENCY
  )

  const logger = components.logs.getLogger('synchronizer')
  const genesisTimestamp = options.fromTimestamp || 0
  const bootstrappingServersFromSnapshots: Set<string> = new Set()
  const bootstrappingServersFromPointerChanges: Set<string> = new Set()
  const syncingServers: Set<string> = new Set()
  // The servers the caller last asked for, i.e. the authority every state transition is checked
  // against. The three sets above are *where* a server currently is; this is whether it should be
  // anywhere at all.
  const desiredServers: Set<string> = new Set()
  const lastEntityTimestampFromSnapshotsByServer: Map<string, number> = new Map()
  // Sync jobs are serialized: only one runs at a time, the rest queue (FIFO).
  const syncJobsRunner = createSerialJobRunner(logger)
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
  // The periodic post-bootstrap sync's run, so stop() can wait for it rather than only signalling it.
  let regularSyncRun: Promise<void> | undefined
  // Memoised shutdown, so concurrent stop() callers share one and none of them returns early.
  let shutdown: Promise<void> | undefined
  // The sync job that is enqueued but has not started yet, if any. At most one is ever waiting: see
  // the coalescing note in syncWithServers.
  let queuedSyncJob: ReturnType<typeof createSyncJob> | undefined

  /** Clears the pending slot, but only if `job` is still the one holding it. */
  function releaseQueuedSyncJob(job: ReturnType<typeof createSyncJob>) {
    if (queuedSyncJob === job) {
      queuedSyncJob = undefined
    }
  }

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

  // Serializes every snapshot sync, whoever starts it. The periodic post-bootstrap job is not
  // enqueued in syncJobsRunner (it loops forever, so it would block the queue), which used to let it
  // run concurrently with a bootstrap sync job. Two concurrent runs can both decide to process a
  // snapshot hash advertised by servers in different states, and the first stream to finish deletes
  // the snapshot file from storage while the other is still reading it.
  let snapshotsSyncChain: Promise<unknown> = Promise.resolve()

  function syncFromSnapshots(serversToSync: Set<string>): Promise<Set<string>> {
    const run = snapshotsSyncChain.then(
      () => syncFromSnapshotsExclusively(serversToSync),
      () => syncFromSnapshotsExclusively(serversToSync)
    )
    snapshotsSyncChain = run.then(
      () => undefined,
      () => undefined
    )
    return run
  }

  async function syncFromSnapshotsExclusively(serversToSync: Set<string>): Promise<Set<string>> {
    // Callers check isStopped before asking for a sync, but queueing behind the lock means the check
    // can go stale before this run starts. Re-check here so a shutdown that lands while a sync is
    // queued doesn't kick off a fresh round of /snapshots requests and snapshot downloads.
    if (isStopped) {
      return new Set()
    }

    type Snapshot = SnapshotMetadata & { server: string }
    const snapshotsByHash: Map<string, Snapshot[]> = new Map()
    const snapshotLastTimestampByServer: Map<string, number> = new Map()
    // Fetch all servers concurrently; getSnapshots already runs through the concurrency-limited
    // downloadQueue. The synchronous map mutations below can't interleave (no await between them).
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
      concurrency: snapshotDeploymentsConcurrency,
      autoStart: false
    })

    // Resolve all processed snapshot hashes in one storage call instead of one per snapshot; the
    // per-snapshot decisions below read from this set.
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
    // Servers with at least one snapshot that could not be fully deployed. Their bootstrap is
    // incomplete, so they must neither advance their last-entity timestamp nor be reported as
    // synced: either one would resume pointer-changes past entities that were never deployed, and
    // (because the snapshot stays unmarked) those entities would only reappear on the next full
    // snapshot sync — up to 14 days later.
    const serversWithFailedSnapshots = new Set<string>()
    // Each decision may still hit snapshotStorage; run them with bounded concurrency instead of a
    // serial chain. The synchronous push/enqueue after each await can't interleave.
    const shouldProcessChecksQueue = new PQueue({ concurrency: snapshotChecksConcurrency })
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
                try {
                  await deployEntitiesFromSnapshot(components, options, snapshotHash, servers, () => isStopped)
                } catch (err: any) {
                  // Recorded inside the job (not in a .catch on the add() promise) so the set is
                  // guaranteed to be populated before onIdle() resolves and it gets read below.
                  logger.error(err)
                  for (const server of servers) {
                    serversWithFailedSnapshots.add(server)
                  }
                }
              })
              .catch((err) => logger.error(err))
          }
        })
      )
    )

    // stop() now waits for this action to return, so bail before the expensive phase rather than
    // deploying every queued snapshot while the caller's shutdown blocks on us. The queue is never
    // started, so nothing in it runs.
    if (isStopped) {
      logger.info('Stopped while syncing from snapshots; abandoning the queued snapshot deployments.')
      deploymentsProcessorsQueue.clear()
      return new Set()
    }

    logger.info('Warming up deployer.')
    await components.deployer.prepareForDeploymentsIn(timeRangesOfEntitiesToDeploy)

    logger.info('Starting to deploy entities from snapshots.')
    deploymentsProcessorsQueue.start()

    await deploymentsProcessorsQueue.onIdle()
    logger.info('End deploying entities from snapshots.')

    // Once the snapshots were correctly streamed, update the last entity timestamps
    for (const [server, lastTimestamp] of snapshotLastTimestampByServer) {
      if (serversWithFailedSnapshots.has(server)) {
        logger.warn('Keeping the last entity timestamp: some of its snapshots failed to deploy.', { server })
        continue
      }
      increaseLastTimestamp(server, lastTimestamp)
    }
    // We only return servers that got their snapshots AND deployed every one of them, so a server
    // with a failed snapshot stays in the bootstrapping state and the whole bootstrap is retried.
    return new Set(
      Array.from(snapshotLastTimestampByServer.keys()).filter((server) => !serversWithFailedSnapshots.has(server))
    )
  }

  /**
   * Whether a server may still be advanced to the next sync state.
   *
   * Bootstrap phases are long-running, so `syncWithServers` can drop a server while a phase that
   * already captured it is still in flight. Promoting unconditionally when that phase finishes
   * resurrects the server: it lands in `syncingServers` and the next `setDesiredJobs` starts a
   * long-lived pointer-changes job for a server the caller explicitly removed. `desiredServers` is the
   * authority on what the caller last asked for, so every state transition is validated against it.
   */
  function canAdvanceServer(server: string): boolean {
    return !isStopped && desiredServers.has(server)
  }

  async function bootstrapFromSnapshots() {
    logger.debug(`Bootstrapping servers (snapshots): ${Array.from(bootstrappingServersFromSnapshots)}`)
    const syncedServersFromSnapshot = await syncFromSnapshots(bootstrappingServersFromSnapshots)

    for (const bootstrappedServer of syncedServersFromSnapshot) {
      bootstrappingServersFromSnapshots.delete(bootstrappedServer)
      if (!canAdvanceServer(bootstrappedServer)) {
        logger.info('Not advancing a server to the pointer-changes bootstrap: it is no longer desired.', {
          server: bootstrappedServer
        })
        continue
      }
      bootstrappingServersFromPointerChanges.add(bootstrappedServer)
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
          bootstrappingServersFromPointerChanges.delete(bootstrappingServersFromPointerChange)
          if (!canAdvanceServer(bootstrappingServersFromPointerChange)) {
            logger.info('Not moving a server to the syncing state: it is no longer desired.', {
              server: bootstrappingServersFromPointerChange
            })
            return
          }
          syncingServers.add(bootstrappingServersFromPointerChange)
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
      if (lastEntityTimestampFromSnapshotsByServer.get(contentServer) === undefined) {
        throw new Error(
          `Can't start pointer changes stream without last entity timestamp for ${contentServer}. This should never happen.`
        )
      }
      const metricsLabels = contentServerMetricLabels(contentServer)
      // Per-job stop signal. The retry component's stop() only prevents the NEXT iteration, and the
      // in-flight pointer-changes stream polls forever on its own (pointerChangesWaitTime > 0), so
      // without a job-scoped flag a server dropped from the desired set would keep being polled and
      // deployed until the whole synchronizer stopped.
      let jobStopped = false
      const shouldStopStream = () => isStopped || jobStopped

      const exponentialFallofRetryComponent = createExponentialFallofRetry(logger, {
        async action() {
          if (shouldStopStream()) {
            return
          }

          // Read the timestamp on every attempt. increaseLastTimestamp advances it as entities are
          // deployed, so a reconnect resumes from the latest processed entity; capturing it once at
          // job creation made every reconnect re-stream everything since bootstrap.
          const fromTimestamp = lastEntityTimestampFromSnapshotsByServer.get(contentServer)
          if (fromTimestamp === undefined) {
            throw new Error(
              `Can't start pointer changes stream without last entity timestamp for ${contentServer}. This should never happen.`
            )
          }

          try {
            components.metrics.increment('dcl_deployments_stream_reconnection_count', metricsLabels)
            await deployEntitiesFromPointerChanges(
              components,
              { ...options, fromTimestamp },
              contentServer,
              shouldStopStream,
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
        maxInterval: options.syncingReconnection.maxReconnectionTime,
        // Surviving a full poll cycle is evidence the stream was healthy, so an isolated failure after
        // hours of syncing reconnects at the base interval instead of the grown one.
        healthyRunTime: Math.max(options.pointerChangesWaitTime, options.syncingReconnection.reconnectTime)
      })

      return {
        ...exponentialFallofRetryComponent,
        async stop() {
          // Set before awaiting, so the in-flight stream sees it at its next check.
          jobStopped = true
          await exponentialFallofRetryComponent.stop()
        }
      }
    }
  })

  function createSyncJob() {
    const onFirstBootstrapFinishedCallbacks: Array<() => Promise<void>> = []
    let firstBootstrapTryFinished = false
    const syncFinished = future<void>()
    // onSyncFinished() hands this future to consumers, so every path that ends the job must settle
    // it. Stopping the synchronizer makes the action short-circuit and exitOnSuccess ends the retry
    // loop, which would otherwise leave the future pending and hang the consumer's shutdown forever.
    // The no-op catch keeps that rejection from surfacing as an unhandled rejection for consumers
    // that never call onSyncFinished().
    syncFinished.catch(() => undefined)

    function abortSyncFinished() {
      if (syncFinished.isPending) {
        syncFinished.reject(new Error('The synchronization job was stopped before it finished.'))
      }
    }

    const syncRetry = createExponentialFallofRetry(logger, {
      async action() {
        if (isStopped) {
          abortSyncFinished()
          return
        }

        logger.info(`Bootstrap (snapshots): ${Array.from(bootstrappingServersFromSnapshots)}`)
        await bootstrapFromSnapshots()

        logger.info(`Bootstrap (pointer-changes): ${Array.from(bootstrappingServersFromPointerChanges)}`)
        await bootstrapFromPointerChanges()

        logger.info('Bootstrap finished')

        if (isStopped) {
          abortSyncFinished()
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
      async start() {
        try {
          await syncRetry.start()
        } finally {
          // Catch-all: whatever reason the retry loop exits for, the future must be settled. Only the
          // success path resolves it, and the loop has other exits — a zero `reconnectTime` makes it
          // return after the first failure — each of which would otherwise leave a consumer awaiting
          // onSyncFinished() forever. A no-op once resolved.
          abortSyncFinished()
        }
      },
      async stop() {
        await syncRetry.stop()
        // Also covers jobs the serial runner drops from its queue without ever starting them.
        abortSyncFinished()
      },
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
      // 0. Record what the caller wants, so an in-flight bootstrap phase that already captured a
      //    now-removed server cannot promote it back into the sync states when it finishes.
      desiredServers.clear()
      for (const serverToSync of serversToSync) {
        desiredServers.add(serverToSync)
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

      // A job that is queued but not yet running is pure redundancy against a second one: when it
      // starts it re-reads the live bootstrapping/syncing sets, which steps 0-2 above have already
      // updated for this caller. Handing back the waiting job keeps a periodic DAO refresh from
      // stacking one full bootstrap pass per call behind a job that is slow or repeatedly failing —
      // the runner is serial, so that queue only ever drained as N redundant passes.
      if (queuedSyncJob) {
        return queuedSyncJob
      }

      const newSyncJob = createSyncJob()
      // Claimed before the only await below, so a concurrent caller coalesces onto this job rather
      // than racing to create a second one in that window.
      queuedSyncJob = newSyncJob
      if (!firstSyncJobStarted) {
        firstSyncJobStarted = true
        await newSyncJob.onInitialBootstrapFinished(async () => {
          snapshotsSyncTimeout = setTimeout(() => {
            // Kept so stop() can wait for it. Dropping the promise here meant nothing could await the
            // periodic sync, so stop() returned while it was still deploying entities.
            regularSyncRun = regularSyncFromSnapshotsAfterBootstrapJob.start()
          }, 3_600_000)
        })
      }
      syncJobsRunner.enqueue({
        async start() {
          // No longer waiting, so the next caller queues a fresh job instead of being handed this
          // one, whose view of the desired servers is already fixed.
          releaseQueuedSyncJob(newSyncJob)
          await newSyncJob.start()
        },
        stop: () => newSyncJob.stop()
      })
      // The await above is the only suspension point in this method, so stop() can land in the middle
      // of the very first call. The runner silently drops jobs enqueued after it stopped, which would
      // leave this job's onSyncFinished() pending forever, so settle it here instead.
      if (isStopped) {
        releaseQueuedSyncJob(newSyncJob)
        await newSyncJob.stop()
      }
      return newSyncJob
    },
    async stop() {
      // Component will not stop until the sync from snapshots is over. Memoised so overlapping callers
      // all wait for the same shutdown instead of a second one reporting success immediately.
      if (!shutdown) {
        shutdown = (async () => {
          isStopped = true

          // The runner drops whatever is still queued, so the pending slot must not keep handing that
          // now-dead job to anyone.
          queuedSyncJob = undefined
          await syncJobsRunner.stop()
          syncingServers.clear()
          if (deployPointerChangesAfterBootstrapJobManager.stop) {
            await deployPointerChangesAfterBootstrapJobManager.stop()
          }
          // Cancel the pending start before stopping the job, so a timer that is about to fire cannot
          // restart it after it was stopped.
          if (snapshotsSyncTimeout) {
            clearTimeout(snapshotsSyncTimeout)
            snapshotsSyncTimeout = undefined
          }
          await regularSyncFromSnapshotsAfterBootstrapJob.stop()
          // Signalling it is not enough: wait for the run itself, as we do for every other job.
          await regularSyncRun?.catch(() => undefined)
        })()
      }
      return shutdown
    }
  }
}

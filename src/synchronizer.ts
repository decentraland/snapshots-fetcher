import future from 'fp-future'
import PQueue from 'p-queue'
import { getSnapshots } from './client'
import {
  decideSnapshotDeploymentFromProcessedSet,
  deployEntitiesFromPointerChanges,
  deployEntitiesFromSnapshot,
  filterProcessedSnapshotsInChunks,
  PointerChangesDeploymentReport
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
import { contentServerMetricLabels, resolveTransferLimits } from './utils'

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

function validateReconnectionOptions(name: string, options: SynchronizerOptions['bootstrapReconnection']): void {
  if (!Number.isFinite(options.reconnectTime) || options.reconnectTime < 0) {
    throw new Error(`options.${name}.reconnectTime must be a finite number >= 0, got ${options.reconnectTime}`)
  }
  if (
    options.reconnectRetryTimeExponent !== undefined &&
    (!Number.isFinite(options.reconnectRetryTimeExponent) || options.reconnectRetryTimeExponent < 1)
  ) {
    throw new Error(
      `options.${name}.reconnectRetryTimeExponent must be a finite number >= 1, got ${options.reconnectRetryTimeExponent}`
    )
  }
  if (
    options.maxReconnectionTime !== undefined &&
    (!Number.isFinite(options.maxReconnectionTime) || options.maxReconnectionTime < 0)
  ) {
    throw new Error(
      `options.${name}.maxReconnectionTime must be a finite number >= 0, got ${options.maxReconnectionTime}`
    )
  }
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

  validateReconnectionOptions('bootstrapReconnection', options.bootstrapReconnection)
  validateReconnectionOptions('syncingReconnection', options.syncingReconnection)

  // Same reasoning as the concurrency options: resolved here so a bad transfer limit is rejected at
  // construction rather than surfacing as a puzzling failure on some individual download later.
  resolveTransferLimits(options.transferLimits)

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
    // Servers whose bootstrap is incomplete, for any reason: a snapshot list we could not fully read, or
    // a snapshot that failed to deploy. They must neither advance their last-entity timestamp nor be
    // reported as synced — either one would resume pointer-changes past entities that were never
    // deployed, and (because the snapshot stays unmarked) those entities would only reappear on the next
    // full snapshot sync.
    const serversWithFailedSnapshots = new Set<string>()
    // Fetch all servers concurrently; getSnapshots already runs through the concurrency-limited
    // downloadQueue. The synchronous map mutations below can't interleave (no await between them).
    await Promise.all(
      Array.from(serversToSync).map(async (server) => {
        try {
          const { snapshots, discardedEntries } = await getSnapshots(
            components,
            server,
            options.requestMaxRetries,
            options.transferLimits
          )
          if (discardedEntries > 0) {
            // The surviving subset is not this server's history. Each discarded entry stood for a time
            // range, so deploying only what parsed and then advancing past the newest of them would skip
            // whatever lived in the ranges we threw away. Still deploy what we can read — that work is
            // real — but keep the server in snapshot bootstrap so the list is fetched again.
            logger.warn('Snapshot list was not fully readable; keeping the server in snapshot bootstrap.', {
              server,
              discardedEntries: String(discardedEntries)
            })
            serversWithFailedSnapshots.add(server)
          }
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
        ? await filterProcessedSnapshotsInChunks(components, allSnapshotHashesToCheck)
        : new Set<string>()

    const timeRangesOfEntitiesToDeploy: TimeRange[] = []
    // Each decision may still hit snapshotStorage; run them with bounded concurrency instead of a
    // serial chain.
    const shouldProcessChecksQueue = new PQueue({ concurrency: snapshotChecksConcurrency })

    // A decision can mark a snapshot as processed, because a group it replaces already is — and that
    // mark is exactly what makes the snapshot replacing IT skippable in turn. Every decision in one
    // pass reads the same set, so a replacement chain (h2 replaces h1 replaces an processed h0) only
    // collapses one link per pass and its tail gets deployed for nothing.
    //
    // So re-run over the candidates that still look deployable until a pass marks nothing new. Only
    // that outcome can change: "already processed", "older than the genesis timestamp" and "present in
    // snapshotStorage" do not depend on what else got marked, so those snapshots are decided for good
    // the first time and drop out. Each pass therefore either marks at least one snapshot — shrinking
    // the candidate pool, since a marked snapshot is no longer deployable — or ends the loop.
    let candidates = Array.from(snapshotsByHash)
    let snapshotsToDeploy: typeof candidates = []
    for (;;) {
      const markedBeforePass = processedSnapshots.size
      const stillDeployable: typeof candidates = []
      await Promise.all(
        candidates.map(([snapshotHash, snapshots]) =>
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
              // Only collected here; the deployment is enqueued once the set has settled, so a snapshot
              // a later pass turns out to have marked is never queued at all.
              stillDeployable.push([snapshotHash, snapshots])
            }
          })
        )
      )
      if (processedSnapshots.size === markedBeforePass) {
        snapshotsToDeploy = stillDeployable
        break
      }
      candidates = stillDeployable
    }

    // Which servers advertised each snapshot we are about to deploy, so the marker re-check after the
    // deployer drains can attribute an unfinished snapshot back to them.
    const serversBySnapshotDeployed = new Map<string, Set<string>>()
    for (const [snapshotHash, snapshots] of snapshotsToDeploy) {
      const servers = new Set(snapshots.map((s) => s.server))
      serversBySnapshotDeployed.set(snapshotHash, servers)
      timeRangesOfEntitiesToDeploy.push(...snapshots.map((s) => s.timeRange))
      deploymentsProcessorsQueue
        .add(async () => {
          try {
            // Stops when the shutdown flag is set, or when no server that advertised this snapshot is
            // wanted any more — matching how a pointer-changes job stops as soon as its server is dropped.
            // Checked across all of them rather than one: a snapshot advertised by several servers is still
            // worth deploying while any of them is desired, since its entities serve all of them.
            //
            // An early stop leaves the snapshot unmarked, so the marker re-check after the deployer drains
            // attributes it back to those servers as unfinished and they do not advance. That is the
            // correct outcome — the work really did not complete — and it costs nothing for a server that
            // is no longer desired.
            await deployEntitiesFromSnapshot(
              components,
              options,
              snapshotHash,
              servers,
              () => isStopped || !Array.from(servers).some((server) => desiredServers.has(server))
            )
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
    // The queue draining only means every snapshot finished STREAMING. Per IDeployerComponent,
    // scheduleEntityDeployment may resolve before the entity is deployed and onIdle() is the drain
    // signal, so with a batching deployer the entities are still in flight here. Advancing a server's
    // timestamp now would resume its pointer-changes past entities that had only been scheduled.
    await components.deployer.onIdle()
    logger.info('End deploying entities from snapshots.')

    // Draining proves the deployer's queue is empty, NOT that every scheduled entity reported back
    // through markAsDeployed. deployEntitiesFromSnapshot only marks a snapshot once they all have, and
    // it returns normally either way, so the marker is the only evidence that a snapshot completed.
    // Without re-reading it, a deployer that quietly dropped an entity would still let the servers
    // advertising that snapshot advance past it.
    if (serversBySnapshotDeployed.size > 0) {
      const processedAfterDeploying = await filterProcessedSnapshotsInChunks(
        components,
        serversBySnapshotDeployed.keys()
      )
      for (const [snapshotHash, servers] of serversBySnapshotDeployed) {
        if (processedAfterDeploying.has(snapshotHash)) {
          continue
        }
        logger.warn('Snapshot was not marked as processed once the deployer drained; treating it as failed.', {
          snapshotHash
        })
        for (const server of servers) {
          serversWithFailedSnapshots.add(server)
        }
      }
    }

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
    // Servers whose pointer-changes bootstrap streamed through without error. They are promoted to the
    // syncing state only after the deployer has drained, not as each stream ends.
    const bootstrappedFromPointerChanges = new Set<string>()
    // Where each server WOULD resume from, held back until the drain confirms it.
    //
    // Advancing the canonical high-water mark as each entity reports deployed commits progress the drain
    // has not confirmed. If onIdle() then rejects because some other queued deployment failed, the
    // server correctly stays in bootstrap — but a later successful deployment has already pushed its
    // mark past the failed one, so the retry resumes beyond an entity that never deployed. Only the
    // 20-minute bootstrap shift stood between that and a silent gap.
    const tentativeTimestamps = new Map<string, number>()
    function collectTentativeTimestamp(contentServer: string, ...newTimestamps: number[]) {
      if (newTimestamps.length === 0) {
        return
      }
      const current = tentativeTimestamps.get(contentServer)
      tentativeTimestamps.set(
        contentServer,
        current === undefined ? Math.max(...newTimestamps) : Math.max(current, ...newTimestamps)
      )
    }
    // What each server's run handed to the deployer against what the deployer confirmed. Draining says
    // the queue emptied, not that every entity in it reported back, and because the resume point is a
    // maximum over acknowledged timestamps a deployer that drops t=100 and confirms t=200 still leaves a
    // mark past the dropped one. This is the pointer-changes equivalent of re-reading the processed
    // marker after a snapshot deployment.
    const deploymentReports = new Map<string, PointerChangesDeploymentReport>()
    let minStartingPoint: undefined | number
    for (const bootstrappingServersFromPointerChange of bootstrappingServersFromPointerChanges) {
      const fromTimestamp = pointerChangesStartingTimestamp(bootstrappingServersFromPointerChange)
      minStartingPoint = Math.min(fromTimestamp, minStartingPoint ?? fromTimestamp)
      pointerChangesBootstrappingJobs.push(async () => {
        try {
          const fromTimestamp = pointerChangesStartingTimestamp(bootstrappingServersFromPointerChange)
          const report: PointerChangesDeploymentReport = { scheduled: 0, acknowledged: 0 }
          deploymentReports.set(bootstrappingServersFromPointerChange, report)
          await deployEntitiesFromPointerChanges(
            components,
            { ...options, fromTimestamp, pointerChangesWaitTime: 0 },
            bootstrappingServersFromPointerChange,
            // Also stops when the caller drops this server, not only on a full shutdown. A bootstrap
            // pass over a long backlog can run for a while, and canAdvanceServer only stops the server
            // being promoted at the end of it — it does not stop the streaming and deploying in the
            // meantime, so a server removed mid-pass kept having its entities deployed until the pass
            // happened to finish.
            () => isStopped || !desiredServers.has(bootstrappingServersFromPointerChange),
            collectTentativeTimestamp,
            report
          )
          // Both leaving pointer-changes bootstrap and entering the syncing state are deferred until
          // the deployer has drained, below. Removing it here would strand the server in neither state
          // if the drain then fails: the retry would find nothing left to bootstrap, report the sync as
          // successful, and stop syncing a server the caller still wants.
          bootstrappedFromPointerChanges.add(bootstrappingServersFromPointerChange)
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
      // A stream ending means every deployment was SCHEDULED. Entering the syncing state is what makes
      // the server resume from its high-water timestamp, so wait for the deployer's own drain signal
      // first — otherwise an asynchronous deployer is still working through entities the server is
      // already considered to be past.
      // If this rejects, nothing below runs and every server stays in pointer-changes bootstrap, so the
      // retry picks them up again rather than losing them.
      await components.deployer.onIdle()
      for (const server of bootstrappedFromPointerChanges) {
        // The drain succeeding is necessary but not sufficient: it says the queue emptied, and this says
        // everything that went into it came back. Short of that the run is incomplete, so the server
        // keeps its old resume point and stays in pointer-changes bootstrap to be retried — committing
        // the tentative mark here would move it past whatever the deployer dropped.
        const report = deploymentReports.get(server)
        if (report && report.acknowledged < report.scheduled) {
          logger.warn(
            'Not all pointer-change deployments were acknowledged once the deployer drained; keeping the server in bootstrap.',
            {
              server,
              scheduled: String(report.scheduled),
              acknowledged: String(report.acknowledged)
            }
          )
          continue
        }
        // Committed only here: the drain succeeded, so everything this pass scheduled for this server
        // really did deploy. A rejection above skips this entirely and the pass is retried from the
        // timestamp it started at.
        const tentative = tentativeTimestamps.get(server)
        if (tentative !== undefined) {
          increaseLastTimestamp(server, tentative)
        }
        bootstrappingServersFromPointerChanges.delete(server)
        if (!canAdvanceServer(server)) {
          logger.info('Not moving a server to the syncing state: it is no longer desired.', { server })
          continue
        }
        syncingServers.add(server)
      }
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

          // The resume point is a maximum over acknowledged timestamps, so committing each mark as its
          // entity reports deployed lets an entity confirmed at t=200 carry the mark past one the deployer
          // dropped at t=100. This stream resumes from the raw mark with no 20-minute shift, so the dropped
          // entity would never be re-fetched from pointer-changes — only the periodic snapshot re-sync
          // would eventually recover it. A single scalar cannot express "everything through T except
          // t=100", which is why no amount of validation fixes this; the mark has to stop advancing past
          // an unconfirmed entity.
          //
          // So hold the marks and commit them at a poll boundary, once the deployer has drained and every
          // entity scheduled so far has reported back. It is the same shape as bootstrap's guard, applied
          // per poll instead of once — the end of a poll is the only checkpoint a stream designed never to
          // end has.
          //
          // Measured cost, against this code with a batching deployer: 0.5% with a 5s poll interval, 2.4%
          // with 1s, 3.7-5.3% back-to-back when either side dominates, and 18.5% back-to-back when network
          // and deployer latency are evenly matched. Far less than it sounds like, because the drain sits at
          // the end of a poll rather than per page or per entity: the pagination loop already overlaps
          // fetching with deploying, so this only ever waits for the residual queue.
          //
          // Liveness is preserved: an entity the deployer permanently drops pins the durable resume point
          // but does not stall the stream, which keeps polling from its own internal high-water mark.
          const tentativeTimestamps: number[] = []
          const report: PointerChangesDeploymentReport = { scheduled: 0, acknowledged: 0 }
          try {
            components.metrics.increment('dcl_deployments_stream_reconnection_count', metricsLabels)
            await deployEntitiesFromPointerChanges(
              components,
              { ...options, fromTimestamp },
              contentServer,
              shouldStopStream,
              (_server, ...newTimestamps) => tentativeTimestamps.push(...newTimestamps),
              report,
              async () => {
                await components.deployer.onIdle()
                // Cumulative, not per poll: this asks whether everything scheduled since the stream
                // started has been confirmed, which is exactly the contiguity condition.
                if (report.acknowledged < report.scheduled) {
                  // End the run rather than carry on. Holding the durable mark back already stops the
                  // missing entity being skipped, but the live stream polls from its OWN internal
                  // high-water mark, so continuing means nothing re-fetches that entity until the stream
                  // happens to restart — and the periodic snapshot sync that would otherwise catch it runs
                  // every 14 days. Ending the run reconnects from the durable mark, which re-delivers it.
                  //
                  // Well-behaved against a deployer that drops it permanently: each attempt now fails
                  // fast, so the exponential falloff throttles the retries, while healthyRunTime means a
                  // single bad poll after a long healthy run reconnects promptly instead of inheriting an
                  // interval grown by unrelated failures.
                  throw new Error(
                    `Not all pointer-change deployments were acknowledged for ${contentServer} (${report.acknowledged} of ${report.scheduled}); reconnecting from the last confirmed timestamp`
                  )
                }
                if (tentativeTimestamps.length === 0) {
                  return
                }
                increaseLastTimestamp(contentServer, ...tentativeTimestamps)
                tentativeTimestamps.length = 0
              }
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

      // Signal the dropped servers' pointer-changes jobs now rather than waiting for the next bootstrap
      // pass to reconcile. That pass is serialized behind whatever is already running, so a server the
      // caller just removed would otherwise keep polling and deploying for as long as that takes. Only
      // already-syncing servers are in this set — newly desired ones start in snapshot bootstrap — so
      // this stops the removed jobs without starting anything.
      deployPointerChangesAfterBootstrapJobManager.setDesiredJobs(syncingServers)

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
          // Our own queues being drained does not mean the deployer is. Entities scheduled before the
          // stop are still being deployed — and mutating the caller's state — until it reports idle, so
          // this is what makes stop() resolving mean "nothing is still deploying".
          await components.deployer.onIdle()
        })()
      }
      return shutdown
    }
  }
}

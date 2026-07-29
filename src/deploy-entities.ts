import { getDeployedEntitiesStreamFromPointerChanges, getDeployedEntitiesStreamFromSnapshot } from '.'
import { SnapshotStreamReport } from './file-processor'
import { contentServerMetricLabels } from './utils'
import {
  IDeployerComponent,
  PointerChangesDeployedEntityStreamOptions,
  SnapshotDeployedEntityStreamOptions,
  SnapshotsFetcherComponents
} from './types'

/**
 * What a pointer-changes run handed to the deployer, against what the deployer confirmed.
 *
 * `deployer.onIdle()` proves the deployer's queue drained, not that every entity in it reported back
 * through `markAsDeployed`. The two counts are how a caller tells those apart: because the resume point
 * is a maximum over acknowledged timestamps, a deployer that drops an entity at t=100 and confirms one
 * at t=200 leaves a mark past the dropped one. Only a caller that knows the run was incomplete can
 * refuse to adopt that mark.
 * @public
 */
export type PointerChangesDeploymentReport = {
  scheduled: number
  acknowledged: number
}

/**
 * This function streams and deploys the entities of pointer-changes of a server. It calls 'increaseLastTimestamp'
 * for each entity deployed.
 *
 * @param report - Optional tally of scheduled versus acknowledged deployments. Callers that record sync
 *   progress from this run must pass one and require `acknowledged >= scheduled` before doing so.
 * @param onPollEnd - Awaited at the end of every poll, before the next one begins. For a polling stream
 *   this is the only checkpoint there is, so it is where a caller settles its deployer and decides what
 *   progress is safe to record.
 * @public
 */
export async function deployEntitiesFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'metrics' | 'fetcher'> & {
    deployer: IDeployerComponent
  },
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string,
  shouldStopStream: () => boolean,
  increaseLastTimestamp: (contentServer: string, ...newTimestamps: number[]) => void,
  report?: PointerChangesDeploymentReport,
  onPollEnd?: () => Promise<void>
) {
  const logger = components.logs.getLogger('deployEntitiesFromPointerChanges')
  const metricsLabels = contentServerMetricLabels(contentServer)
  // The predicate goes into the stream too: checking it only in the loop below cannot end a polling
  // stream whose latest poll returned no deployments at all.
  const deployments = getDeployedEntitiesStreamFromPointerChanges(
    components,
    options,
    contentServer,
    shouldStopStream,
    onPollEnd
  )

  for await (const deployment of deployments) {
    // if the stream is closed then we should not process more deployments
    if (shouldStopStream()) {
      logger.debug('Canceling running stream')
      return
    }

    // Counted before the await, not after: a synchronous deployer calls markAsDeployed from inside
    // scheduleEntityDeployment, so incrementing afterwards would let acknowledged briefly exceed
    // scheduled and make an incomplete run look complete.
    if (report) {
      report.scheduled++
    }
    // Idempotent per scheduled deployment. The counts are what decide whether progress is safe to
    // record, so they have to count *entities acknowledged*, not calls: a deployer that invoked this
    // twice for one entity would otherwise have the second call cover for an entity that was never
    // acknowledged at all, making acknowledged === scheduled with a hole in the middle. It would also
    // double-count the processed metric.
    let alreadyAcknowledged = false
    await components.deployer.scheduleEntityDeployment(
      {
        ...deployment,
        markAsDeployed: async function () {
          if (alreadyAcknowledged) {
            return
          }
          alreadyAcknowledged = true
          components.metrics.increment('dcl_entities_deployments_processed_total', {
            ...metricsLabels,
            source: 'pointer-changes'
          })
          if (report) {
            report.acknowledged++
          }
          // update greatest processed timestamp
          increaseLastTimestamp(contentServer, deployment.localTimestamp)
        }
      },
      [contentServer]
    )
  }
}

/**
 * This function streams and deploys the entities of a snapshot. When the deployer marks all the entities as deployed,
 * it saves the snapshot as processed.
 * @public
 */
export async function deployEntitiesFromSnapshot(
  components: Pick<
    SnapshotsFetcherComponents,
    'metrics' | 'logs' | 'storage' | 'processedSnapshotStorage' | 'snapshotStorage'
  > & {
    deployer: IDeployerComponent
  },
  options: SnapshotDeployedEntityStreamOptions,
  snapshotHash: string,
  servers: Set<string>,
  shouldStopStream: () => boolean
) {
  const logger = components.logs.getLogger('deployEntitiesFromSnapshot')
  // Passed down so the snapshot download abandons its retry ladder on shutdown rather than making
  // whoever called stop() wait it out.
  // Lines the parser could not turn into deployments. Marking a snapshot as processed retires it
  // permanently, so it must only happen when the whole file was actually read: a truncated line or a
  // batch of schema-invalid entities otherwise ends the stream normally and looks identical to an
  // empty snapshot, silently dropping every entity behind those lines.
  const streamReport: SnapshotStreamReport = { unusableLines: 0 }
  const stream = getDeployedEntitiesStreamFromSnapshot(
    components,
    options,
    snapshotHash,
    servers,
    shouldStopStream,
    streamReport
  )
  let snapshotWasCompletelyStreamed = false
  let numberOfStreamedEntities = 0
  let numberOfProcessedEntities = 0
  let snapshotWasMarkedAsProcessed = false
  async function saveIfStreamEndedAndAllEntitiesWereProcessed() {
    // markAsDeployed is idempotent per entity (below), so numberOfProcessedEntities counts entities
    // acknowledged rather than calls received and can never run ahead of numberOfStreamedEntities. That
    // is what makes this comparison mean "all of them": it previously used >= to tolerate a duplicate
    // call, which is precisely how a duplicate could cover for an entity that never deployed and get the
    // snapshot marked processed with a hole in it. >= is kept only as a belt-and-braces guard against
    // ever being left unmarked; the flag (set synchronously before any await) still marks only once.
    if (
      !snapshotWasMarkedAsProcessed &&
      snapshotWasCompletelyStreamed &&
      streamReport.unusableLines === 0 &&
      numberOfProcessedEntities >= numberOfStreamedEntities
    ) {
      snapshotWasMarkedAsProcessed = true
      await components.processedSnapshotStorage.markSnapshotAsProcessed(snapshotHash)
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'saved' })
    }
  }
  for await (const entity of stream) {
    if (shouldStopStream()) {
      logger.debug('Canceling running sync snapshots stream')
      return
    }
    numberOfStreamedEntities++
    // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
    // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
    // Once the entity is truly deployed, it should call the method 'markAsDeployed'
    // Idempotent per scheduled entity, for the same reason as the pointer-changes path: the count is
    // what decides whether the snapshot is complete, so a second call for one entity must not stand in
    // for an entity that was never acknowledged.
    let alreadyAcknowledged = false
    await components.deployer.scheduleEntityDeployment(
      {
        ...entity,
        markAsDeployed: async function () {
          if (alreadyAcknowledged) {
            return
          }
          alreadyAcknowledged = true
          // Empty remote_server, matching the streamed counter: a snapshot has no single origin.
          components.metrics.increment('dcl_entities_deployments_processed_total', {
            remote_server: '',
            source: 'snapshots'
          })
          numberOfProcessedEntities++
          await saveIfStreamEndedAndAllEntitiesWereProcessed()
        },
        snapshotHash
      },
      entity.servers
    )
  }
  snapshotWasCompletelyStreamed = true
  components.metrics.increment('dcl_processed_snapshots_total', { state: 'stream_end' })
  logger.info('Stream ended.', { snapshotHash })
  if (streamReport.unusableLines > 0) {
    // Deliberately left unmarked so a later sync re-downloads and re-streams it. That costs a repeated
    // pass per sync cycle for a snapshot the server keeps serving broken, which is the cheaper of the
    // two failure modes: marking it would drop those entities for good, with nothing to retry.
    components.metrics.increment('dcl_processed_snapshots_total', { state: 'incomplete' })
    // Thrown rather than only logged. The caller treats a rejection as "this server's bootstrap is
    // incomplete", which is what holds its last-entity timestamp back and keeps it in snapshot
    // bootstrap. Returning normally left the snapshot unmarked — so it would be retried — but still let
    // the servers advertising it advance past the range it covers and move on to pointer-changes,
    // skipping the very entities leaving it unmarked was meant to preserve.
    throw new Error(
      `Snapshot ${snapshotHash} had ${streamReport.unusableLines} line(s) that could not be read as deployments; leaving it unprocessed to retry later.`
    )
  }
  await saveIfStreamEndedAndAllEntitiesWereProcessed()
}

/**
 * This function decides if the entities of a snapshot should be deployed or not. It also marks the snapshot as
 * processed if the snapshot was not processed, but at least one whole group of snapshot hashes were processed of one
 * of the replaced ones.
 * @public
 */
// Hashes per processed-snapshots lookup. The aggregate list is not bounded by anything this package
// controls: individual entries cap their replacedSnapshotHashes, but a /snapshots response can carry as
// many entries as fits the 50 MiB body limit — roughly 845,000 hashes — and the synchronizer batches
// every server's entries into one list before looking them up. One oversized `IN` clause is worse than
// slow: Postgres refuses a statement with more than 65,535 bind parameters, so a single server
// advertising enough hashes could make the lookup throw for the whole decision pass, and with it the
// sync from every well-behaved server in the same pass.
//
// 1000 is far under any such limit while keeping the number of round trips small for realistic
// responses, where the whole list fits in one chunk.
const PROCESSED_SNAPSHOT_LOOKUP_CHUNK_SIZE = 1000

/**
 * Looks up processed snapshots in bounded batches, merging the results.
 *
 * Serial rather than concurrent on purpose: the point is to bound the load a single pass puts on the
 * consumer's storage, and issuing every chunk at once would keep the peak it is meant to remove.
 *
 * @param hashes - Any iterable. Taking an iterable rather than an array is what lets a caller avoid
 *   materialising its whole input first: a Set can be passed as-is, and nested groups can be walked by a
 *   generator, so the largest allocation is one chunk. Note that it is consumed lazily *across* the
 *   awaits below, so pass something that will not be mutated while the lookup runs.
 *
 *   Duplicates are not filtered — they only cost slots in a batch, never correctness, and the `Set` of
 *   everything seen that de-duplication would need is exactly the allocation this signature avoids.
 */
export async function filterProcessedSnapshotsInChunks(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage'>,
  hashes: Iterable<string>
): Promise<Set<string>> {
  const processed = new Set<string>()
  let chunk: string[] = []

  async function lookUpChunk() {
    for (const hash of await components.processedSnapshotStorage.filterProcessedSnapshotsFrom(chunk)) {
      processed.add(hash)
    }
    chunk = []
  }

  for (const hash of hashes) {
    chunk.push(hash)
    if (chunk.length === PROCESSED_SNAPSHOT_LOOKUP_CHUNK_SIZE) {
      await lookUpChunk()
    }
  }
  if (chunk.length > 0) {
    await lookUpChunk()
  }
  return processed
}

/** Walks a snapshot and its replaced groups without building the flattened array. */
function* snapshotWithReplacedHashes(snapshotHash: string, replacedSnapshotHashes: string[][]): Generator<string> {
  yield snapshotHash
  for (const group of replacedSnapshotHashes) {
    for (const replacedHash of group) {
      yield replacedHash
    }
  }
}

export async function shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage' | 'snapshotStorage'>,
  genesisTimestamp: number,
  snapshotHash: string,
  greatestEndTimestamp: number,
  replacedSnapshotHashes: string[][]
): Promise<boolean> {
  // Walked by a generator rather than flattened into one array first: this helper is exported, so a
  // direct caller can hand it groups far larger than anything a /snapshots response could produce, and
  // `.flat()` would pay the whole memory and copy cost before any chunking began.
  const processedSnapshots = await filterProcessedSnapshotsInChunks(
    components,
    snapshotWithReplacedHashes(snapshotHash, replacedSnapshotHashes)
  )

  return decideSnapshotDeploymentFromProcessedSet(
    components,
    processedSnapshots,
    genesisTimestamp,
    snapshotHash,
    greatestEndTimestamp,
    replacedSnapshotHashes
  )
}

/**
 * Same decision as shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded, but operating on an
 * already-fetched set of processed snapshot hashes. This lets a caller batch the (potentially
 * expensive) processed-snapshots lookup for many snapshots into as few storage calls as
 * {@link filterProcessedSnapshotsInChunks} needs, rather than one per decision.
 *
 * @param processedSnapshots - Hashes already known to be processed. **Mutated**: when this call marks
 *   `snapshotHash` as processed, it is added here so the set keeps describing storage. A caller reusing
 *   one set across decisions must also re-run those that came back `true`, since a mark can make a
 *   snapshot that replaces the marked one skippable in turn — see the fixed-point loop in
 *   `syncFromSnapshots`.
 */
export async function decideSnapshotDeploymentFromProcessedSet(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage' | 'snapshotStorage'>,
  processedSnapshots: Set<string>,
  genesisTimestamp: number,
  snapshotHash: string,
  greatestEndTimestamp: number,
  replacedSnapshotHashes: string[][]
): Promise<boolean> {
  const snapshotWasProcessed = processedSnapshots.has(snapshotHash)
  const aReplacedGroupWasProcessed = replacedSnapshotHashes.some(
    (replacedGroup) => replacedGroup.length > 0 && replacedGroup.every((s) => processedSnapshots.has(s))
  )

  if (!snapshotWasProcessed) {
    if (!aReplacedGroupWasProcessed) {
      // if the snapshot has newer entities than the genesisPoint (filter)
      return greatestEndTimestamp > genesisTimestamp && !(await components.snapshotStorage.has(snapshotHash))
    } else {
      await components.processedSnapshotStorage.markSnapshotAsProcessed(snapshotHash)
      // Record it in the caller's set too. A batching caller reuses one set across many decisions, and
      // a snapshot marked here is exactly what makes the snapshot that REPLACES it skippable; leaving
      // the set stale meant the next link in a replacement chain was deployed for nothing.
      processedSnapshots.add(snapshotHash)
    }
  }
  return false
}

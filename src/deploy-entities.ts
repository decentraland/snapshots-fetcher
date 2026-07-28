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
 * This function streams and deploys the entities of pointer-changes of a server. It calls 'increaseLastTimestamp'
 * for each entity deployed.
 * @public
 */
export async function deployEntitiesFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'metrics' | 'fetcher'> & {
    deployer: IDeployerComponent
  },
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string,
  shouldStopStream: () => boolean,
  increaseLastTimestamp: (contentServer: string, ...newTimestamps: number[]) => void
) {
  const logger = components.logs.getLogger('deployEntitiesFromPointerChanges')
  const metricsLabels = contentServerMetricLabels(contentServer)
  // The predicate goes into the stream too: checking it only in the loop below cannot end a polling
  // stream whose latest poll returned no deployments at all.
  const deployments = getDeployedEntitiesStreamFromPointerChanges(components, options, contentServer, shouldStopStream)

  for await (const deployment of deployments) {
    // if the stream is closed then we should not process more deployments
    if (shouldStopStream()) {
      logger.debug('Canceling running stream')
      return
    }

    await components.deployer.scheduleEntityDeployment(
      {
        ...deployment,
        markAsDeployed: async function () {
          components.metrics.increment('dcl_entities_deployments_processed_total', {
            ...metricsLabels,
            source: 'pointer-changes'
          })
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
    // >= (not ===) so an extra markAsDeployed call can't leave the snapshot unmarked forever; the
    // flag (set synchronously before any await) ensures we still mark only once.
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
    await components.deployer.scheduleEntityDeployment(
      {
        ...entity,
        markAsDeployed: async function () {
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
    logger.error('Snapshot had lines that could not be read as deployments; leaving it unprocessed to retry later.', {
      snapshotHash,
      unusableLines: String(streamReport.unusableLines)
    })
  }
  await saveIfStreamEndedAndAllEntitiesWereProcessed()
}

/**
 * This function decides if the entities of a snapshot should be deployed or not. It also marks the snapshot as
 * processed if the snapshot was not processed, but at least one whole group of snapshot hashes were processed of one
 * of the replaced ones.
 * @public
 */
export async function shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage' | 'snapshotStorage'>,
  genesisTimestamp: number,
  snapshotHash: string,
  greatestEndTimestamp: number,
  replacedSnapshotHashes: string[][]
): Promise<boolean> {
  const processedSnapshots = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([
    snapshotHash,
    ...replacedSnapshotHashes.flat()
  ])

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
 * expensive) filterProcessedSnapshotsFrom lookup for many snapshots into a single storage call.
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
    }
  }
  return false
}

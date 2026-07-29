import { PointerChangesSyncDeployment } from '@dcl/schemas'
import { createHash } from 'crypto'
import { fetchPointerChanges } from './client'
import { downloadFileWithRetries } from './downloader'
import { processDeploymentsInFile, SnapshotStreamReport } from './file-processor'
import { assertPointerChangesDeploymentWithinStructuralLimits } from './pointer-changes-limits'
import {
  PointerChangesDeployedEntityStreamOptions,
  SnapshotDeployedEntityStreamOptions,
  SnapshotsFetcherComponents
} from './types'
import { contentServerMetricLabels, sleepUnlessStopped } from './utils'

export { metricsDefinitions } from './metrics'
export { IDeployerComponent, SynchronizerComponent } from './types'

/**
 * Accepts a fromTimestamp option to filter out previous deployments.
 * Loads deployments from snapshots and returns an async iterable of deployments.
 * Snapshots are downloaded to the provided "storage" component and deleted right after processing.
 * @public
 */
export async function* getDeployedEntitiesStreamFromSnapshot(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'storage'> & {
    metrics?: SnapshotsFetcherComponents['metrics']
  },
  options: SnapshotDeployedEntityStreamOptions,
  snapshotHash: string,
  servers: Set<string>,
  shouldStop: () => boolean = () => false,
  report?: SnapshotStreamReport
) {
  const genesisTimestamp = options.fromTimestamp || 0
  const logs = components.logs.getLogger('getDeployedEntitiesStreamFromSnapshot')
  // Materialised once. Every yielded deployment carries this list, and rebuilding it per entity meant
  // one array allocation per entity in the snapshot.
  const serversList = Array.from(servers)
  logs.info('Snapshot to be processed.', { hash: snapshotHash, contentServers: JSON.stringify(serversList) })
  try {
    // 1. download the snapshot file if needed
    await downloadFileWithRetries(
      components,
      snapshotHash,
      options.tmpDownloadFolder,
      serversList,
      new Map(),
      options.requestMaxRetries,
      options.requestRetryWaitTime,
      shouldStop,
      options.transferLimits
    )

    // 2. open the snapshot file and process line by line
    const deploymentsInFile = processDeploymentsInFile(snapshotHash, components, logs, report)
    for await (const deployment of deploymentsInFile) {
      if (deployment.entityTimestamp >= genesisTimestamp) {
        // Empty remote_server: a snapshot is content-addressed and usually advertised by several
        // servers at once, so its entities cannot be attributed to one origin.
        components.metrics?.increment('dcl_entities_deployments_streamed_total', {
          remote_server: '',
          source: 'snapshots'
        })
        yield {
          ...deployment,
          snapshotHash,
          servers: serversList
        }
      }
    }
  } finally {
    if (options.deleteSnapshotAfterUsage !== false) {
      try {
        await components.storage.delete([snapshotHash])
      } catch (err: any) {
        logs.error(err)
      }
    }
  }
}

/**
 * Accepts a fromTimestamp option to filter out previous deployments.
 *
 * @param shouldStop - Consulted before every poll and every yielded deployment. Required to end a
 *   polling stream (`pointerChangesWaitTime > 0`), which otherwise runs forever: a consumer that
 *   only breaks out of its own `for await` body never gets to decide anything on a poll that returns
 *   no deployments at all.
 * @public
 */
// Bounds the boundary state. Beyond this many distinct rows at a single timestamp the stream stops
// recording them, which can only cause a re-delivery on the next poll — the deployer is idempotent, so a
// re-yield costs work rather than correctness, whereas failing to yield a row loses it. Erring towards
// re-delivery is what makes exceeding this safe.
const MAX_BOUNDARY_ROWS_TRACKED = 10_000

/**
 * Identity of a boundary *row*, not of its entity.
 *
 * `entityId` is the hash of the entity file and does not cover the authChain, so two rows can legitimately
 * share an id while being distinct deployments. A budget keyed by id alone therefore cannot tell a replayed
 * row from a new one — and nothing guarantees a server replays rows in the order it first sent them. If a
 * new row arrives before the replay of the one already delivered, an id-keyed budget spends the allowance
 * on the new row, suppresses it, and then yields the replay: the new pointer-change is silently lost.
 *
 * Fingerprinting every field the schema carries removes that guesswork. Each field is streamed into a
 * fixed-size digest rather than first serializing the whole row: remote pointer and auth-chain strings are
 * bounded only by the per-page response cap, so retaining or transiently duplicating their canonical form
 * would let one timestamp consume far more memory than its fixed-size keys require. Length prefixes keep
 * adjacent variable-length fields unambiguous. Counts are still kept alongside the digest, because two rows
 * identical in all of these fields are genuinely indistinguishable.
 *
 * @internal
 */
export function boundaryRowFingerprint(deployment: PointerChangesSyncDeployment): string {
  // Defensive for direct/deep callers. The normal stream path has already applied this before yielding
  // the deployment, but this function remains a runtime export even when stripInternal removes it from
  // declarations.
  assertPointerChangesDeploymentWithinStructuralLimits(deployment)
  const hash = createHash('sha256')
  const updateField = (value: string | number): void => {
    const encoded = String(value)
    hash.update(String(Buffer.byteLength(encoded, 'utf8')))
    hash.update(':')
    hash.update(encoded, 'utf8')
  }

  // Domain/version first, so changing the canonical field sequence later cannot silently reuse keys.
  updateField('pointer-change-boundary-row-v1')
  updateField(deployment.entityType)
  updateField(deployment.entityId)
  updateField(deployment.entityTimestamp)
  updateField(deployment.localTimestamp)

  // Sorted so a server listing an equivalent pointer set in another order does not read as a new row.
  const sortedPointers = [...deployment.pointers].sort()
  updateField(sortedPointers.length)
  for (const pointer of sortedPointers) {
    updateField(pointer)
  }

  updateField(deployment.authChain.length)
  for (const link of deployment.authChain) {
    updateField(link.type)
    updateField(link.payload)
    updateField(link.signature ?? '')
  }

  return hash.digest('base64url')
}

export async function* getDeployedEntitiesStreamFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'fetcher'> & {
    metrics?: SnapshotsFetcherComponents['metrics']
  },
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string,
  shouldStop: () => boolean = () => false,
  /** Awaited at the end of every poll, before the next one starts. See the call site below. */
  onPollEnd?: () => Promise<void>
) {
  const logs = components.logs.getLogger(`pointerChangesStream(${contentServer})`)
  // Origin only, so the label stays low-cardinality. Unlike a snapshot, a pointer-changes deployment
  // has exactly one server behind it, which is what makes "who stopped delivering entities?"
  // answerable from this counter.
  const pointerChangesMetricLabels = contentServerMetricLabels(contentServer)
  // fetch the /pointer-changes of the remote server using the last timestamp from the previous step with a grace period of 20 min
  const genesisTimestamp = options.fromTimestamp || 0
  let greatestLocalTimestampProcessed = genesisTimestamp
  // `from` is inclusive, so every poll re-returns the deployments sitting exactly at the high-water
  // timestamp. This records how many rows earlier polls already delivered there, keyed by
  // {@link boundaryRowFingerprint} — the only re-yields there are to suppress.
  //
  // Three properties, each removing a dependency on the server behaving well:
  //
  //   - keyed by full-row fingerprint, so a new row cannot spend the allowance belonging to a replayed one
  //     when the server returns them in a different order than it first did;
  //   - counted rather than a membership test, so "delivered once" is distinguishable from "delivered
  //     twice" when a later poll shows a replay plus a new identical row;
  //   - read frozen for the duration of a poll, so a poll's own rows never suppress each other.
  let rowsDeliveredAtPreviousBoundary = new Map<string, number>()
  logs.debug('Starting to stream entities from Pointer-Changes.', {
    contentServer,
    timestamp: new Date(genesisTimestamp).toISOString()
  })
  do {
    if (shouldStop()) {
      logs.debug('Stopping the Pointer-Changes stream.', { contentServer })
      return
    }

    // Captured before the poll so the suppression budget is fixed at what earlier polls delivered; this
    // poll's own rows cannot add to it.
    const boundaryTimestamp = greatestLocalTimestampProcessed
    const remainingBoundarySuppressions = new Map(rowsDeliveredAtPreviousBoundary)
    // Seeded from the previous boundary because the stream is still standing at that timestamp: if this
    // poll never advances past it, the next one must still skip what was already sent there.
    let rowsDeliveredAtGreatestTimestamp = new Map(rowsDeliveredAtPreviousBoundary)
    // 1. download pointer changes and yield
    const pointerChanges = fetchPointerChanges(
      components,
      contentServer,
      greatestLocalTimestampProcessed,
      logs,
      options.transferLimits
    )
    for await (const deployment of pointerChanges) {
      if (shouldStop()) {
        logs.debug('Stopping the Pointer-Changes stream.', { contentServer })
        return
      }

      const localTimestamp = deployment.localTimestamp

      // when we move past the previous high-water timestamp, nothing has been delivered at the new one yet
      if (localTimestamp > greatestLocalTimestampProcessed) {
        greatestLocalTimestampProcessed = localTimestamp
        rowsDeliveredAtGreatestTimestamp = new Map<string, number>()
      }

      // Computed only where it is needed — to test a boundary row, or to record one at the high-water
      // timestamp — rather than for every row of every page.
      const isBoundaryRow = localTimestamp === boundaryTimestamp
      const isAtGreatestTimestamp = localTimestamp === greatestLocalTimestampProcessed
      const fingerprint = isBoundaryRow || isAtGreatestTimestamp ? boundaryRowFingerprint(deployment) : undefined

      // Spends one allowance per matching row, so a row beyond what was delivered is not suppressed.
      let alreadyDeliveredBeforeThisPoll = false
      if (isBoundaryRow && fingerprint !== undefined) {
        const remaining = remainingBoundarySuppressions.get(fingerprint) ?? 0
        if (remaining > 0) {
          remainingBoundarySuppressions.set(fingerprint, remaining - 1)
          alreadyDeliveredBeforeThisPoll = true
        }
      }

      // selectively ignore deployments by localTimestamp, and skip only what an earlier poll delivered
      if (localTimestamp >= genesisTimestamp && !alreadyDeliveredBeforeThisPoll) {
        if (
          localTimestamp === greatestLocalTimestampProcessed &&
          fingerprint !== undefined &&
          !rowsDeliveredAtGreatestTimestamp.has(fingerprint) &&
          rowsDeliveredAtGreatestTimestamp.size >= MAX_BOUNDARY_ROWS_TRACKED
        ) {
          // Failing before yielding the untrackable row means no work from this poll is committed by the
          // synchronizer's onPollEnd checkpoint. Its retry component then backs off instead of deploying
          // the overflow rows on every inclusive poll forever.
          throw new Error(
            `Too many distinct deployments at local timestamp ${localTimestamp} from ${contentServer}; ` +
              `the maximum boundary rows tracked per poll is ${MAX_BOUNDARY_ROWS_TRACKED}`
          )
        }
        components.metrics?.increment('dcl_entities_deployments_streamed_total', {
          ...pointerChangesMetricLabels,
          source: 'pointer-changes'
        })
        yield deployment
        if (localTimestamp === greatestLocalTimestampProcessed && fingerprint !== undefined) {
          const alreadyTracked = rowsDeliveredAtGreatestTimestamp.has(fingerprint)
          if (alreadyTracked || rowsDeliveredAtGreatestTimestamp.size < MAX_BOUNDARY_ROWS_TRACKED) {
            rowsDeliveredAtGreatestTimestamp.set(
              fingerprint,
              (rowsDeliveredAtGreatestTimestamp.get(fingerprint) ?? 0) + 1
            )
          }
        }
      }
    }

    // Whatever ended up at the high-water timestamp is what the next poll's inclusive `from` will hand
    // back, so it becomes that poll's suppression budget.
    rowsDeliveredAtPreviousBoundary = rowsDeliveredAtGreatestTimestamp

    // The end of a poll: everything this poll had to offer has been yielded, and nothing more will be
    // until the next one. That makes it the only point in a stream designed never to end where a
    // consumer can ask "did all of it actually land?" — awaited here rather than announced, so the
    // consumer can settle its deployer before more entities arrive.
    if (onPollEnd) {
      await onPollEnd()
    }

    // Interruptible: lifecycle stop now awaits the running stream, so a plain sleep here would make
    // shutdown latency the full configured poll interval.
    await sleepUnlessStopped(options.pointerChangesWaitTime, shouldStop)
  } while (options.pointerChangesWaitTime > 0 && !shouldStop())
}

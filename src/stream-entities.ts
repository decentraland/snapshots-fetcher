import { fetchPointerChanges } from './client'
import { downloadFileWithRetries } from './downloader'
import { processDeploymentsInFile, SnapshotStreamReport } from './file-processor'
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
      shouldStop
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
export async function* getDeployedEntitiesStreamFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'fetcher'> & {
    metrics?: SnapshotsFetcherComponents['metrics']
  },
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string,
  shouldStop: () => boolean = () => false
) {
  const logs = components.logs.getLogger(`pointerChangesStream(${contentServer})`)
  // Origin only, so the label stays low-cardinality. Unlike a snapshot, a pointer-changes deployment
  // has exactly one server behind it, which is what makes "who stopped delivering entities?"
  // answerable from this counter.
  const pointerChangesMetricLabels = contentServerMetricLabels(contentServer)
  // fetch the /pointer-changes of the remote server using the last timestamp from the previous step with a grace period of 20 min
  const genesisTimestamp = options.fromTimestamp || 0
  let greatestLocalTimestampProcessed = genesisTimestamp
  // `from` is inclusive, so each poll re-returns the deployments at the high-water timestamp. Track
  // the entityIds already yielded at that timestamp to skip those re-yields (never a distinct one).
  let entityIdsYieldedAtGreatestTimestamp = new Set<string>()
  logs.debug('Starting to stream entities from Pointer-Changes.', {
    contentServer,
    timestamp: new Date(genesisTimestamp).toISOString()
  })
  do {
    if (shouldStop()) {
      logs.debug('Stopping the Pointer-Changes stream.', { contentServer })
      return
    }

    // 1. download pointer changes and yield
    const pointerChanges = fetchPointerChanges(components, contentServer, greatestLocalTimestampProcessed, logs)
    for await (const deployment of pointerChanges) {
      if (shouldStop()) {
        logs.debug('Stopping the Pointer-Changes stream.', { contentServer })
        return
      }

      const localTimestamp = deployment.localTimestamp

      // when we move past the previous high-water timestamp, reset the per-timestamp dedup set
      if (localTimestamp > greatestLocalTimestampProcessed) {
        greatestLocalTimestampProcessed = localTimestamp
        entityIdsYieldedAtGreatestTimestamp = new Set<string>()
      }

      const alreadyYielded =
        localTimestamp === greatestLocalTimestampProcessed &&
        entityIdsYieldedAtGreatestTimestamp.has(deployment.entityId)

      // selectively ignore deployments by localTimestamp, and skip ones already yielded this run
      if (localTimestamp >= genesisTimestamp && !alreadyYielded) {
        components.metrics?.increment('dcl_entities_deployments_streamed_total', {
          ...pointerChangesMetricLabels,
          source: 'pointer-changes'
        })
        yield deployment
        if (localTimestamp === greatestLocalTimestampProcessed) {
          entityIdsYieldedAtGreatestTimestamp.add(deployment.entityId)
        }
      }
    }

    // Interruptible: lifecycle stop now awaits the running stream, so a plain sleep here would make
    // shutdown latency the full configured poll interval.
    await sleepUnlessStopped(options.pointerChangesWaitTime, shouldStop)
  } while (options.pointerChangesWaitTime > 0 && !shouldStop())
}

import { fetchPointerChanges } from './client'
import { downloadFileWithRetries } from './downloader'
import { processDeploymentsInFile } from './file-processor'
import {
  PointerChangesDeployedEntityStreamOptions,
  SnapshotDeployedEntityStreamOptions,
  SnapshotsFetcherComponents
} from './types'
import { sleep } from './utils'

export { metricsDefinitions } from './metrics'
export { IDeployerComponent, SynchronizerComponent } from './types'

/**
 * Accepts a fromTimestamp option to filter out previous deployments.
 * Loads deployments from snapshots and returns an async iterable of deployments.
 * Snapshots are downloaded to the provided "storage" component and deleted right after processing.
 * @public
 */
export async function* getDeployedEntitiesStreamFromSnapshot(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'metrics' | 'storage'>,
  options: SnapshotDeployedEntityStreamOptions,
  snapshotHash: string,
  servers: Set<string>
) {
  const genesisTimestamp = options.fromTimestamp || 0
  const logs = components.logs.getLogger('getDeployedEntitiesStreamFromSnapshot')
  logs.info('Snapshot to be processed.', { hash: snapshotHash, contentServers: JSON.stringify(Array.from(servers)) })
  try {
    // 1. download the snapshot file if needed
    await downloadFileWithRetries(
      components,
      snapshotHash,
      options.tmpDownloadFolder,
      Array.from(servers),
      new Map(),
      options.requestMaxRetries,
      options.requestRetryWaitTime
    )

    // 2. open the snapshot file and process line by line
    const deploymentsInFile = processDeploymentsInFile(snapshotHash, components, logs)
    for await (const deployment of deploymentsInFile) {
      if (deployment.entityTimestamp >= genesisTimestamp) {
        components.metrics.increment('dcl_entities_deployments_streamed_total', { source: 'snapshots' })
        yield {
          ...deployment,
          snapshotHash,
          servers: Array.from(servers)
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
 * @public
 */
export async function* getDeployedEntitiesStreamFromPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'logs' | 'metrics' | 'fetcher'>,
  options: PointerChangesDeployedEntityStreamOptions,
  contentServer: string
) {
  const logs = components.logs.getLogger(`pointerChangesStream(${contentServer})`)
  // fetch the /pointer-changes of the remote server using the last timestamp from the previous step with a grace period of 20 min
  const genesisTimestamp = options.fromTimestamp || 0
  let greatestLocalTimestampProcessed = genesisTimestamp
  logs.info('Starting to stream entities from Pointer-Changes.', {
    contentServer,
    timestamp: new Date(genesisTimestamp).toISOString()
  })
  do {
    // 1. download pointer changes and yield
    const pointerChanges = fetchPointerChanges(components, contentServer, greatestLocalTimestampProcessed, logs)
    for await (const deployment of pointerChanges) {
      // selectively ignore deployments by localTimestamp
      if (deployment.localTimestamp >= genesisTimestamp) {
        components.metrics.increment('dcl_entities_deployments_streamed_total', { source: 'pointer-changes' })
        yield deployment
      }

      // update greatest processed local timestamp
      if (deployment.localTimestamp) {
        greatestLocalTimestampProcessed = Math.max(greatestLocalTimestampProcessed, deployment.localTimestamp)
      }
    }

    await sleep(options.pointerChangesWaitTime)
  } while (options.pointerChangesWaitTime > 0)
}

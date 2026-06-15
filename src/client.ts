import { PointerChangesSyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { metricsDefinitions } from './metrics'
import { SnapshotMetadata, SnapshotsFetcherComponents } from './types'
import {
  contentServerMetricLabels,
  fetchJson,
  isValidContentHash,
  saveContentFileToDisk as saveContentFile
} from './utils'

// The /snapshots payload comes from remote, untrusted servers. We only trust entries that have the
// shape we actually rely on (a valid content hash and a numeric time range) so that a malformed or
// malicious response can't break downstream logic (e.g. Math.max over timestamps) or smuggle a
// path-traversal value through the snapshot hash.
function isValidSnapshotMetadata(snapshot: any): snapshot is SnapshotMetadata {
  return (
    !!snapshot &&
    typeof snapshot.hash === 'string' &&
    isValidContentHash(snapshot.hash) &&
    !!snapshot.timeRange &&
    typeof snapshot.timeRange.initTimestamp === 'number' &&
    typeof snapshot.timeRange.endTimestamp === 'number' &&
    (snapshot.replacedSnapshotHashes === undefined ||
      (Array.isArray(snapshot.replacedSnapshotHashes) &&
        snapshot.replacedSnapshotHashes.every((hash: any) => typeof hash === 'string')))
  )
}

export async function getSnapshots(
  components: SnapshotsFetcherComponents,
  server: string,
  retries: number
): Promise<SnapshotMetadata[]> {
  const logger = components.logs.getLogger('getSnapshots')
  const incrementalSnapshotsUrl = new URL(`${server}/snapshots`).toString()
  const response = await components.downloadQueue.scheduleJobWithRetries(
    () => fetchJson(incrementalSnapshotsUrl, components.fetcher, { timeout: 15000 }),
    retries
  )

  if (!Array.isArray(response)) {
    throw new Error(`Invalid /snapshots response from ${server}: expected an array`)
  }

  const validSnapshots: SnapshotMetadata[] = []
  for (const snapshot of response) {
    if (isValidSnapshotMetadata(snapshot)) {
      validSnapshots.push(snapshot)
    } else {
      logger.error('Ignoring invalid snapshot metadata received from server', {
        server,
        snapshot: JSON.stringify(snapshot)
      })
    }
  }

  // newest first
  return validSnapshots.sort((s1, s2) => s2.timeRange.endTimestamp - s1.timeRange.endTimestamp)
}

export async function* fetchJsonPaginated<T>(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  url: string,
  selector: (responseBody: any) => T[],
  responseTimeMetric: keyof typeof metricsDefinitions
): AsyncIterable<T> {
  // Perform the different queries
  let currentUrl = url
  while (currentUrl) {
    const metricLabels = contentServerMetricLabels(currentUrl)
    const { end: stopTimer } = components.metrics?.startTimer(responseTimeMetric) || { end: () => {} }
    const partialHistory: any = await fetchJson(currentUrl, components.fetcher)
    stopTimer({ ...metricLabels })

    for (const elem of selector(partialHistory)) {
      yield elem
    }

    if (partialHistory.pagination) {
      const nextRelative: string | void = partialHistory.pagination.next
      if (!nextRelative) break
      currentUrl = new URL(nextRelative, currentUrl).toString()
    } else {
      break
    }
  }
}

export async function* fetchPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  server: string,
  fromTimestamp: number,
  logger: ILoggerComponent.ILogger
): AsyncIterable<PointerChangesSyncDeployment> {
  const url = new URL(
    `${server}/pointer-changes?sortingOrder=ASC&sortingField=local_timestamp&from=${encodeURIComponent(fromTimestamp)}`
  ).toString()
  for await (const deployment of fetchJsonPaginated(
    components,
    url,
    ($) => $.deltas,
    'dcl_catalysts_pointer_changes_response_time_seconds'
  )) {
    if (PointerChangesSyncDeployment.validate(deployment)) {
      yield deployment
    } else {
      logger.error('ERROR: Invalid entity deployment from /pointer-changes', {
        deployment: JSON.stringify(deployment),
        error: JSON.stringify(PointerChangesSyncDeployment.validate.errors)
      })
    }
  }
}

export async function saveContentFileToDisk(
  components: Pick<SnapshotsFetcherComponents, 'storage'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  server: string,
  hash: string,
  destinationFilename: string
) {
  const url = new URL(`${server}/contents/${hash}`).toString()

  return saveContentFile(components, url, destinationFilename, hash)
}

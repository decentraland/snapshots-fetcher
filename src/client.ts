import { DeploymentWithAuthChain } from '@dcl/schemas'
import { metricsDefinitions } from './metrics'
import { SnapshotsFetcherComponents } from './types'
import { contentServerMetricLabels, fetchJson, saveContentFileToDisk as saveContentFile } from './utils'

export async function getSnapshots(components: SnapshotsFetcherComponents, server: string, retries: number):
  Promise<{
    hash: string,
    lastIncludedDeploymentTimestamp: number
    replacedSnapshotHashes?: string[]
  }[]> {
  try {
    const incrementalSnapshotsUrl = new URL(`${server}/snapshots`).toString()
    // TODO: validate response
    const newSnapshots: {
      hash: string,
      timeRange: { initTimestamp: number, endTimestamp: number },
      replacedSnapshotHashes?: string[]
    }[] = await components.downloadQueue.scheduleJobWithRetries(() => fetchJson(incrementalSnapshotsUrl, components.fetcher), 1)
    return newSnapshots
      // newest first
      .sort((s1, s2) => s2.timeRange.endTimestamp - s1.timeRange.endTimestamp)
      .map(newSnapshot => ({
        hash: newSnapshot.hash,
        lastIncludedDeploymentTimestamp: newSnapshot.timeRange.endTimestamp,
        replacedSnapshotHashes: newSnapshot.replacedSnapshotHashes
      }))
  } catch {
    components.logs.getLogger('snapshots-fetcher')
      .info(`Error fetching new snapshots from ${server}. Will continue to fetch old one.`)
  }

  const globalSnapshotUrl = new URL(`${server}/snapshot`).toString()
  return [await components.downloadQueue.scheduleJobWithRetries(() => fetchJson(globalSnapshotUrl, components.fetcher), retries)]
}

export async function* fetchJsonPaginated<T>(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'metrics'>,
  url: string,
  selector: (responseBody: any) => T[],
  responseTimeMetric: keyof typeof metricsDefinitions
): AsyncIterable<T> {
  // Perform the different queries
  let currentUrl = url
  while (currentUrl) {
    const metricLabels = contentServerMetricLabels(currentUrl)
    const { end: stopTimer } = components.metrics.startTimer(responseTimeMetric)
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

export function fetchPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'metrics'>,
  server: string,
  fromTimestamp: number
): AsyncIterable<DeploymentWithAuthChain> {
  const url = new URL(
    `${server}/pointer-changes?sortingOrder=ASC&sortingField=local_timestamp&from=${encodeURIComponent(fromTimestamp)}`
  ).toString()
  return fetchJsonPaginated(components, url, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds')
}

export async function saveContentFileToDisk(
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
  server: string,
  hash: string,
  destinationFilename: string
) {
  const url = new URL(`${server}/contents/${hash}`).toString()

  return saveContentFile(components, url, destinationFilename, hash)
}

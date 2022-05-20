import { DeploymentWithAuthChain } from '@dcl/schemas'
import { metricsDefinitions } from './metrics'
import { SnapshotsFetcherComponents } from './types'
import { contentServerMetricLabels, fetchJson, saveContentFileToDisk as saveContentFile } from './utils'

export async function getGlobalSnapshot(components: SnapshotsFetcherComponents, server: string, retries: number) {
  const url = new URL(`${server}/snapshot`).toString()

  // TODO: validate response
  return await components.downloadQueue.scheduleJobWithRetries(() => fetchJson(url, components.fetcher), retries)
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

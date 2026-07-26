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

// Cap how many invalid snapshot entries we log per response, so a server returning many of them
// (the body can be up to MAX_JSON_RESPONSE_SIZE_IN_BYTES) can't flood the logs.
const MAX_INVALID_SNAPSHOT_LOGS = 100

// Every request this module makes must be bounded. The fetch component applies no default timeout,
// so without an explicit one a server that accepts the connection and then stops responding leaves
// the promise pending forever — and a pending promise never reaches the exponential-falloff retry
// that is supposed to reconnect, so the whole sync stream for that server stalls silently.
const REQUEST_TIMEOUT_IN_MS = 15_000

// Backstop on how many pages a single paginated call will follow. A server that keeps advertising a
// `next` link makes the loop run forever (measured: ~670 requests/second), which silently pins the
// sync stream on one server. Set far above any legitimate page count so it only ever trips on a
// server that is broken or hostile.
// Still far above any legitimate page count (at ~1000 entries per page this is 10M deployments in one
// poll) but low enough that the visited-URL set stays small even against deliberately long links.
const MAX_PAGES_PER_PAGINATED_CALL = 10_000
// Conventional URL length ceiling. Bounds what a server can make us retain per page.
const MAX_PAGINATION_LINK_LENGTH = 2048

// Snapshot metadata comes from untrusted servers; keep only entries with the shape we rely on
// (valid content hash + numeric time range) so a malformed response can't break downstream logic.
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
        snapshot.replacedSnapshotHashes.every((hash: any) => isValidContentHash(hash))))
  )
  // Deliberately NOT validating numberOfEntities / generationTimestamp. Nothing in this package reads
  // them, so a wrong type cannot hurt us — but rejecting the entry over one would drop the whole
  // snapshot, and a server that reports `numberOfEntities: "5"` would silently stop being synced from.
  // Validating a field we never use can only cause harm here.
}

export async function getSnapshots(
  components: SnapshotsFetcherComponents,
  server: string,
  retries: number
): Promise<SnapshotMetadata[]> {
  const logger = components.logs.getLogger('getSnapshots')
  const incrementalSnapshotsUrl = new URL(`${server}/snapshots`).toString()
  const response = await components.downloadQueue.scheduleJobWithRetries(
    () => fetchJson(incrementalSnapshotsUrl, components.fetcher, { timeout: REQUEST_TIMEOUT_IN_MS }),
    retries
  )

  if (!Array.isArray(response)) {
    throw new Error(`Invalid /snapshots response from ${server}: expected an array`)
  }

  const validSnapshots: SnapshotMetadata[] = []
  let invalidSnapshots = 0
  for (const snapshot of response) {
    if (isValidSnapshotMetadata(snapshot)) {
      validSnapshots.push(snapshot)
      continue
    }
    invalidSnapshots++
    if (invalidSnapshots <= MAX_INVALID_SNAPSHOT_LOGS) {
      logger.error('Ignoring invalid snapshot metadata received from server', {
        server,
        snapshot: JSON.stringify(snapshot)
      })
    }
  }
  if (invalidSnapshots > MAX_INVALID_SNAPSHOT_LOGS) {
    logger.error('Ignored additional invalid snapshot metadata entries from server', {
      server,
      total: String(invalidSnapshots)
    })
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
  // Every page a paginated call has already fetched. A server that points `next` back at a page it
  // already served would otherwise cycle forever; this catches that immediately instead of waiting
  // for the page cap.
  const visitedUrls = new Set<string>()

  while (currentUrl) {
    if (visitedUrls.has(currentUrl)) {
      throw new Error(`Pagination loop while fetching ${url}: ${currentUrl} was already fetched`)
    }
    if (visitedUrls.size >= MAX_PAGES_PER_PAGINATED_CALL) {
      throw new Error(`Too many pages while fetching ${url}: stopped after ${MAX_PAGES_PER_PAGINATED_CALL}`)
    }
    visitedUrls.add(currentUrl)

    const metricLabels = contentServerMetricLabels(currentUrl)
    const { end: stopTimer } = components.metrics?.startTimer(responseTimeMetric) || { end: () => {} }
    let partialHistory: any
    try {
      partialHistory = await fetchJson(currentUrl, components.fetcher, { timeout: REQUEST_TIMEOUT_IN_MS })
    } finally {
      // Stop the timer even when the request fails, so a failed page can't leak a running timer.
      stopTimer({ ...metricLabels })
    }

    // The body is remote and untrusted: a bare `null` document, or a page whose element list is not an
    // array, would otherwise surface as an opaque TypeError from the destructuring below.
    if (!partialHistory || typeof partialHistory !== 'object') {
      throw new Error(`Invalid paginated response from ${currentUrl}: expected a JSON object`)
    }
    const elements = selector(partialHistory)
    if (!Array.isArray(elements)) {
      throw new Error(`Invalid paginated response from ${currentUrl}: expected an array of elements`)
    }

    for (const elem of elements) {
      yield elem
    }

    if (partialHistory.pagination) {
      const nextRelative: unknown = partialHistory.pagination.next
      if (!nextRelative || typeof nextRelative !== 'string') break
      // `next` is remote text: an unparseable value would otherwise surface as an opaque TypeError
      // from the URL constructor, and an enormous one would be retained in visitedUrls for the rest of
      // the call.
      if (nextRelative.length > MAX_PAGINATION_LINK_LENGTH) {
        throw new Error(
          `Invalid pagination link while fetching ${url}: longer than ${MAX_PAGINATION_LINK_LENGTH} characters`
        )
      }
      let nextUrl: URL
      try {
        nextUrl = new URL(nextRelative, currentUrl)
      } catch {
        throw new Error(`Invalid pagination link while fetching ${url}: ${JSON.stringify(nextRelative)}`)
      }
      // `next` is chosen by the remote server, so an absolute URL here would let it steer our
      // requests at any host it likes — internal addresses and cloud metadata endpoints included —
      // using this process as the client. Pagination never legitimately leaves the server that
      // issued it, so anything cross-origin is rejected rather than followed.
      if (nextUrl.origin !== new URL(currentUrl).origin) {
        throw new Error(
          `Refusing to follow a cross-origin pagination link while fetching ${url}: ${nextUrl.origin} does not match ${
            new URL(currentUrl).origin
          }`
        )
      }
      currentUrl = nextUrl.toString()
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

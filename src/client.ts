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

/**
 * A remote timestamp we are willing to do arithmetic with.
 *
 * `typeof x === 'number'` is not enough. A JSON body cannot carry NaN — `{"a":NaN}` is a syntax error —
 * but it CAN carry Infinity, because the JSON number grammar has no range limit and a large exponent
 * (`1e999`) parses to it. An infinite endTimestamp becomes the server's high-water mark, which
 * `Math.max` can then never advance past, so it polls `from=Infinity` for the rest of the process's
 * life. Negative values are nonsense for an epoch timestamp.
 *
 * Deliberately not requiring an integer: fractional milliseconds would be odd but harmless, and
 * rejecting an entry drops the whole snapshot — which silently stops us syncing from that server.
 */
function isUsableTimestamp(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
}

// Snapshot metadata comes from untrusted servers; keep only entries with the shape we rely on
// (valid content hash + usable time range) so a malformed response can't break downstream logic.
function isValidSnapshotMetadata(snapshot: any): snapshot is SnapshotMetadata {
  return (
    !!snapshot &&
    typeof snapshot.hash === 'string' &&
    isValidContentHash(snapshot.hash) &&
    !!snapshot.timeRange &&
    isUsableTimestamp(snapshot.timeRange.initTimestamp) &&
    isUsableTimestamp(snapshot.timeRange.endTimestamp) &&
    // An inverted range is malformed, and it is handed straight to the deployer's warm-up.
    snapshot.timeRange.initTimestamp <= snapshot.timeRange.endTimestamp &&
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
  // Parsed once, and every `next` is validated against THIS rather than against the previous page, so
  // no chain of individually-permitted hops can walk away from the endpoint the call started from.
  const requestUrl = new URL(url)
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
      // `next` is chosen by the remote server, and this process is the client that would follow it, so
      // it is pinned to the endpoint the call started from and may vary only the query string — which
      // is all pagination legitimately needs (catalysts return `?from=…&entityId=…`).
      //
      // Rejecting cross-origin links stops it naming another host outright. That alone is not the whole
      // story: a URL origin is a hostname, not an address, so a hostile host can serve page 1 from a
      // public IP and rebind the name to loopback or 169.254.169.254 before page 2 while the origin
      // still matches. That rebinding cannot be closed here — `IFetchComponent` exposes no DNS `lookup`
      // hook, the same reason fetchJson refuses redirects outright — and it applies to the first request
      // of every poll regardless of pagination. What holding the path fixed removes is the part that IS
      // ours to control: without it a rebound request would carry a server-chosen path (an internal
      // admin endpoint); with it, the most a rebound address can be asked for is the very endpoint we
      // were going to request anyway.
      if (nextUrl.origin !== requestUrl.origin) {
        throw new Error(
          `Refusing to follow a cross-origin pagination link while fetching ${url}: ${nextUrl.origin} does not match ${requestUrl.origin}`
        )
      }
      if (nextUrl.pathname !== requestUrl.pathname) {
        throw new Error(
          `Refusing to follow a pagination link that changes the path while fetching ${url}: ${nextUrl.pathname} does not match ${requestUrl.pathname}`
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

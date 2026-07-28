import { PointerChangesSyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { metricsDefinitions } from './metrics'
import { SnapshotMetadata, SnapshotsFetcherComponents } from './types'
import {
  contentServerMetricLabels,
  fetchJson,
  isUsableTimestamp,
  isValidContentHash,
  truncateForLog,
  saveContentFileToDisk as saveContentFile
} from './utils'

// Cap how many invalid snapshot entries we log per response, so a server returning many of them
// (the body can be up to MAX_JSON_RESPONSE_SIZE_IN_BYTES) can't flood the logs.
const MAX_INVALID_SNAPSHOT_LOGS = 100

// The same bound for deltas we refuse from /pointer-changes. A server can put one on every page, and
// MAX_PAGES_PER_PAGINATED_CALL is 10,000.
const MAX_REJECTED_DELTA_LOGS = 100

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

// Ceiling on the snapshots one snapshot may claim to replace. Every entry ends up in the batched
// processed-snapshots lookup — one large `IN` clause against the consumer's storage — so an entry with a
// pathological list sizes that query. Real snapshots replace tens (a daily replaces its hours, a monthly
// its days), so this only trips on a broken or hostile server.
//
// Note this bounds a single entry, not the aggregate: a server can still spread hashes across many
// snapshots, and the real ceiling on the total is MAX_JSON_RESPONSE_SIZE_IN_BYTES. Chunking the lookup is
// what would bound the query itself.
const MAX_REPLACED_SNAPSHOT_HASHES = 1000

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
        snapshot.replacedSnapshotHashes.length <= MAX_REPLACED_SNAPSHOT_HASHES &&
        snapshot.replacedSnapshotHashes.every((hash: any) => isValidContentHash(hash))))
  )
  // Deliberately NOT validating numberOfEntities / generationTimestamp. Nothing in this package reads
  // them, so a wrong type cannot hurt us — but rejecting the entry over one would drop the whole
  // snapshot, and a server that reports `numberOfEntities: "5"` would silently stop being synced from.
  // Validating a field we never use can only cause harm here.
}

/**
 * A server's snapshot list, together with whether we had to discard anything from it.
 *
 * The count matters as much as the list. Each snapshot stands for a whole time range, so a discarded
 * entry is a range nothing else covers: treating the surviving subset as the server's complete history
 * would advance it past those entities and never revisit them. Callers that record sync progress must
 * check this rather than only reading `snapshots`.
 */
export type SnapshotsFromServer = {
  snapshots: SnapshotMetadata[]
  discardedEntries: number
}

export async function getSnapshots(
  components: SnapshotsFetcherComponents,
  server: string,
  retries: number
): Promise<SnapshotsFromServer> {
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
        snapshot: truncateForLog(JSON.stringify(snapshot))
      })
    }
  }
  if (invalidSnapshots > MAX_INVALID_SNAPSHOT_LOGS) {
    logger.error('Ignored additional invalid snapshot metadata entries from server', {
      server,
      total: String(invalidSnapshots)
    })
  }

  return {
    // newest first
    snapshots: validSnapshots.sort((s1, s2) => s2.timeRange.endTimestamp - s1.timeRange.endTimestamp),
    discardedEntries: invalidSnapshots
  }
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
  // Bounded like the snapshot-file reader's, and for the same reason: a server can put a rejectable delta
  // on every page, and the page cap is 10,000. Truncating each entry is not enough on its own when the
  // number of entries is also attacker-chosen.
  let rejectedDeltasLogged = 0
  function reportRejectedDelta(message: string, buildExtra: () => Record<string, string>) {
    if (rejectedDeltasLogged >= MAX_REJECTED_DELTA_LOGS) {
      return
    }
    rejectedDeltasLogged++
    logger.error(message, buildExtra())
    if (rejectedDeltasLogged === MAX_REJECTED_DELTA_LOGS) {
      logger.error('Too many rejected deltas from /pointer-changes, suppressing further ones', {
        server,
        suppressedAfter: String(MAX_REJECTED_DELTA_LOGS)
      })
    }
  }

  for await (const deployment of fetchJsonPaginated(
    components,
    url,
    ($) => $.deltas,
    'dcl_catalysts_pointer_changes_response_time_seconds'
  )) {
    if (!PointerChangesSyncDeployment.validate(deployment)) {
      reportRejectedDelta('ERROR: Invalid entity deployment from /pointer-changes', () => ({
        deployment: truncateForLog(JSON.stringify(deployment)),
        error: JSON.stringify(PointerChangesSyncDeployment.validate.errors)
      }))
      continue
    }
    // The schema is not enough. It rejects Infinity and negatives but bounds nothing above, so a
    // far-future, 1e308, above-2^53 or fractional localTimestamp is schema-valid — and localTimestamp is
    // exactly what markAsDeployed feeds to increaseLastTimestamp. One such delta permanently pins the
    // server's high-water mark at a point no real deployment can exceed, and it then polls from an
    // impossible `from=` forever. Same hazard the snapshot time ranges are checked for.
    //
    // Skipped rather than fatal, unlike a malformed snapshot: a delta is one entity, and failing the
    // stream on it would let a single permanently-broken record stall every later deployment from that
    // server. A snapshot covers a whole range, so there dropping it silently is the worse trade.
    if (!isUsableTimestamp(deployment.localTimestamp) || !isUsableTimestamp(deployment.entityTimestamp)) {
      reportRejectedDelta('ERROR: Implausible timestamp in entity deployment from /pointer-changes', () => ({
        deployment: truncateForLog(JSON.stringify(deployment)),
        localTimestamp: String(deployment.localTimestamp),
        entityTimestamp: String(deployment.entityTimestamp)
      }))
      continue
    }
    yield deployment
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

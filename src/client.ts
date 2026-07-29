import { PointerChangesSyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { metricsDefinitions } from './metrics'
import { assertPointerChangesDeploymentWithinStructuralLimits } from './pointer-changes-limits'
import { SnapshotMetadata, SnapshotsFetcherComponents, TransferLimits } from './types'
import {
  contentServerMetricLabels,
  fetchJson,
  isUsableTimestamp,
  isValidContentHash,
  resolveTransferLimits,
  sanitizeUrlForLog,
  truncateForLog,
  saveContentFileToDisk as saveContentFile
} from './utils'

// Cap how many invalid snapshot entries we log per response, so a server returning many of them
// (the body can be up to MAX_JSON_RESPONSE_SIZE_IN_BYTES) can't flood the logs.
const MAX_INVALID_SNAPSHOT_LOGS = 100

// The same bound for deltas we refuse from /pointer-changes. A server can put one on every page, and
// the page cap (transferLimits.maxPagesPerPaginatedCall) is 10,000 by default.
const MAX_REJECTED_DELTA_LOGS = 100

// Every request this module makes must be bounded. The fetch component applies no default timeout, so
// without an explicit one a server that accepts the connection and then stops responding leaves the
// promise pending forever — and a pending promise never reaches the exponential-falloff retry that is
// supposed to reconnect, so the whole sync stream for that server stalls silently. The bound itself is
// `transferLimits.requestTimeoutInMs`, defaulting to the 15s this module always used.

// Backstop on how many pages a single paginated call will follow. A server that keeps advertising a
// `next` link makes the loop run forever (measured: ~670 requests/second), which silently pins the
// sync stream on one server. The bound is `transferLimits.maxPagesPerPaginatedCall`, defaulting to the
// 10,000 this module always used: far above any legitimate page count (at ~1000 entries per page that is
// 10M deployments in one poll) but low enough that the visited-URL set stays small even against
// deliberately long links. Lower it to cap the amplification one poll can produce more tightly.
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
  retries: number,
  transferLimits?: TransferLimits
): Promise<SnapshotsFromServer> {
  const logger = components.logs.getLogger('getSnapshots')
  const limits = resolveTransferLimits(transferLimits)
  const incrementalSnapshotsUrl = new URL(`${server}/snapshots`).toString()
  const response = await components.downloadQueue.scheduleJobWithRetries(
    () =>
      fetchJson(incrementalSnapshotsUrl, components.fetcher, {
        timeout: limits.requestTimeoutInMs,
        transferLimits
      }),
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
  responseTimeMetric: keyof typeof metricsDefinitions,
  transferLimits?: TransferLimits
): AsyncIterable<T> {
  // Perform the different queries
  let currentUrl = url
  // Parsed once, and every `next` is validated against THIS rather than against the previous page, so
  // no chain of individually-permitted hops can walk away from the endpoint the call started from.
  const requestUrl = new URL(url)
  // The initial url is normalised exactly as every `next` is below, so the visited-URL rule reads the
  // same for page 1 as for the rest: a caller-supplied fragment is just as meaningless to the server,
  // and leaving one on would let page 1 and a fragmentless `next` to the same resource count as two
  // distinct pages. Re-serialised only when there is a fragment to drop, so a url without one is passed
  // through byte for byte. Internal callers build fragmentless urls, so today this is consistency
  // rather than a live bug.
  if (requestUrl.hash) {
    requestUrl.hash = ''
    currentUrl = requestUrl.toString()
  }
  // Every page a paginated call has already fetched. A server that points `next` back at a page it
  // already served would otherwise cycle forever; this catches that immediately instead of waiting
  // for the page cap.
  const visitedUrls = new Set<string>()
  const limits = resolveTransferLimits(transferLimits)

  while (currentUrl) {
    if (visitedUrls.has(currentUrl)) {
      throw new Error(
        `Pagination loop while fetching ${sanitizeUrlForLog(url)}: ${sanitizeUrlForLog(currentUrl)} was already fetched`
      )
    }
    if (visitedUrls.size >= limits.maxPagesPerPaginatedCall) {
      throw new Error(
        `Too many pages while fetching ${sanitizeUrlForLog(url)}: stopped after ${limits.maxPagesPerPaginatedCall}`
      )
    }
    visitedUrls.add(currentUrl)

    const metricLabels = contentServerMetricLabels(currentUrl)
    const { end: stopTimer } = components.metrics?.startTimer(responseTimeMetric) || { end: () => {} }
    let partialHistory: any
    try {
      partialHistory = await fetchJson(currentUrl, components.fetcher, {
        timeout: limits.requestTimeoutInMs,
        transferLimits
      })
    } finally {
      // Stop the timer even when the request fails, so a failed page can't leak a running timer.
      stopTimer({ ...metricLabels })
    }

    // The body is remote and untrusted: a bare `null` document, or a page whose element list is not an
    // array, would otherwise surface as an opaque TypeError from the destructuring below.
    if (!partialHistory || typeof partialHistory !== 'object') {
      throw new Error(`Invalid paginated response from ${sanitizeUrlForLog(currentUrl)}: expected a JSON object`)
    }
    const elements = selector(partialHistory)
    if (!Array.isArray(elements)) {
      throw new Error(`Invalid paginated response from ${sanitizeUrlForLog(currentUrl)}: expected an array of elements`)
    }

    for (const elem of elements) {
      yield elem
    }

    const pagination: unknown = partialHistory.pagination
    // Absent or null means an unpaginated response, which is a legitimate way for a feed to end.
    if (pagination !== undefined && pagination !== null) {
      // A truthy non-object was previously waved through by the `if`: reading `.next` off `true` or
      // `"more"` yields undefined, which then ended the feed as though the server had said there was
      // nothing more. Same hazard as a wrong-typed `next` — a container we cannot read must not be read
      // as an ending — so the shape is checked before the link inside it. An array is not a container
      // either, despite being typeof 'object'.
      if (typeof pagination !== 'object' || Array.isArray(pagination)) {
        throw new Error(
          `Invalid pagination while fetching ${sanitizeUrlForLog(url)}: expected an object, got ${
            Array.isArray(pagination) ? 'an array' : typeof pagination
          }`
        )
      }
      const nextRelative: unknown = (pagination as { next?: unknown }).next
      // Absent, null or empty means "no next page", which is how a feed legitimately ends.
      if (nextRelative === undefined || nextRelative === null || nextRelative === '') {
        break
      }
      // Anything else that is not a string is a malformed response, not an ending. Treating it as an
      // ending made a truncated feed indistinguishable from a complete one, and it was the only
      // malformed shape here that did not fail: too long, unparseable, cross-origin and path-changing
      // links all throw. A server that says "there is more" in a way we cannot read should not have that
      // read as "there is no more".
      if (typeof nextRelative !== 'string') {
        throw new Error(
          `Invalid pagination link while fetching ${sanitizeUrlForLog(
            url
          )}: expected a string, got ${typeof nextRelative}`
        )
      }
      // `next` is remote text: an unparseable value would otherwise surface as an opaque TypeError
      // from the URL constructor, and an enormous one would be retained in visitedUrls for the rest of
      // the call.
      if (nextRelative.length > MAX_PAGINATION_LINK_LENGTH) {
        throw new Error(
          `Invalid pagination link while fetching ${sanitizeUrlForLog(
            url
          )}: longer than ${MAX_PAGINATION_LINK_LENGTH} characters`
        )
      }
      let nextUrl: URL
      try {
        nextUrl = new URL(nextRelative, currentUrl)
      } catch {
        throw new Error(
          `Invalid pagination link while fetching ${sanitizeUrlForLog(url)}: ${truncateForLog(
            JSON.stringify(nextRelative)
          )}`
        )
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
          `Refusing to follow a cross-origin pagination link while fetching ${sanitizeUrlForLog(url)}: ` +
            `${nextUrl.origin} does not match ${requestUrl.origin}`
        )
      }
      if (nextUrl.pathname !== requestUrl.pathname) {
        throw new Error(
          `Refusing to follow a pagination link that changes the path while fetching ${sanitizeUrlForLog(url)}: ` +
            `${truncateForLog(nextUrl.pathname)} does not match ${requestUrl.pathname}`
        )
      }
      // Fragments are never sent to a server, so two links differing only by `#…` address the same
      // network resource — but as distinct strings they slipped past the visited-URL check, letting a feed
      // re-fetch one page up to the page cap. Cleared rather than rejected: a
      // fragment is meaningless here rather than malformed, and normalising costs nothing.
      nextUrl.hash = ''
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
  logger: ILoggerComponent.ILogger,
  transferLimits?: TransferLimits
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
    'dcl_catalysts_pointer_changes_response_time_seconds',
    transferLimits
  )) {
    if (!PointerChangesSyncDeployment.validate(deployment)) {
      reportRejectedDelta('ERROR: Invalid entity deployment from /pointer-changes', () => ({
        deployment: truncateForLog(JSON.stringify(deployment)),
        error: JSON.stringify(PointerChangesSyncDeployment.validate.errors)
      }))
      continue
    }
    // The schema has minItems but no maximum counts or string lengths. These fields are later sorted
    // and hashed for inclusive-boundary de-duplication, so one schema-valid row must not be allowed to
    // size that work up to the whole paginated-response cap. Throwing fails the poll before its
    // high-water mark is committed; skipping would silently advance past the rejected deployment.
    assertPointerChangesDeploymentWithinStructuralLimits(deployment)
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
  destinationFilename: string,
  transferLimits?: TransferLimits
) {
  const url = new URL(`${server}/contents/${hash}`).toString()

  return saveContentFile(components, url, destinationFilename, hash, true, transferLimits)
}

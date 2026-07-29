import { hashV0, hashV1 } from '@dcl/hashing'
import { IFetchComponent, RequestOptions } from '@dcl/core-commons'
import * as crypto from 'crypto'
import { lookup as dnsLookup } from 'dns'
import * as fs from 'fs'
import * as http from 'http'
import * as https from 'https'
import * as net from 'net'
import { LookupFunction } from 'net'
import * as path from 'path'
import { Readable, Transform } from 'stream'
import { pipeline as streamPipeline } from 'stream/promises'
import * as zlib from 'zlib'
import { ContentServerMetricLabels } from './metrics'
import { Path, ResolvedTransferLimits, Server, SnapshotsFetcherComponents, TransferLimits } from './types'

// Bounds buffered JSON responses so a malicious server can't OOM the process via response.json().
const MAX_JSON_RESPONSE_SIZE_IN_BYTES = 50 * 1024 * 1024 // 50 MiB

// Fallback for a fetchJson caller that names no timeout of its own. Distinct from
// `requestTimeoutInMs` below, which is what this package's own JSON calls pass: collapsing the two would
// quietly halve this one.
const DEFAULT_BODY_READ_TIMEOUT_IN_MS = 30_000

// The values this package used before these became configurable, so an omitted `transferLimits` — or an
// omitted field within it — behaves exactly as before.
//
// requestTimeoutInMs: an *inactivity* deadline refreshed per chunk, not a total one, so a large
// response can legitimately outlast it while making steady progress.
//
// minTransferRateInBytesPerSecond: the companion to that deadline. Refreshing per chunk means the
// deadline only ever asks "did a byte arrive recently?", which a peer trickling one byte per window
// answers yes to forever while holding its slot; the size caps are then the only other bound, and at
// that rate 1 GiB is geological. The floor is what makes staying connected cost real bandwidth. Set far
// below any usable link (4 KiB/s is worse than a 56k modem) rather than near real throughput: it exists
// to catch transfers that are not really transferring, and downloads retry, so a false positive costs
// an attempt rather than the entity.
//
// transferRateGracePeriodInMs: small responses finish well inside it, and earlier samples are dominated
// by connection setup and server think-time rather than by throughput.
export const DEFAULT_TRANSFER_LIMITS: ResolvedTransferLimits = {
  requestTimeoutInMs: 15_000,
  downloadInactivityTimeoutInMs: 30_000,
  maxDownloadedFileSizeInBytes: 1024 * 1024 * 1024, // 1 GiB
  minTransferRateInBytesPerSecond: 4 * 1024,
  transferRateGracePeriodInMs: 60_000,
  maxPagesPerPaginatedCall: 10_000
}

// Zero is meaningful for these two and not for the others: a rate floor of 0 disables the check, and a
// grace period of 0 judges from the first chunk. A timeout or size cap of 0 would reject every transfer.
const TRANSFER_LIMIT_MINIMUMS: ResolvedTransferLimits = {
  requestTimeoutInMs: 1,
  downloadInactivityTimeoutInMs: 1,
  maxDownloadedFileSizeInBytes: 1,
  minTransferRateInBytesPerSecond: 0,
  transferRateGracePeriodInMs: 0,
  maxPagesPerPaginatedCall: 1
}

/**
 * Fills in {@link DEFAULT_TRANSFER_LIMITS} for anything the caller omitted, and rejects values that
 * would silently disable a bound rather than tune it.
 *
 * Validated here rather than at each point of use so a misconfiguration surfaces once, at construction,
 * instead of as a puzzling per-download failure.
 */
export function resolveTransferLimits(limits?: TransferLimits): ResolvedTransferLimits {
  // Only *defined* fields override a default. A plain spread would let `{ requestTimeoutInMs: undefined }`
  // overwrite the default with undefined and then fail validation, which punishes the common case of
  // config assembled from env vars or optional options where an absent value arrives as an explicit
  // undefined rather than a missing key.
  const resolved = { ...DEFAULT_TRANSFER_LIMITS }
  for (const [name, value] of Object.entries(limits ?? {})) {
    if (value !== undefined) {
      resolved[name as keyof ResolvedTransferLimits] = value as number
    }
  }
  for (const name of Object.keys(DEFAULT_TRANSFER_LIMITS) as Array<keyof ResolvedTransferLimits>) {
    const value = resolved[name]
    const minimum = TRANSFER_LIMIT_MINIMUMS[name]
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw new Error(`transferLimits.${name} must be an integer >= ${minimum}, got ${value}`)
    }
  }
  return resolved
}

/**
 * Companion to the per-chunk inactivity deadlines: they check that bytes are still arriving, this
 * checks that the bytes add up to progress. Returns the error to fail with, or undefined to continue.
 */
export function tooSlowToContinue(
  bytesSoFar: number,
  startedAt: number,
  limits: ResolvedTransferLimits
): Error | undefined {
  if (limits.minTransferRateInBytesPerSecond === 0) {
    return undefined
  }
  const elapsed = Date.now() - startedAt
  if (elapsed <= limits.transferRateGracePeriodInMs) {
    return undefined
  }
  const bytesPerSecond = (bytesSoFar * 1000) / elapsed
  if (bytesPerSecond >= limits.minTransferRateInBytesPerSecond) {
    return undefined
  }
  // The byte total is part of the message because the rate alone rounds a trickle and a dead silence
  // to the same "0.0 bytes/s", and those are different problems to go looking for.
  return new Error(
    `Transfer of ${bytesSoFar} bytes averaged ${bytesPerSecond.toFixed(2)} bytes/s over ` +
      `${Math.round(elapsed / 1000)}s, below the minimum of ${limits.minTransferRateInBytesPerSecond} bytes/s`
  )
}

// Reads a response body while enforcing a maximum size. The native fetcher (unlike node-fetch) has
// no `size` option, so we cap manually: read the stream with a running byte count and abort —
// cancelling the stream to free its socket — if it exceeds the limit.
async function readBodyWithSizeLimit(
  response: Response,
  maxBytes: number,
  timeoutMs: number,
  limits: ResolvedTransferLimits
): Promise<string> {
  const reader = response.body?.getReader()
  if (!reader) {
    return ''
  }

  const chunks: Uint8Array[] = []
  let total = 0
  const startedAt = Date.now()
  // The fetch component's timeout only covers time-to-headers: it clears its abort timer as soon as the
  // response object exists. Without a deadline here a server can send headers, write half a document
  // and then go silent forever — exactly the stall the request timeout was meant to prevent, and a
  // pending promise never reaches the reconnection logic.
  //
  // An *inactivity* deadline, refreshed on every chunk, not a total one: a large response (the cap is
  // 50 MiB) can legitimately take longer than one request timeout to stream, and a total deadline would
  // abort a slow-but-healthy content server that is making steady progress. This mirrors
  // DOWNLOAD_INACTIVITY_TIMEOUT_MS on the file-download path.
  let timedOut = false
  let timeout: NodeJS.Timeout | undefined
  const refreshInactivityTimeout = () => {
    if (timeout) clearTimeout(timeout)
    timeout = setTimeout(() => {
      timedOut = true
      void reader.cancel().catch(() => undefined)
    }, timeoutMs)
  }
  const timedOutError = () => new Error(`Timed out after ${timeoutMs}ms without receiving response body data`)

  refreshInactivityTimeout()

  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) {
        refreshInactivityTimeout()
        total += value.byteLength
        if (total > maxBytes) {
          throw new Error(`Response body exceeds the maximum allowed size of ${maxBytes} bytes`)
        }
        const tooSlow = tooSlowToContinue(total, startedAt, limits)
        if (tooSlow) {
          throw tooSlow
        }
        chunks.push(value)
      }
    }
  } catch (e) {
    throw timedOut ? timedOutError() : e
  } finally {
    if (timeout) clearTimeout(timeout)
    // Frees the socket on the size-exceeded path; a no-op once the stream has been fully read.
    await reader.cancel().catch(() => undefined)
  }

  if (timedOut) {
    throw timedOutError()
  }

  return Buffer.concat(chunks).toString('utf8')
}

export async function fetchJson(
  url: string,
  fetcher: IFetchComponent,
  init?: RequestOptions & {
    /** Bounds for this request's body read. Resolved from the defaults when omitted. */
    transferLimits?: TransferLimits
  }
): Promise<any> {
  const { transferLimits, ...requestInit } = init ?? {}
  const limits = resolveTransferLimits(transferLimits)
  const bodyReadTimeout = requestInit.timeout ?? DEFAULT_BODY_READ_TIMEOUT_IN_MS

  // JSON endpoints do not follow redirects at all.
  //
  // The fetch implementation's default ('follow') chases any host the server names, which made every
  // JSON endpoint an SSRF primitive. Following same-origin redirects here instead was not enough: a URL
  // origin is a hostname, not an address, so a hostile host can serve the first response from a public
  // IP, return `Location: /next`, and rebind that same hostname to loopback or 169.254.169.254 before
  // the next request — the origin still matches and the check passes.
  //
  // The download path defends against that with a DNS `lookup` that classifies and pins resolved
  // addresses, but `IFetchComponent` exposes no way to inject one: the consumer supplies the fetcher,
  // and native fetch resolves DNS itself. Any check performed here would be a separate resolution from
  // the one the request actually uses, i.e. defeated by the same race it claims to close. Refusing
  // redirects is the only complete answer available on this path.
  //
  // No known catalyst JSON endpoint redirects. If one legitimately must, the fix is a redirect-aware
  // fetch component, not a check here that cannot bind to the request's own resolution.
  const response = await fetcher.fetch(url, { ...requestInit, redirect: 'manual' })

  if (response.status >= 300 && response.status < 400) {
    await response.body?.cancel().catch(() => undefined)
    throw new Error(
      `Refusing to follow a redirect while fetching ${sanitizeUrlForLog(url)}: got status ${
        response.status
      } to ${truncateForLog(JSON.stringify(response.headers.get('location')))}`
    )
  }

  if (!response.ok) {
    // Drain the body so undici releases the socket back to the pool before throwing.
    await response.body?.cancel().catch(() => undefined)
    throw new Error('Error fetching ' + sanitizeUrlForLog(url) + '. Status code was: ' + response.status)
  }

  const body = await readBodyWithSizeLimit(response, MAX_JSON_RESPONSE_SIZE_IN_BYTES, bodyReadTimeout, limits)
  if (body === '') {
    throw new Error('Error fetching ' + sanitizeUrlForLog(url) + '. The response body was empty.')
  }

  return JSON.parse(body)
}

export async function checkFileExists(file: string): Promise<boolean> {
  return fs.promises
    .access(file, fs.constants.F_OK)
    .then(() => true)
    .catch(() => false)
}

export async function sleep(time: number): Promise<void> {
  if (time <= 0) return
  return new Promise<void>((resolve) => setTimeout(resolve, time))
}

// How often an interruptible sleep samples its stop predicate.
const STOP_SIGNAL_SAMPLE_INTERVAL_IN_MS = 50

/**
 * sleep(), but returns early once `shouldStop()` becomes true.
 *
 * Every waiting period between attempts or polls is otherwise added to shutdown latency: callers now
 * await the running work rather than merely signalling it, so a plain sleep of the poll interval or the
 * retry backoff is time a caller's stop() spends blocked. The signal is a predicate rather than an
 * event, so it is sampled; at these intervals that is far cheaper than the wait it replaces.
 */
export async function sleepUnlessStopped(time: number, shouldStop?: () => boolean): Promise<void> {
  if (!shouldStop) return sleep(time)
  if (time <= 0) return

  const deadline = Date.now() + time
  while (Date.now() < deadline) {
    if (shouldStop()) return
    await sleep(Math.min(STOP_SIGNAL_SAMPLE_INTERVAL_IN_MS, deadline - Date.now()))
  }
}

// How much of an untrusted string is worth keeping in a log entry.
const LOG_PREVIEW_LENGTH = 512

/**
 * A log-safe rendering of remote text.
 *
 * Every string a content server sends is a candidate log payload, and several are logged once per
 * offending item: a snapshot line can be 10 MiB, a snapshot metadata entry or a pointer-change delta can
 * be most of a 50 MiB response body. Logging them whole lets one bad response push gigabytes of
 * attacker-chosen text into the log pipeline. The prefix is what identifies the problem; the length is
 * what tells you it was truncated.
 */
export function truncateForLog(text: string): string {
  return text.length <= LOG_PREVIEW_LENGTH
    ? text
    : `${text.slice(0, LOG_PREVIEW_LENGTH)}… (truncated, original length ${text.length})`
}

// Content hashes are IPFS CIDs (base58/base32), hence alphanumeric. Validating against this charset
// before using a hash in a path or storage key prevents path traversal from untrusted hashes.
const VALID_CONTENT_HASH = /^[a-zA-Z0-9]+$/
export function isValidContentHash(hash: string): boolean {
  return typeof hash === 'string' && hash.length > 0 && hash.length <= 128 && VALID_CONTENT_HASH.test(hash)
}

// How far ahead of our own clock a remote timestamp may sit. Snapshots describe history that already
// happened, so anything beyond this is either the server's clock being wrong or a value we should not
// adopt as sync state.
const MAX_TIMESTAMP_CLOCK_SKEW_IN_MS = 24 * 60 * 60 * 1000

/**
 * A remote timestamp we are willing to adopt as sync state.
 *
 * These values become a server's high-water mark, and `increaseLastTimestamp` only ever moves it forward,
 * so a single bad one is permanent for the life of the process: the server then polls `/pointer-changes`
 * from a point nothing can ever reach and silently stops syncing.
 *
 * `typeof x === 'number'` does not begin to cover it, and neither does finiteness:
 *
 * - A JSON body cannot carry NaN (`{"a":NaN}` is a syntax error) but it CAN carry Infinity, because the
 *   number grammar has no range limit and `1e999` parses to it.
 * - `1e308` is finite, non-negative, and every bit as poisonous — it is not a date any deployment can
 *   exceed. Finiteness was the wrong test; being a plausible instant is the right one.
 * - Past 2^53 integer arithmetic silently stops being exact, so `Number.isSafeInteger` is the floor for a
 *   value we do `Math.max` on. It also rejects fractions, which epoch milliseconds never legitimately are
 *   — an earlier version of this deliberately allowed them, but the upper bound below makes that leniency
 *   pointless and a fractional timestamp only ever indicates a malformed server.
 */
export function isUsableTimestamp(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= 0 &&
    value <= Date.now() + MAX_TIMESTAMP_CLOCK_SKEW_IN_MS
  )
}

/**
 * Whether {@link assertHash} can verify bytes against this hash at all.
 *
 * `isValidContentHash` is deliberately looser, because it answers a different question: is this string
 * safe to use as a filesystem path and a storage key. Verification is narrower — assertHash dispatches on
 * the `Qm`/`ba` CID prefixes and can do nothing with anything else — so a hash that passes the first check
 * but not this one can only fail *after* its bytes have been fetched.
 *
 * Deliberately mirrors assertHash's own dispatch rather than a stricter CID grammar: a tighter predicate
 * here could refuse a hash assertHash would have happily verified, which would stop syncing content that
 * works today. This can only ever reject hashes that were going to fail anyway.
 */
/**
 * Makes a URL safe to put in an error or log line.
 *
 * Two problems, both of which only exist once redirects are in play: after a hop the URL is the server's
 * text rather than ours, so its length is unbounded; and a URL can carry `user:password@`, which must
 * never reach a log. Falls back to truncation alone for something unparseable, since that is already not
 * a URL we should be echoing in full.
 */
export function sanitizeUrlForLog(value: string): string {
  try {
    const parsed = new URL(value)
    parsed.username = ''
    parsed.password = ''
    return truncateForLog(parsed.toString())
  } catch {
    return truncateForLog(value)
  }
}

export function isVerifiableContentHash(hash: string): boolean {
  return hash.startsWith('Qm') || hash.startsWith('ba')
}

export async function assertHash(filename: string, hash: string) {
  if (hash.startsWith('Qm')) {
    const file = fs.createReadStream(filename)
    try {
      const qmHash = await hashV0(file as any)
      if (qmHash !== hash) {
        throw new Error(
          `Download error: hashes do not match(expected:${hash} != calculated:${qmHash}) for file ${filename}`
        )
      }
    } finally {
      file.close()
    }
  } else if (hash.startsWith('ba')) {
    const file = fs.createReadStream(filename)
    try {
      const baHash = await hashV1(file as any)
      if (baHash !== hash) {
        throw new Error(
          `Download error: hashes do not match(expected:${hash} != calculated:${baHash}) for file ${filename}`
        )
      }
    } finally {
      file.close()
    }
  } else {
    throw new Error(`Unknown hashing algorithm for hash: ${hash}`)
  }
}

export async function saveContentFileToDisk(
  components: Pick<SnapshotsFetcherComponents, 'storage'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  originalUrlString: string,
  destinationFilename: string,
  hash: string,
  checkHash: boolean = true,
  transferLimits?: TransferLimits
): Promise<void> {
  const limits = resolveTransferLimits(transferLimits)
  const tmpFolder = path.dirname(destinationFilename)
  await ensureFolderExists(tmpFolder)

  // A 128-bit random suffix on a content-addressed path: a collision needs the same hash *and* the
  // same suffix, and concurrent downloads of one hash already share a single job. Not worth an
  // existence check (a syscall) on every downloaded file.
  const tmpFileName = destinationFilename + crypto.randomBytes(16).toString('hex')

  const metricsLabels: ContentServerMetricLabels = {
    remote_server: ''
  }

  try {
    await downloadFile(originalUrlString, metricsLabels, components, tmpFileName, limits)

    // make files not executable
    await fs.promises.chmod(tmpFileName, 0o644)

    // check hash if present. delete file and fail in case of mismatch
    if (checkHash) {
      try {
        await assertHash(tmpFileName, hash)
      } catch (e) {
        components.metrics?.increment('dcl_content_download_hash_errors_total', metricsLabels)
        // delete the downloaded file if failed
        await deleteFileIfPresent(tmpFileName)
        throw e
      }
    }

    // move downloaded file to target folder
    const storedStream = fs.createReadStream(tmpFileName)
    // storage is supplied by the consumer. A component that resolves storeStream without consuming the
    // stream leaves it to open the temp file after the finally below has removed it, and an unhandled
    // 'error' event takes the whole process down. Absorb it here: a real read failure still reaches the
    // storage component through its own consumption of the stream.
    storedStream.on('error', () => undefined)
    await components.storage.storeStream(hash, storedStream)
  } catch (e) {
    // The folder may have been removed from under us; forget it so a retry recreates it.
    ensuredFolders.delete(tmpFolder)
    throw e
  } finally {
    await deleteFileIfPresent(tmpFileName)
  }
}

// Folders this process has already created. mkdir(recursive) still costs a syscall when the folder is
// already there, and saveContentFileToDisk runs once per downloaded file — which is once per entry of
// every entity's content[]. Invalidated on failure so an externally removed folder self-heals.
const ensuredFolders = new Set<Path>()

async function ensureFolderExists(folder: Path): Promise<void> {
  if (ensuredFolders.has(folder)) {
    return
  }
  await fs.promises.mkdir(folder, { recursive: true })
  ensuredFolders.add(folder)
}

// unlink + ignore ENOENT rather than exists-then-unlink: one syscall instead of two, and no window
// between the check and the delete.
async function deleteFileIfPresent(filename: string): Promise<void> {
  await fs.promises.unlink(filename).catch(() => undefined)
}

// Stop following redirects after this many hops.
const MAX_REDIRECTS = 10
// Redirects worth following for a content download. 303/307/308 are as common as 301/302 in front of
// object storage (HTTP->HTTPS upgrades, bucket relocations); treating them as hard errors made every
// download from such a peer burn its whole retry ladder. All of these are followed with GET, which is
// what 303 mandates and what 307/308 preserve, since this client only ever issues GET.
const REDIRECT_STATUS_CODES = new Set([301, 302, 303, 307, 308])
function isNonPublicIPv4(address: string): boolean {
  const [first, second] = address.split('.').map(Number)
  if (first === 0) return true // 0.0.0.0/8 "this network"
  if (first === 10) return true // 10.0.0.0/8 private
  if (first === 127) return true // loopback
  if (first === 169 && second === 254) return true // link-local, incl. cloud metadata
  if (first === 172 && second >= 16 && second <= 31) return true // 172.16.0.0/12 private
  if (first === 192 && second === 168) return true // 192.168.0.0/16 private
  if (first === 100 && second >= 64 && second <= 127) return true // 100.64.0.0/10 CGNAT
  if (first >= 224) return true // multicast and reserved
  return false
}

/**
 * Expands an IPv6 literal to its 16 bytes, resolving `::` and any trailing dotted-quad.
 *
 * Textual matching is not good enough here: WHATWG URL rewrites every IPv4-mapped literal into
 * compressed hex (`::ffff:127.0.0.1` becomes `::ffff:7f00:1`), so a guard that only recognises the
 * dotted form never sees the shape that actually arrives. Comparing bytes removes that whole class of
 * near-miss.
 */
function parseIPv6ToBytes(address: string): Uint8Array | undefined {
  // A zone id is meaningless to us and cannot reach here through URL parsing; drop it defensively.
  let text = address.toLowerCase().split('%')[0]

  const trailingIPv4 = text.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/)
  if (trailingIPv4) {
    const octets = trailingIPv4[1].split('.').map(Number)
    if (octets.some((octet) => !Number.isInteger(octet) || octet < 0 || octet > 255)) return undefined
    const high = ((octets[0] << 8) | octets[1]).toString(16)
    const low = ((octets[2] << 8) | octets[3]).toString(16)
    text = text.slice(0, trailingIPv4.index) + high + ':' + low
  }

  const halves = text.split('::')
  if (halves.length > 2) return undefined
  const toHextets = (part: string) => (part ? part.split(':') : [])
  const head = toHextets(halves[0])
  const tail = halves.length === 2 ? toHextets(halves[1]) : []

  let hextets: string[]
  if (halves.length === 1) {
    if (head.length !== 8) return undefined
    hextets = head
  } else {
    const missing = 8 - head.length - tail.length
    if (missing < 0) return undefined
    hextets = [...head, ...new Array(missing).fill('0'), ...tail]
  }

  const bytes = new Uint8Array(16)
  for (let index = 0; index < 8; index++) {
    if (!/^[0-9a-f]{1,4}$/.test(hextets[index])) return undefined
    const value = parseInt(hextets[index], 16)
    bytes[index * 2] = value >> 8
    bytes[index * 2 + 1] = value & 0xff
  }
  return bytes
}

/**
 * True for addresses that are only reachable from inside our own network: RFC1918 private ranges,
 * loopback, link-local (which covers the cloud metadata endpoints at 169.254.169.254), carrier-grade
 * NAT, "this network", multicast/reserved space, and their IPv6 equivalents.
 *
 * Anything that is not a recognisable IP literal is reported as non-public: callers only ever pass
 * resolved addresses, so an unparseable value means something unexpected happened and refusing is the
 * safe direction.
 */
export function isNonPublicAddress(address: string): boolean {
  if (net.isIPv4(address)) {
    return isNonPublicIPv4(address)
  }

  if (net.isIPv6(address)) {
    const bytes = parseIPv6ToBytes(address)
    if (!bytes) return true

    const first10AreZero = bytes.subarray(0, 10).every((byte) => byte === 0)
    // ::ffff:0:0/96 — IPv4-mapped. Judge the IPv4 address it carries.
    if (first10AreZero && bytes[10] === 0xff && bytes[11] === 0xff) {
      return isNonPublicIPv4(`${bytes[12]}.${bytes[13]}.${bytes[14]}.${bytes[15]}`)
    }
    // ::/96 — covers :: (unspecified), ::1 (loopback) and the deprecated IPv4-compatible form.
    if (bytes.subarray(0, 12).every((byte) => byte === 0)) return true
    if (bytes[0] === 0xfe && (bytes[1] & 0xc0) === 0x80) return true // fe80::/10 link-local
    if ((bytes[0] & 0xfe) === 0xfc) return true // fc00::/7 unique local
    if (bytes[0] === 0xff) return true // ff00::/8 multicast
    return false
  }

  return true
}

/**
 * A DNS lookup that refuses to resolve to a non-public address, so a redirect cannot steer a download
 * at an internal service by pointing at a hostname whose A record is private. `allowedHostname` — the
 * host the caller originally asked for, which comes from the trusted server list — is always allowed,
 * which is what keeps loopback-based local development and tests working.
 */
/**
 * Exported for testing: this guard's decisions depend on DNS answer sets that are impractical to
 * produce through a real download, and it is the piece of this file with the worst track record.
 */
export function createRedirectSafeLookup(allowedHostname: string): LookupFunction {
  const allowed = allowedHostname.toLowerCase()
  // Whether the caller's own host resolved to a non-public address the first time we looked it up.
  // Recorded once and then held for the whole download, which is what closes DNS rebinding: a host that
  // answers the initial request from a public address cannot later resolve to loopback on a same-host
  // redirect, even though such a redirect passes every origin and hostname check.
  //
  // Deliberately the public/non-public *classification* rather than the exact address, so a legitimate
  // round-robin CDN handing out a different public IP per query still works.
  let allowedHostWasNonPublic: boolean | undefined

  return ((hostname: string, options: any, callback: any) => {
    dnsLookup(hostname, options, (err: any, address: any, family: any) => {
      if (err) {
        callback(err, address, family)
        return
      }
      const resolved: { address: string }[] = Array.isArray(address) ? address : [{ address }]
      const nonPublic = resolved.find((entry) => isNonPublicAddress(entry.address))

      if (hostname.toLowerCase() === allowed) {
        // The caller's host comes from the trusted server list, so it is allowed to be private — that
        // is what keeps loopback-based local development and the tests working. It just may not
        // *change* classification mid-download.
        //
        // A mixed answer set has no single classification to pin, and taking "contains a non-public
        // address" as the answer is exploitable: the connection may well use the public address, while
        // the guard records "non-public" — after which a same-host redirect resolving to ONLY loopback
        // matches the recorded value and is waved through, which is the exact pivot this exists to stop.
        // It cuts the other way too, rejecting a redirect that narrows to public-only. Neither set is a
        // configuration a real content server has, so refuse the ambiguity instead of guessing at it.
        const everyAddressNonPublic = resolved.every((entry) => isNonPublicAddress(entry.address))
        if (nonPublic && !everyAddressNonPublic) {
          callback(
            new Error(
              `Refusing to use ${hostname}: it resolves to a mix of public and non-public addresses, which has no single classification to hold for the rest of the download`
            )
          )
          return
        }
        if (allowedHostWasNonPublic === undefined) {
          allowedHostWasNonPublic = everyAddressNonPublic
        } else if (everyAddressNonPublic !== allowedHostWasNonPublic) {
          callback(
            new Error(
              `Refusing to follow a redirect to ${hostname}: it now resolves to ${
                nonPublic ? 'a non-public' : 'a public'
              } address, unlike the original request`
            )
          )
          return
        }
        callback(null, address, family)
        return
      }

      if (nonPublic) {
        callback(new Error(`Refusing to follow a redirect to ${hostname} (${nonPublic.address}): not a public address`))
        return
      }
      callback(null, address, family)
    })
  }) as LookupFunction
}

// Fails the pipeline once more than maxBytes have flowed through it. Placed *after* gunzip so it
// bounds the decompressed size.
/**
 * Rate floor for a body stream. **Must sit on the raw response, before any decompression.**
 *
 * A Transform's check only runs when a chunk reaches it, and gunzip can consume unbounded raw input while
 * emitting nothing at all — concatenated empty gzip members are valid and decompress to zero bytes
 * (measured: 1 MB of raw input, 0 bytes out, this check never invoked once). Placed after gunzip the
 * guard was not merely lenient, it never ran, while the raw socket traffic kept the inactivity deadline
 * refreshed. The clock starts when the response headers arrive, which is the right origin for measuring
 * body throughput.
 */
export function createTransferRateGuard(limits: ResolvedTransferLimits): Transform {
  let total = 0
  const startedAt = Date.now()
  return new Transform({
    transform(chunk, _encoding, callback) {
      total += chunk.length
      const tooSlow = tooSlowToContinue(total, startedAt, limits)
      if (tooSlow) {
        callback(tooSlow)
        return
      }
      callback(null, chunk)
    }
  })
}

/**
 * Ceiling on the bytes passing through, named by `subject` so a failure says which side of a gzip
 * boundary tripped.
 */
export function createSizeCap(maxBytes: number, subject: string): Transform {
  let total = 0
  return new Transform({
    transform(chunk, _encoding, callback) {
      total += chunk.length
      if (total > maxBytes) {
        callback(new Error(`${subject} exceeds the maximum allowed size of ${maxBytes} bytes`))
        return
      }
      callback(null, chunk)
    }
  })
}

/**
 * The transforms a download applies, in order, between the response and the file.
 *
 * Exported as one unit because the *ordering* is the security property — see
 * {@link createTransferRateGuard} for what putting the rate check on the wrong side of gunzip costs — and
 * a test that rebuilt the chain itself would not be testing this ordering at all.
 */
export function createDownloadTransforms(isGzip: boolean, limits: ResolvedTransferLimits): Transform[] {
  const maxBytes = limits.maxDownloadedFileSizeInBytes
  const transforms: Transform[] = [createTransferRateGuard(limits)]
  if (isGzip) {
    // Bounds the compressed stream too, or a peer could stream valid gzip indefinitely while staying
    // above the rate floor and never produce a decompressed byte for the cap below to measure. For real
    // content this never binds: gzip exceeds its input only for incompressible data, and then by ~0.03%.
    transforms.push(createSizeCap(maxBytes, 'Compressed response'))
    transforms.push(zlib.createGunzip())
  }
  // After decompression, so this bounds what actually reaches the disk: the gzip-bomb guard.
  transforms.push(createSizeCap(maxBytes, 'Downloaded file'))
  return transforms
}

function downloadFile(
  originalUrlString: string,
  metricsLabels: ContentServerMetricLabels,
  components: { metrics?: SnapshotsFetcherComponents['metrics'] },
  tmpFileName: string,
  limits: ResolvedTransferLimits
) {
  return new Promise<void>((resolve, reject) => {
    // One timer for the whole download instead of one per redirect hop: a hop that redirects never
    // reaches a terminal state, so its timer was started and then silently dropped. The labels are
    // supplied at end() (they are merged there) because the origin that ultimately served the file
    // is only known after the last redirect.
    const { end: endTimeMeasurement } = components.metrics?.startTimer('dcl_content_download_duration_seconds') || {
      end: (_endLabels?: ContentServerMetricLabels) => {}
    }

    // A destroyed socket can reject the pipeline *and* emit 'error' on the request. Without this
    // guard that records the duration twice and double-counts the error, so the first terminal
    // outcome wins and the rest are ignored.
    let settled = false

    function settleWithSuccess(bytesWritten: number) {
      if (settled) return
      settled = true
      components.metrics?.increment('dcl_content_download_bytes_total', metricsLabels, bytesWritten)
      endTimeMeasurement({ ...metricsLabels })
      resolve()
    }

    function settleWithError(err: Error) {
      if (settled) return
      settled = true
      components.metrics?.increment('dcl_content_download_errors_total', metricsLabels)
      endTimeMeasurement({ ...metricsLabels })
      reject(err)
    }

    const originalHostname = new URL(originalUrlString).hostname
    // Created once for the whole download, not per hop: it carries the resolved-address classification
    // of the original host between hops, which is what lets it detect a rebind.
    const redirectSafeLookup = createRedirectSafeLookup(originalHostname)

    function requestWithRedirects(redirectedUrl: string, baseUrl: string, redirects: number) {
      // Relative redirects must be resolved against the URL that issued them, not the original URL.
      //
      // Guarded because this function is re-entered from inside the response listener: a `Location`
      // the URL parser rejects (`http://[`, `http://:80`, …) would throw there, escape this Promise's
      // executor and surface as an uncaughtException — a remote crash of the whole process, triggerable
      // by any server in the list on any content file.
      let url: URL
      try {
        url = new URL(redirectedUrl, baseUrl)
      } catch {
        settleWithError(
          new Error(
            `Invalid redirect location ${truncateForLog(JSON.stringify(redirectedUrl))} from ` +
              sanitizeUrlForLog(baseUrl)
          )
        )
        return
      }
      // Only http(s) is supported; reject other schemes (e.g. file:) a redirect could point to.
      if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        settleWithError(new Error('Unsupported protocol in URL ' + sanitizeUrlForLog(url.toString())))
        return
      }
      const httpModule = url.protocol === 'https:' ? https : http
      if (redirects > MAX_REDIRECTS) {
        settleWithError(new Error('Too much redirects'))
        return
      }

      // A literal IP never reaches the lookup below (there is nothing to resolve), so it is checked
      // here. Redirect targets are chosen by the remote server; without this it could point the
      // download at an internal address and use this process to reach it.
      const hostnameWithoutBrackets = url.hostname.replace(/^\[|\]$/g, '')
      if (
        url.hostname.toLowerCase() !== originalHostname.toLowerCase() &&
        net.isIP(hostnameWithoutBrackets) &&
        isNonPublicAddress(hostnameWithoutBrackets)
      ) {
        settleWithError(new Error(`Refusing to follow a redirect to ${url.hostname}: not a public address`))
        return
      }

      Object.assign(metricsLabels, contentServerMetricLabels(url.toString()))

      const requestOptions = {
        headers: { 'accept-encoding': 'gzip' },
        lookup: redirectSafeLookup
      }

      const request = httpModule.get(url.toString(), requestOptions, (response) => {
        if (response.statusCode && REDIRECT_STATUS_CODES.has(response.statusCode) && response.headers.location) {
          // Destroy rather than drain. Only the Location header matters here, and resume() would keep
          // reading a body we already decided to throw away — a server can attach an arbitrarily large or
          // slow one to every hop, so up to MAX_REDIRECTS of them would stream in parallel with the hops
          // that follow. Destroying frees the socket immediately and bounds a redirect to its headers.
          // Verified not to make the abandoned request emit 'error': that would otherwise reach
          // settleWithError and fail the download even though the next hop is proceeding.
          response.destroy()
          // handle redirection
          requestWithRedirects(response.headers.location!, url.toString(), redirects + 1)
          return
          // Only a 2xx carries the content. The previous `> 300` test also accepted 300 Multiple
          // Choices, writing its body to disk as though it were the file, and accepted a redirect
          // status that arrived without a Location header.
        } else if (!response.statusCode || response.statusCode < 200 || response.statusCode >= 300) {
          // Destroyed, not drained, for the same reason as the redirect path above: this body is already
          // being thrown away, and resume() keeps reading it to completion outside every transfer bound —
          // the size caps and rate floor live in createDownloadTransforms, which this path never reaches.
          // A server can therefore pair an error status with an endless body and keep the socket and the
          // bandwidth after the promise has already rejected and the caller has moved on.
          response.destroy()
          settleWithError(
            new Error('Invalid response from ' + sanitizeUrlForLog(url.toString()) + ' status: ' + response.statusCode)
          )
          return
        } else {
          // Content-coding tokens are case-insensitive (RFC 9110 §8.4.1) and `x-gzip` is a legacy alias
          // for `gzip` that some servers still send, so matching the exact string `gzip` meant a correct
          // `Content-Encoding: GZip` response was written to disk still compressed, failed hash
          // verification, and burned the whole retry ladder against a well-behaved server. `identity`
          // means no coding was applied and is dropped here rather than treated as one.
          const declaredEncodings = String(response.headers['content-encoding'] ?? '')
            .split(',')
            .map((coding) => coding.trim().toLowerCase())
            .filter((coding) => coding !== '' && coding !== 'identity')
          const isGzip =
            declaredEncodings.length === 1 && (declaredEncodings[0] === 'gzip' || declaredEncodings[0] === 'x-gzip')

          // Any other coding — `br`, `deflate`, or several layered together — is one this client cannot
          // undo, so the bytes could never match the hash. Failing here names the reason instead of
          // surfacing as a hash mismatch after the retry ladder has been spent. Truncated because the
          // header value is the server's choice.
          if (declaredEncodings.length > 0 && !isGzip) {
            // Destroyed rather than drained: see the status path above. The coding is chosen by the
            // server, so draining here would be an attacker-selectable way back to an unbounded read.
            response.destroy()
            settleWithError(
              new Error(
                `Cannot decode ${sanitizeUrlForLog(url.toString())}: unsupported content-encoding ` +
                  truncateForLog(JSON.stringify(declaredEncodings.join(', ')))
              )
            )
            return
          }

          const file = fs.createWriteStream(tmpFileName, {
            emitClose: true
          })

          const pipe = streamPipeline([response, ...createDownloadTransforms(isGzip, limits), file])

          pipe
            .then(() => {
              file.close() // close() is async, call cb after close completes.
              settleWithSuccess(file.bytesWritten)
            })
            .catch((err) => {
              file.close()
              settleWithError(err)
            })
        }
      })

      // Reject (instead of hanging forever) when the connection stalls before/while downloading.
      request.setTimeout(limits.downloadInactivityTimeoutInMs, () => {
        request.destroy(new Error('Timeout while downloading ' + sanitizeUrlForLog(url.toString())))
      })

      request.on('error', function (err) {
        settleWithError(err)
      })
    }

    requestWithRedirects(originalUrlString, originalUrlString, 0)
  })
}

export function pickRandomServer(serversToPickFrom: Server[]): string {
  if (serversToPickFrom.length === 0) {
    throw new Error('Cannot pick a server from an empty list of servers')
  }
  // A uniformly-random pick spreads load across servers well enough at scale, without round-robin bookkeeping.
  return serversToPickFrom[Math.floor(Math.random() * serversToPickFrom.length)]
}

export function contentServerMetricLabels(contentServer: string): ContentServerMetricLabels {
  const url = new URL(contentServer)
  return {
    remote_server: url.origin
  }
}

/**
 * Reads a stream fully into memory.
 *
 * @param maxBytes - Optional ceiling. Reject and destroy the stream once more than this many bytes
 *   have arrived, instead of buffering whatever the producer sends. Callers reading content-addressed
 *   files should pass one: the download cap alone allows a single file of
 *   `transferLimits.maxDownloadedFileSizeInBytes`, and nothing stops several from being read at once.
 */
export function streamToBuffer(stream: Readable, maxBytes?: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const buffers: Buffer[] = []
    let total = 0

    function fail(error: Error) {
      // Without this the producer keeps reading into a buffer nobody will use, and the underlying
      // handle is only reclaimed whenever GC gets to it.
      stream.destroy()
      reject(error)
    }

    stream.on('error', reject)
    stream.on('data', (data: Buffer) => {
      total += data.length
      if (maxBytes !== undefined && total > maxBytes) {
        fail(new Error(`Stream exceeds the maximum allowed size of ${maxBytes} bytes`))
        return
      }
      buffers.push(data)
    })
    stream.on('end', () => resolve(Buffer.concat(buffers)))
  })
}

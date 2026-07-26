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
import { pipeline, Readable, Transform } from 'stream'
import { promisify } from 'util'
import * as zlib from 'zlib'
import { ContentServerMetricLabels } from './metrics'
import { Path, Server, SnapshotsFetcherComponents } from './types'

const streamPipeline = promisify(pipeline)

// Bounds buffered JSON responses so a malicious server can't OOM the process via response.json().
const MAX_JSON_RESPONSE_SIZE_IN_BYTES = 50 * 1024 * 1024 // 50 MiB

// Reads a response body while enforcing a maximum size. The native fetcher (unlike node-fetch) has
// no `size` option, so we cap manually: read the stream with a running byte count and abort —
// cancelling the stream to free its socket — if it exceeds the limit.
// Applied to the body read when the caller did not ask for a specific timeout.
const DEFAULT_BODY_READ_TIMEOUT_IN_MS = 30_000
// How many same-origin redirects fetchJson will follow itself.
const MAX_JSON_REDIRECTS = 5

async function readBodyWithSizeLimit(response: Response, maxBytes: number, timeoutMs: number): Promise<string> {
  const reader = response.body?.getReader()
  if (!reader) {
    return ''
  }

  const chunks: Uint8Array[] = []
  let total = 0
  // The fetch component's timeout only covers time-to-headers: it clears its abort timer as soon as
  // the response object exists. Without a deadline here a server can send headers, write half a
  // document and then go silent forever — exactly the stall the request timeout was meant to prevent,
  // and a pending promise never reaches the reconnection logic.
  let timedOut = false
  const timeout = setTimeout(() => {
    timedOut = true
    void reader.cancel().catch(() => undefined)
  }, timeoutMs)

  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) {
        total += value.byteLength
        if (total > maxBytes) {
          throw new Error(`Response body exceeds the maximum allowed size of ${maxBytes} bytes`)
        }
        chunks.push(value)
      }
    }
  } catch (e) {
    throw timedOut ? new Error(`Timed out after ${timeoutMs}ms while reading the response body`) : e
  } finally {
    clearTimeout(timeout)
    // Frees the socket on the size-exceeded path; a no-op once the stream has been fully read.
    await reader.cancel().catch(() => undefined)
  }

  if (timedOut) {
    throw new Error(`Timed out after ${timeoutMs}ms while reading the response body`)
  }

  return Buffer.concat(chunks).toString('utf8')
}

export async function fetchJson(url: string, fetcher: IFetchComponent, init?: RequestOptions): Promise<any> {
  const bodyReadTimeout = init?.timeout ?? DEFAULT_BODY_READ_TIMEOUT_IN_MS

  // redirect: 'manual' so redirects are followed here, under a same-origin rule, instead of by the
  // fetch implementation — whose default ('follow') chases any host the server names. That turned
  // every JSON endpoint into an SSRF primitive and let a redirect sidestep the same-origin check
  // fetchJsonPaginated applies to `pagination.next`.
  let currentUrl = url
  for (let redirects = 0; ; redirects++) {
    const response = await fetcher.fetch(currentUrl, { ...init, redirect: 'manual' })

    if (response.status >= 300 && response.status < 400) {
      await response.body?.cancel().catch(() => undefined)
      const location = response.headers.get('location')
      if (!location) {
        throw new Error(`Error fetching ${currentUrl}. Redirect status ${response.status} without a location.`)
      }
      if (redirects >= MAX_JSON_REDIRECTS) {
        throw new Error(`Error fetching ${url}. Too many redirects.`)
      }
      let redirectTarget: URL
      try {
        redirectTarget = new URL(location, currentUrl)
      } catch {
        throw new Error(`Error fetching ${currentUrl}. Invalid redirect location ${JSON.stringify(location)}.`)
      }
      if (redirectTarget.origin !== new URL(currentUrl).origin) {
        throw new Error(
          `Refusing to follow a cross-origin redirect while fetching ${url}: ${redirectTarget.origin} does not match ${
            new URL(currentUrl).origin
          }`
        )
      }
      currentUrl = redirectTarget.toString()
      continue
    }

    if (!response.ok) {
      // Drain the body so undici releases the socket back to the pool before throwing.
      await response.body?.cancel().catch(() => undefined)
      throw new Error('Error fetching ' + currentUrl + '. Status code was: ' + response.status)
    }

    const body = await readBodyWithSizeLimit(response, MAX_JSON_RESPONSE_SIZE_IN_BYTES, bodyReadTimeout)
    if (body === '') {
      throw new Error('Error fetching ' + currentUrl + '. The response body was empty.')
    }

    return JSON.parse(body)
  }
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

// Content hashes are IPFS CIDs (base58/base32), hence alphanumeric. Validating against this charset
// before using a hash in a path or storage key prevents path traversal from untrusted hashes.
const VALID_CONTENT_HASH = /^[a-zA-Z0-9]+$/
export function isValidContentHash(hash: string): boolean {
  return typeof hash === 'string' && hash.length > 0 && hash.length <= 128 && VALID_CONTENT_HASH.test(hash)
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
  checkHash: boolean = true
): Promise<void> {
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
    await downloadFile(originalUrlString, metricsLabels, components, tmpFileName)

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
    await components.storage.storeStream(hash, fs.createReadStream(tmpFileName))
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
// Abort a download after this many milliseconds of socket inactivity. Healthy downloads keep the
// socket busy, so this only trips on stalled connections (e.g. a server that stops sending bytes).
const DOWNLOAD_INACTIVITY_TIMEOUT_MS = 30_000
// Hard cap on the number of bytes written to disk (after decompression). Protects against gzip
// bombs and otherwise unbounded responses that could exhaust the disk.
const MAX_DOWNLOADED_FILE_SIZE_IN_BYTES = 1024 * 1024 * 1024 // 1 GiB

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
function createRedirectSafeLookup(allowedHostname: string): LookupFunction {
  const allowed = allowedHostname.toLowerCase()

  return ((hostname: string, options: any, callback: any) => {
    if (hostname.toLowerCase() === allowed) {
      dnsLookup(hostname, options, callback)
      return
    }

    dnsLookup(hostname, options, (err: any, address: any, family: any) => {
      if (err) {
        callback(err, address, family)
        return
      }
      const resolved: { address: string }[] = Array.isArray(address) ? address : [{ address }]
      const blocked = resolved.find((entry) => isNonPublicAddress(entry.address))
      if (blocked) {
        callback(new Error(`Refusing to follow a redirect to ${hostname} (${blocked.address}): not a public address`))
        return
      }
      callback(null, address, family)
    })
  }) as LookupFunction
}

// Fails the pipeline once more than maxBytes have flowed through it. Placed *after* gunzip so it
// bounds the decompressed size.
function createSizeLimiter(maxBytes: number): Transform {
  let total = 0
  return new Transform({
    transform(chunk, _encoding, callback) {
      total += chunk.length
      if (total > maxBytes) {
        callback(new Error(`Downloaded file exceeds the maximum allowed size of ${maxBytes} bytes`))
      } else {
        callback(null, chunk)
      }
    }
  })
}

function downloadFile(
  originalUrlString: string,
  metricsLabels: ContentServerMetricLabels,
  components: { metrics?: SnapshotsFetcherComponents['metrics'] },
  tmpFileName: string
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
        settleWithError(new Error(`Invalid redirect location ${JSON.stringify(redirectedUrl)} from ${baseUrl}`))
        return
      }
      // Only http(s) is supported; reject other schemes (e.g. file:) a redirect could point to.
      if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        settleWithError(new Error('Unsupported protocol in URL ' + url.toString()))
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
        lookup: createRedirectSafeLookup(originalHostname)
      }

      const request = httpModule.get(url.toString(), requestOptions, (response) => {
        if ((response.statusCode === 302 || response.statusCode === 301) && response.headers.location) {
          // drain the redirect response so its socket is freed (and its inactivity timer cleared)
          response.resume()
          // handle redirection
          requestWithRedirects(response.headers.location!, url.toString(), redirects + 1)
          return
        } else if (!response.statusCode || response.statusCode > 300) {
          response.resume()
          settleWithError(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
          return
        } else {
          const file = fs.createWriteStream(tmpFileName, {
            emitClose: true
          })

          const isGzip = response.headers['content-encoding'] === 'gzip'
          const sizeLimiter = createSizeLimiter(MAX_DOWNLOADED_FILE_SIZE_IN_BYTES)

          const pipe = isGzip
            ? streamPipeline(response, zlib.createGunzip(), sizeLimiter, file)
            : streamPipeline(response, sizeLimiter, file)

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
      request.setTimeout(DOWNLOAD_INACTIVITY_TIMEOUT_MS, () => {
        request.destroy(new Error('Timeout while downloading ' + url.toString()))
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

export function streamToBuffer(stream: Readable): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const buffers: any[] = []
    stream.on('error', reject)
    stream.on('data', (data) => buffers.push(data))
    stream.on('end', () => resolve(Buffer.concat(buffers)))
  })
}

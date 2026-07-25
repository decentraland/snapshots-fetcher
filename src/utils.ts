import { hashV0, hashV1 } from '@dcl/hashing'
import { IFetchComponent, RequestOptions } from '@dcl/core-commons'
import * as crypto from 'crypto'
import * as fs from 'fs'
import * as http from 'http'
import * as https from 'https'
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
async function readBodyWithSizeLimit(response: Response, maxBytes: number): Promise<string> {
  const reader = response.body?.getReader()
  if (!reader) {
    return ''
  }

  const chunks: Uint8Array[] = []
  let total = 0

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
  } finally {
    // Frees the socket on the size-exceeded path; a no-op once the stream has been fully read.
    await reader.cancel().catch(() => undefined)
  }

  return Buffer.concat(chunks).toString('utf8')
}

export async function fetchJson(url: string, fetcher: IFetchComponent, init?: RequestOptions): Promise<any> {
  const response = await fetcher.fetch(url, init)

  if (!response.ok) {
    // Drain the body so undici releases the socket back to the pool before throwing.
    await response.body?.cancel().catch(() => undefined)
    throw new Error('Error fetching ' + url + '. Status code was: ' + response.status)
  }

  const body = await readBodyWithSizeLimit(response, MAX_JSON_RESPONSE_SIZE_IN_BYTES)
  if (body === '') {
    throw new Error('Error fetching ' + url + '. The response body was empty.')
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

    function requestWithRedirects(redirectedUrl: string, baseUrl: string, redirects: number) {
      // Relative redirects must be resolved against the URL that issued them, not the original URL.
      const url = new URL(redirectedUrl, baseUrl)
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

      Object.assign(metricsLabels, contentServerMetricLabels(url.toString()))

      const request = httpModule.get(url.toString(), { headers: { 'accept-encoding': 'gzip' } }, (response) => {
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

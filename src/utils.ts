import { hashV0, hashV1 } from '@dcl/hashing'
import { IFetchComponent, RequestOptions } from '@dcl/core-commons'
import * as crypto from 'crypto'
import * as fs from 'fs'
import * as http from 'http'
import * as https from 'https'
import { pipeline, Readable, Transform } from 'stream'
import { promisify } from 'util'
import * as zlib from 'zlib'
import { ContentServerMetricLabels } from './metrics'
import { Server, SnapshotsFetcherComponents } from './types'

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

  return JSON.parse(await readBodyWithSizeLimit(response, MAX_JSON_RESPONSE_SIZE_IN_BYTES))
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
  let tmpFileName: string

  do {
    tmpFileName = destinationFilename + crypto.randomBytes(16).toString('hex')
    // this is impossible
  } while (await checkFileExists(tmpFileName))

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
        try {
          if (await checkFileExists(tmpFileName)) {
            await fs.promises.unlink(tmpFileName)
          }
        } catch {}
        throw e
      }
    }

    // move downloaded file to target folder
    await components.storage.storeStream(hash, fs.createReadStream(tmpFileName))
  } finally {
    // Delete the file async.
    if (await checkFileExists(tmpFileName)) {
      await fs.promises.unlink(tmpFileName)
    }
  }
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
    function requestWithRedirects(redirectedUrl: string, baseUrl: string, redirects: number) {
      // Relative redirects must be resolved against the URL that issued them, not the original URL.
      const url = new URL(redirectedUrl, baseUrl)
      // Only http(s) is supported; reject other schemes (e.g. file:) a redirect could point to.
      if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        reject(new Error('Unsupported protocol in URL ' + url.toString()))
        return
      }
      const httpModule = url.protocol === 'https:' ? https : http
      if (redirects > MAX_REDIRECTS) {
        reject(new Error('Too much redirects'))
        return
      }

      Object.assign(metricsLabels, contentServerMetricLabels(url.toString()))

      const { end: endTimeMeasurement } = components.metrics?.startTimer(
        'dcl_content_download_duration_seconds',
        metricsLabels
      ) || { end: () => {} }

      const request = httpModule.get(url.toString(), { headers: { 'accept-encoding': 'gzip' } }, (response) => {
        if ((response.statusCode === 302 || response.statusCode === 301) && response.headers.location) {
          // drain the redirect response so its socket is freed (and its inactivity timer cleared)
          response.resume()
          // handle redirection
          requestWithRedirects(response.headers.location!, url.toString(), redirects + 1)
          return
        } else if (!response.statusCode || response.statusCode > 300) {
          response.resume()
          reject(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
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
              components.metrics?.increment('dcl_content_download_bytes_total', metricsLabels, file.bytesWritten)
              endTimeMeasurement()
              resolve()
            })
            .catch((err) => {
              file.close()
              reject(err)
              components.metrics?.increment('dcl_content_download_errors_total', metricsLabels)
              endTimeMeasurement()
            })
        }
      })

      // Reject (instead of hanging forever) when the connection stalls before/while downloading.
      request.setTimeout(DOWNLOAD_INACTIVITY_TIMEOUT_MS, () => {
        request.destroy(new Error('Timeout while downloading ' + url.toString()))
      })

      request.on('error', function (err) {
        reject(err)
        components.metrics?.increment('dcl_content_download_errors_total', metricsLabels)
        endTimeMeasurement()
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

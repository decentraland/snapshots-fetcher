import * as fs from 'fs'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import * as http from 'http'
import * as https from 'https'
import * as crypto from 'crypto'
import * as zlib from 'zlib'
import * as multihashes from 'multihashes'
import { importer } from 'ipfs-unixfs-importer'
import CID from 'cids'
import path from 'path'
import { IFetchComponent } from '@well-known-components/http-server'
import { RemoteEntityDeployment, Server, SnapshotsFetcherComponents } from './types'
import { ContentServerMetricLabels } from './metrics'

const streamPipeline = promisify(pipeline)

export async function fetchJson(url: string, fetcher: IFetchComponent): Promise<any> {
  const response = await fetcher.fetch(url)

  if (!response.ok) {
    throw new Error('Error fetching ' + url + '. Status code was: ' + response.status)
  }

  const body = await response.json()
  return body
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

/**
 * Calculates a Qm prefixed hash for Decentraland (NOT CIDv0) from a readable stream
 */
export async function hashStreamV0(stream: AsyncGenerator<Uint8Array>) {
  const hash = crypto.createHash('sha256')
  for await (const chunk of stream) {
    hash.update(chunk)
  }
  let lastDigest = multihashes.encode(hash.digest(), 'sha2-256')
  return new CID(0, 'dag-pb', lastDigest).toBaseEncodedString()
}

/**
 * Calculates a CIDv1 from a readable stream
 */
export async function hashStreamV1(content: AsyncGenerator<Uint8Array>) {
  const block = {
    get: (cid: any) => Promise.reject(new Error(`unexpected block API get for ${cid}`)),
    put: () => Promise.reject(new Error('unexpected block API put')),
  } as any

  let lastCid

  for await (const { cid } of importer([{ content }], block, {
    cidVersion: 1,
    onlyHash: true,
    rawLeaves: true,
  })) {
    lastCid = cid
  }

  return `${lastCid}`
}

export async function assertHash(filename: string, hash: string) {
  if (hash.startsWith('Qm')) {
    const file = fs.createReadStream(filename)
    try {
      const qmHash = await hashStreamV0(file as any)
      if (qmHash != hash) {
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
      const baHash = await hashStreamV1(file as any)
      if (baHash != hash) {
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

export async function saveToDisk(
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
  originalUrlString: string,
  destinationFilename: string,
  checkHash?: string
): Promise<{}> {
  let tmpFileName: string

  do {
    tmpFileName = destinationFilename + crypto.randomBytes(16).toString('hex')
    // this is impossible
  } while (await checkFileExists(tmpFileName))

  const metricsLabels: ContentServerMetricLabels = {
    remote_server: '',
  }

  try {
    await new Promise<void>((resolve, reject) => {
      const MAX_REDIRECTS = 10

      function requestWithRedirects(redirectedUrl: string, redirects: number) {
        const url = new URL(redirectedUrl, originalUrlString)
        const httpModule = url.protocol === 'https:' ? https : http
        if (redirects > MAX_REDIRECTS) {
          reject(new Error('Too much redirects'))
          return
        }

        Object.assign(metricsLabels, contentServerMetricLabels(url.toString()))

        const { end: endTimeMeasurement } = components.metrics.startTimer(
          'dcl_content_download_duration_seconds',
          metricsLabels
        )

        httpModule
          .get(url.toString(), { headers: { 'accept-encoding': 'gzip' } }, (response) => {
            if ((response.statusCode == 302 || response.statusCode == 301) && response.headers.location) {
              // handle redirection
              requestWithRedirects(response.headers.location!, redirects + 1)
              return
            } else if (!response.statusCode || response.statusCode > 300) {
              reject(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
              return
            } else {
              const file = fs.createWriteStream(tmpFileName, { emitClose: true })

              const isGzip = response.headers['content-encoding'] == 'gzip'

              const pipe = isGzip ? streamPipeline(response, zlib.createGunzip(), file) : streamPipeline(response, file)

              pipe
                .then(() => {
                  file.close() // close() is async, call cb after close completes.
                  components.metrics.increment('dcl_content_download_bytes_total', metricsLabels, file.bytesWritten)
                  endTimeMeasurement()
                  resolve()
                })
                .catch((err) => {
                  file.close()
                  reject(err)
                  components.metrics.increment('dcl_content_download_errors_total', metricsLabels)
                  endTimeMeasurement()
                })
            }
          })
          .on('error', function (err) {
            reject(err)
            components.metrics.increment('dcl_content_download_errors_total', metricsLabels)
            endTimeMeasurement()
          })
      }

      requestWithRedirects(originalUrlString, 0)
    })

    // make files not executable
    await fs.promises.chmod(tmpFileName, 0o644)

    // check hash if present. delete file and fail in case of mismatch
    if (checkHash) {
      try {
        await assertHash(tmpFileName, checkHash)
      } catch (e) {
        components.metrics.increment('dcl_content_download_hash_errors_total', metricsLabels)
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
    const originalFile = path.parse(destinationFilename)
    await components.storage.storeExistingContentItem(tmpFileName, originalFile.base)
  } finally {
    // Delete the file async.
    if (await checkFileExists(tmpFileName)) {
      await fs.promises.unlink(tmpFileName)
    }
  }

  return {}
}

export function coerceEntityDeployment(value: any): RemoteEntityDeployment | null {
  if (
    value &&
    typeof value == 'object' &&
    typeof value.entityId == 'string' &&
    typeof value.entityType == 'string' &&
    typeof value.localTimestamp == 'number' &&
    Array.isArray(value.authChain)
  ) {
    return value
  }

  console.error('ERROR: Invalid entity deployment', value)
  return null
}

export function pickLeastRecentlyUsedServer(
  serversToPickFrom: Server[],
  _serverMap: Map<string, number /* timestamp */>
): string {
  let mostSuitableOption = serversToPickFrom[Math.floor(Math.random() * serversToPickFrom.length)]
  // TODO: implement load balancing strategy
  return mostSuitableOption
}

export function contentServerMetricLabels(contentServer: string): ContentServerMetricLabels {
  const url = new URL(contentServer)
  return {
    remote_server: url.origin,
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

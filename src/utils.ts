import * as fs from 'fs'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import * as http from 'http'
import * as https from 'https'
import * as crypto from 'crypto'
import * as zlib from 'zlib'
import { IFetchComponent } from '@well-known-components/http-server'
import { Server, SnapshotsFetcherComponents } from './types'
import { ContentServerMetricLabels } from './metrics'
import { hashV0, hashV1 } from '@dcl/hashing'
import * as fetch from 'node-fetch'

const streamPipeline = promisify(pipeline)

export async function fetchJson(url: string, fetcher: IFetchComponent, init?: fetch.RequestInit): Promise<any> {
  const response = await fetcher.fetch(url, init)

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
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
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
    await components.storage.storeStream(hash, fs.createReadStream(tmpFileName))
  } finally {
    // Delete the file async.
    if (await checkFileExists(tmpFileName)) {
      await fs.promises.unlink(tmpFileName)
    }
  }
}

function downloadFile(
  originalUrlString: string,
  metricsLabels: ContentServerMetricLabels,
  components: Pick<SnapshotsFetcherComponents, 'metrics'>,
  tmpFileName: string
) {
  return new Promise<void>((resolve, reject) => {
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
          if ((response.statusCode === 302 || response.statusCode === 301) && response.headers.location) {
            // handle redirection
            requestWithRedirects(response.headers.location!, redirects + 1)
            return
          } else if (!response.statusCode || response.statusCode > 300) {
            reject(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
            return
          } else {
            const file = fs.createWriteStream(tmpFileName, {
              emitClose: true
            })

            const isGzip = response.headers['content-encoding'] === 'gzip'

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
}

export function pickLeastRecentlyUsedServer(serversToPickFrom: Server[]): string {
  // Here is the thing. We could perfectly use round-robin to download content files
  // and/or spend precious CPU cycles in a fancy load balancing algorithm.
  // But we are dealing with thousands of "load balancing events". And Math.random()
  // has a **normal distribution**, which has in practice (and big numbers) the same
  // effect, load balancing.
  //
  // Math is lovely.
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

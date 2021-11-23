import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import * as http from 'http'
import * as https from 'https'
import * as crypto from 'crypto'
import * as multihashes from 'multihashes'
import { importer } from 'ipfs-unixfs-importer'
import CID from 'cids'
import { IFetchComponent } from '@well-known-components/http-server'
import { RemoteEntityDeployment, Server } from './types'

export async function fetchJson(url: string, fetcher: IFetchComponent): Promise<any> {
  const request = await fetcher.fetch(url)

  if (!request.ok) {
    throw new Error('HTTP Error while loading JSON from: ' + url)
  }

  const body = await request.json()
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

export default function withCache<R>(handler: () => R): () => R {
  const empty = Symbol('@empty')
  let cache: R | symbol = empty
  return () => {
    if (cache === empty) {
      cache = handler()
    }

    return cache as R
  }
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
    const qmHash = await hashStreamV0(fs.createReadStream(filename) as any)
    if (qmHash != hash) {
      throw new Error(
        `Download error: hashes do not match(expected:${hash} != calculated:${qmHash}) for file ${filename}`
      )
    }
  } else if (hash.startsWith('ba')) {
    const baHash = await hashStreamV1(fs.createReadStream(filename) as any)
    if (baHash != hash) {
      throw new Error(
        `Download error: hashes do not match(expected:${hash} != calculated:${baHash}) for file ${filename}`
      )
    }
  } else {
    throw new Error(`Unknown hashing algorithm for hash: ${hash}`)
  }
}

export async function saveToDisk(originalUrl: string, destinationFilename: string, checkHash?: string): Promise<{}> {
  const tmpFileName = await tmpFile('saveToDisk')

  await new Promise<void>((resolve, reject) => {
    const httpModule = originalUrl.startsWith('https:') ? https : http
    const MAX_REDIRECTS = 10

    function requestWithRedirects(redirectedUrl: string, redirects: number) {
      const url = new URL(redirectedUrl, originalUrl).toString()
      if (redirects > MAX_REDIRECTS) {
        reject(new Error('Too much redirects'))
        return
      }
      httpModule
        .get(url, (response) => {
          if ((response.statusCode == 302 || response.statusCode == 301) && response.headers.location) {
            // handle redirection
            requestWithRedirects(response.headers.location!, redirects + 1)
          } else if (!response.statusCode || response.statusCode > 300) {
            reject(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
            return
          } else {
            const file = fs.createWriteStream(tmpFileName)
            response.pipe(file)

            response.on('error', (err) => {
              // Handle errors
              fs.unlink(tmpFileName, () => {}) // Delete the file async. (But we don't check the result)
              file.close()
              reject(err)
            })

            file.on('finish', function () {
              file.close() // close() is async, call cb after close completes.
              resolve()
            })
          }
        })
        .on('error', function (err) {
          // Handle errors
          fs.unlink(tmpFileName, () => {}) // Delete the file async. (But we don't check the result)
          reject(err)
        })
    }

    requestWithRedirects(originalUrl, 0)
  })

  // make files not executable
  await fs.promises.chmod(tmpFileName, 0o644)

  // check hash if present. delete file and fail in case of mismatch
  if (checkHash) {
    try {
      await assertHash(tmpFileName, checkHash)
    } catch (e) {
      // delete the downloaded file if failed
      try {
        await fs.promises.unlink(tmpFileName)
      } catch {}
      throw e
    }
  }

  // delete target file if exists
  if (await checkFileExists(destinationFilename)) {
    await fs.promises.unlink(destinationFilename)
  }

  // move downloaded file to target folder
  await fs.promises.rename(tmpFileName, destinationFilename)

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

/**
 * Returns a temporary directory for this process
 */
export const getTmpDir = withCache(async () => {
  const tempPath = path.join(os.tmpdir(), 'dcl-')

  return new Promise<string>((resolve, reject) => {
    fs.mkdtemp(tempPath, (err, folder) => {
      if (err) return reject(err)
      resolve(folder)
    })
  })
})

export async function tmpFile(postfix: string): Promise<string> {
  const fileName = `dcl-${crypto.randomBytes(16).toString('hex')}-${postfix}`
  return path.join(await getTmpDir(), fileName)
}

export function pickLeastRecentlyUsedServer(
  serversToPickFrom: Server[],
  _serverMap: Map<string, number /* timestamp */>
): string {
  let mostSuitableOption = serversToPickFrom[Math.floor(Math.random() * serversToPickFrom.length)]
  // TODO: implement load balancing strategy
  return mostSuitableOption
}

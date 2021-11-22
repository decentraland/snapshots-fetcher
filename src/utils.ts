import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import * as http from 'http'
import * as https from 'https'
import * as crypto from 'crypto'
import { IFetchComponent } from '@well-known-components/http-server'
import { Server } from './types'

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

export async function saveToDisk(url: string, destinationFilename: string): Promise<void> {
  const tmpFileName = await tmpFile('saveToDisk')
  await new Promise<void>((resolve, reject) => {
    const httpModule = url.startsWith('https:') ? https : http

    httpModule
      .get(url, function (response) {
        if (!response.statusCode || response.statusCode > 300) {
          reject(new Error('Invalid response from ' + url + ' status: ' + response.statusCode))
          return
        }

        const file = fs.createWriteStream(tmpFileName)
        response.pipe(file)

        response.on('error', (err) => {
          // Handle errors
          fs.unlink(destinationFilename, () => {}) // Delete the file async. (But we don't check the result)
          reject(err)
        })

        file.on('finish', function () {
          file.close() // close() is async, call cb after close completes.
          resolve()
        })
      })
      .on('abort', console.dir)
      .on('error', function (err) {
        // Handle errors
        fs.unlink(destinationFilename, () => {}) // Delete the file async. (But we don't check the result)
        reject(err)
      })
  })

  // make files not executable
  await fs.promises.chmod(tmpFileName, 0o644)

  // delete target file if exists
  if (await checkFileExists(destinationFilename)) {
    await fs.promises.unlink(destinationFilename)
  }

  // move downloaded file to target folder
  await fs.promises.rename(tmpFileName, destinationFilename)
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

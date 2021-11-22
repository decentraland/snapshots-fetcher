import { Path } from './types'
import * as path from 'path'
import { saveContentFileToDisk } from './client'
import { checkFileExists, pickLeastRecentlyUsedServer, sleep } from './utils'

const downloadFileJobsMap = new Map<Path, ReturnType<typeof downloadFileWithRetries>>()

async function downloadJob(
  hashToDownload: string,
  finalFileName: string,
  presentInServers: string[],
  serverMapLRU: Map<string, number>,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<string> {
  // cancel early if the file is already downloaded
  if (await checkFileExists(finalFileName)) return finalFileName

  let retries = 0

  for (;;) {
    retries++
    try {
      const serverToUse = pickLeastRecentlyUsedServer(presentInServers, serverMapLRU)
      await downloadContentFile(hashToDownload, finalFileName, serverToUse)

      return finalFileName
    } catch (e: any) {
      console.log(`Retrying download of hash ${hashToDownload} ${retries}/${maxRetries}. Reason: ${e}`)
      if (retries < maxRetries) {
        await sleep(waitTimeBetweenRetries)
        continue
      } else {
        throw e
      }
    }
  }
}

/**
 * Downloads a content file, reuses jobs if the file is already scheduled to be downloaded or it is
 * being downloaded
 */
export async function downloadFileWithRetries(
  hashToDownload: string,
  targetFolder: string,
  presentInServers: string[],
  serverMapLRU: Map<string, number>,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<string> {
  const finalFileName = path.resolve(targetFolder, hashToDownload)

  if (downloadFileJobsMap.has(finalFileName)) {
    return downloadFileJobsMap.get(finalFileName)!
  }

  try {
    const downloadWithRetriesJob = downloadJob(
      hashToDownload,
      finalFileName,
      presentInServers,
      serverMapLRU,
      maxRetries,
      waitTimeBetweenRetries
    )
    downloadFileJobsMap.set(finalFileName, downloadWithRetriesJob)

    return await downloadWithRetriesJob
  } finally {
    downloadFileJobsMap.delete(finalFileName)
  }
}

async function downloadContentFile(hash: string, finalFileName: string, serverToUse: string) {
  if (!(await checkFileExists(finalFileName))) {
    await saveContentFileToDisk(serverToUse, hash, finalFileName)
  }
}

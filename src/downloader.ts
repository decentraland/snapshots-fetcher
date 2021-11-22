import { Path } from './types'
import * as path from 'path'
import { saveContentFileToDisk } from './client'
import { checkFileExists, pickLeastRecentlyUsedServer, sleep } from './utils'

const downloadFileJobsMap = new Map<Path, ReturnType<typeof downloadFileWithRetries>>()
const MAX_DOWNLOAD_RETRIES = process.env.CI ? 3 : 10
const MAX_DOWNLOAD_RETRIES_WAIT_TIME = process.env.CI ? 100 : 3000

async function downloadJob(
  hashToDownload: string,
  finalFileName: string,
  presentInServers: string[],
  serverMapLRU: Map<string, number>
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
      console.log(`Retrying download of hash ${hashToDownload} ${retries}/${MAX_DOWNLOAD_RETRIES}`)
      if (retries < MAX_DOWNLOAD_RETRIES) {
        await sleep(MAX_DOWNLOAD_RETRIES_WAIT_TIME)
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
  serverMapLRU: Map<string, number>
): Promise<string> {
  const finalFileName = path.resolve(targetFolder, hashToDownload)

  if (downloadFileJobsMap.has(finalFileName)) {
    return downloadFileJobsMap.get(finalFileName)!
  }

  try {
    const downloadWithRetriesJob = downloadJob(hashToDownload, finalFileName, presentInServers, serverMapLRU)
    downloadFileJobsMap.set(finalFileName, downloadWithRetriesJob)

    return await downloadWithRetriesJob
  } finally {
    downloadFileJobsMap.delete(finalFileName)
  }
}

async function downloadContentFile(hash: string, finalFileName: string, serverToUse: string) {
  // TODO: save to tmp folder and then move to final destination, for this lib:
  //       always assume that it a file exists in its final destination it is ready to be used

  if (!(await checkFileExists(finalFileName))) {
    // const tmpFile = generateTmpFileName()
    // await saveContentFileToDisk(serverToUse, hash, tmpFile)
    // await mv(tmpFile, finalFileName)
    await saveContentFileToDisk(serverToUse, hash, finalFileName)
  }
}

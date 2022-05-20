import { Path, SnapshotsFetcherComponents } from './types'
import * as path from 'path'
import { saveContentFileToDisk } from './client'
import { pickLeastRecentlyUsedServer, sleep } from './utils'

const downloadFileJobsMap = new Map<Path, ReturnType<typeof downloadFileWithRetries>>()

async function downloadJob(
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
  hashToDownload: string,
  finalFileName: string,
  presentInServers: string[],
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<void> {
  // cancel early if the file is already downloaded
  if (await components.storage.exist(hashToDownload)) return

  let retries = 0

  for (;;) {
    retries++
    const serverToUse = pickLeastRecentlyUsedServer(presentInServers)
    try {
      components.metrics.observe('dcl_available_servers_histogram', {}, presentInServers.length)
      await downloadContentFile(components, hashToDownload, finalFileName, serverToUse)
      components.metrics.observe('dcl_content_download_job_succeed_retries', {}, retries)

      return
    } catch (e: any) {
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
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
  hashToDownload: string,
  targetTempFolder: string,
  presentInServers: string[],
  _serverMapLRU: Map<string, number>,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<void> {
  const finalFileName = path.resolve(targetTempFolder, hashToDownload)

  if (downloadFileJobsMap.has(finalFileName)) {
    return downloadFileJobsMap.get(finalFileName)!
  }

  try {
    const downloadWithRetriesJob = downloadJob(
      components,
      hashToDownload,
      finalFileName,
      presentInServers,
      maxRetries,
      waitTimeBetweenRetries
    )
    downloadFileJobsMap.set(finalFileName, downloadWithRetriesJob)

    await downloadWithRetriesJob
    return
  } finally {
    downloadFileJobsMap.delete(finalFileName)
  }
}

async function downloadContentFile(
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'storage'>,
  hash: string,
  finalFileName: string,
  serverToUse: string
): Promise<void> {
  if (!(await components.storage.exist(finalFileName))) {
    return saveContentFileToDisk(components, serverToUse, hash, finalFileName)
  }
}

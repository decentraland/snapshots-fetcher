import * as path from 'path'
import { saveContentFileToDisk } from './client'
import { SnapshotsFetcherComponents } from './types'
import { isValidContentHash, pickRandomServer, sleep } from './utils'

// Keyed by content hash, not by the resolved temp path: storage is content-addressed, so two callers
// asking for the same hash into different targetTempFolders are the same download and must share one
// job. Keying by path let them run concurrently, duplicating the transfer.
const downloadFileJobsMap = new Map<string, ReturnType<typeof downloadFileWithRetries>>()

async function downloadJob(
  components: Pick<SnapshotsFetcherComponents, 'storage'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  hashToDownload: string,
  finalFileName: string,
  presentInServers: string[],
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<void> {
  // cancel early if the file is already downloaded
  if (await components.storage.exist(hashToDownload)) return

  // Sample the number of candidate servers once per job, not once per retry (which would skew the histogram).
  components.metrics?.observe('dcl_available_servers_histogram', {}, presentInServers.length)

  let retries = 0
  let serversToPickFrom: string[] = presentInServers

  for (;;) {
    retries++
    // Re-check the store only from the second attempt onwards. On the first one nothing is awaited
    // between here and the check above, so it can never see a different answer — and for a remote
    // (e.g. S3-backed) storage component that redundant call is a second network round trip on every
    // single content file. From a retry it is worth paying: another process writing to the same
    // content-addressed storage may have stored the file while this job was failing.
    if (retries > 1 && (await components.storage.exist(hashToDownload))) {
      return
    }

    const serverToUse = pickRandomServer(serversToPickFrom)
    try {
      await saveContentFileToDisk(components, serverToUse, hashToDownload, finalFileName)
      components.metrics?.observe('dcl_content_download_job_succeed_retries', {}, retries)

      return
    } catch (e: any) {
      if (retries < maxRetries) {
        serversToPickFrom =
          serversToPickFrom.length > 1
            ? serversToPickFrom.filter((server) => server !== serverToUse)
            : serversToPickFrom
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
  components: Pick<SnapshotsFetcherComponents, 'storage'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  hashToDownload: string,
  targetTempFolder: string,
  presentInServers: string[],
  _serverMapLRU: Map<string, number>,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<void> {
  // Reject untrusted hashes that are not plain content addresses before using them to build a
  // filesystem path. Without this, a value like "../../etc/x" would escape targetTempFolder.
  if (!isValidContentHash(hashToDownload)) {
    throw new Error(`Invalid content hash: ${JSON.stringify(hashToDownload)}`)
  }

  const finalFileName = path.resolve(targetTempFolder, hashToDownload)

  const inflightJob = downloadFileJobsMap.get(hashToDownload)
  if (inflightJob) {
    return inflightJob
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
    downloadFileJobsMap.set(hashToDownload, downloadWithRetriesJob)

    await downloadWithRetriesJob
    return
  } finally {
    downloadFileJobsMap.delete(hashToDownload)
  }
}

import * as path from 'path'
import { saveContentFileToDisk } from './client'
import { SnapshotsFetcherComponents } from './types'
import { isValidContentHash, pickRandomServer, sleep } from './utils'

// In-flight downloads, per storage component and then per content hash.
//
// Keyed by hash rather than by the resolved temp path because storage is content-addressed: two
// callers asking for the same hash into different targetTempFolders are the same download and should
// share one job (keying by path let them run concurrently and duplicate the transfer).
//
// Scoped *per storage component* because sharing is only sound when both callers end up with the
// file. A job stores into the storage of whoever started it, so two callers holding different storage
// instances must not share: the joiner would be told the download succeeded while its own storage
// stayed empty. A WeakMap so a discarded storage component takes its entry with it.
const inflightDownloadsByStorage = new WeakMap<object, Map<string, ReturnType<typeof downloadFileWithRetries>>>()

function inflightJobsFor(storage: object): Map<string, ReturnType<typeof downloadFileWithRetries>> {
  const existing = inflightDownloadsByStorage.get(storage)
  if (existing) {
    return existing
  }
  const created = new Map<string, ReturnType<typeof downloadFileWithRetries>>()
  inflightDownloadsByStorage.set(storage, created)
  return created
}

async function downloadJob(
  components: Pick<SnapshotsFetcherComponents, 'storage'> & { metrics?: SnapshotsFetcherComponents['metrics'] },
  hashToDownload: string,
  finalFileName: string,
  presentInServers: string[],
  maxRetries: number,
  waitTimeBetweenRetries: number,
  shouldStop?: () => boolean
): Promise<void> {
  if (shouldStop?.()) {
    throw new Error(`Not downloading ${hashToDownload}: the caller asked to stop`)
  }

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
      // Give up the remaining attempts once the caller is shutting down. Without this the ladder runs
      // to completion — maxRetries multiplied by the per-download inactivity timeout — while whoever
      // called stop() waits for this job to return.
      if (shouldStop?.()) {
        throw e
      }
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
  waitTimeBetweenRetries: number,
  /** Consulted before the first attempt and between retries, so a shutdown does not have to wait out
   * the whole retry ladder. */
  shouldStop?: () => boolean
): Promise<void> {
  // Reject untrusted hashes that are not plain content addresses before using them to build a
  // filesystem path. Without this, a value like "../../etc/x" would escape targetTempFolder.
  if (!isValidContentHash(hashToDownload)) {
    throw new Error(`Invalid content hash: ${JSON.stringify(hashToDownload)}`)
  }

  const finalFileName = path.resolve(targetTempFolder, hashToDownload)
  const inflightForStorage = inflightJobsFor(components.storage)

  const inflightJob = inflightForStorage.get(hashToDownload)
  if (inflightJob) {
    try {
      return await inflightJob
    } catch {
      // The shared job failed against *its* candidate servers. This caller may have been given a
      // different set, so fall through and make its own attempt instead of inheriting a failure it
      // might not have suffered.
    }
  }

  const downloadWithRetriesJob = downloadJob(
    components,
    hashToDownload,
    finalFileName,
    presentInServers,
    maxRetries,
    waitTimeBetweenRetries,
    shouldStop
  )
  inflightForStorage.set(hashToDownload, downloadWithRetriesJob)

  try {
    await downloadWithRetriesJob
    return
  } finally {
    // Only clear our own entry. Since a caller whose shared job rejected falls through and registers
    // a replacement, the slot may already hold someone else's in-flight job — evicting that would
    // un-deduplicate it and let the next caller start a second transfer of the same file.
    if (inflightForStorage.get(hashToDownload) === downloadWithRetriesJob) {
      inflightForStorage.delete(hashToDownload)
    }
  }
}

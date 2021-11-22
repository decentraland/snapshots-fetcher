import { RemoteEntityDeployment, SnapshotsFetcherComponents } from './types'
import { fetchJson, saveToDisk } from './utils'

export async function getGlobalSnapshot(components: SnapshotsFetcherComponents, server: string, retries: number) {
  // TODO: validate response
  return await components.downloadQueue.scheduleJobWithRetries(
    () => fetchJson(`${server}/content/snapshot`, components.fetcher),
    retries
  )
}

export async function* fetchJsonPaginated<T>(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  url: string,
  selector: (responseBody: any) => T[]
): AsyncIterable<T> {
  // Perform the different queries
  let currentUrl = url
  while (currentUrl) {
    const res = await components.fetcher.fetch(currentUrl)
    if (!res.ok) {
      throw new Error(
        'Error while requesting deployments to the url ' +
          currentUrl +
          '. Status code was: ' +
          res.status +
          ' Response text was: ' +
          JSON.stringify(await res.text())
      )
    }
    const partialHistory: any = await res.json()
    for (const elem of selector(partialHistory)) {
      yield elem
    }
    const nextRelative: string | void = partialHistory.pagination.next
    if (!nextRelative) break
    currentUrl = new URL(nextRelative, currentUrl).toString()
  }
}

export function fetchPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  server: string,
  fromTimestamp: number
): AsyncIterable<RemoteEntityDeployment> {
  const url = new URL(`/content/pointer-changes?from=${encodeURIComponent(fromTimestamp)}`, server).toString()
  return fetchJsonPaginated(components, url, ($) => $.deltas)
}

export async function saveContentFileToDisk(server: string, hash: string, destinationFilename: string) {
  const url = new URL(`/content/contents/${hash}`, server).toString()

  await saveToDisk(url, destinationFilename)
  // TODO: Check Hash validity or throw
  return
}

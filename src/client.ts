import { EntityDeployment, RemoteEntityDeployment, SnapshotsFetcherComponents } from './types'
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

    if (partialHistory.pagination) {
      const nextRelative: string | void = partialHistory.pagination.next
      if (!nextRelative) break
      currentUrl = new URL(nextRelative, currentUrl).toString()
    }
  }
}

export function fetchPointerChanges(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  server: string,
  fromTimestamp: number
): AsyncIterable<RemoteEntityDeployment> {
  const url = new URL(
    `/content/pointer-changes?sortingOrder=ASC&sortingField=localTimestamp&from=${encodeURIComponent(fromTimestamp)}`,
    server
  ).toString()
  return fetchJsonPaginated(components, url, ($) => $.deltas)
}

export async function saveContentFileToDisk(server: string, hash: string, destinationFilename: string) {
  const url = new URL(`/content/contents/${hash}`, server).toString()

  return await saveToDisk(url, destinationFilename, hash)
}

export async function getEntityById(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  entityId: string,
  server: string
): Promise<EntityDeployment> {
  const url = new URL(`/content/deployments/?entityId=${encodeURIComponent(entityId)}&fields=auditInfo,content`, server)

  const response = await fetchJson(url.toString(), components.fetcher)

  if (!response.deployments[0]) {
    throw new Error(`The entity ${entityId} could not be found in server ${server}`)
  }

  return response.deployments[0]
}

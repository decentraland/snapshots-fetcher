// TODO: first download files to tmp folder, then validate CID and if valid, move to downloads folder

import { fetchJson, pickLeastRecentlyUsedServer } from './utils'
import { EntityDeployment, EntityHash, Server, SnapshotsFetcherComponents, Timestamp } from './types'
import { downloadFileWithRetries } from './downloader'

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

export async function downloadEntityAndContentFiles(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  entityId: EntityHash,
  presentInServers: string[],
  serverMapLRU: Map<Server, number>,
  targetFolder: string
) {
  // download entity metadata + audit info
  const serverToUse = pickLeastRecentlyUsedServer(presentInServers, serverMapLRU)
  const entityMetadata = await getEntityById(components, entityId, serverToUse)

  // download entity file
  await downloadFileWithRetries(entityId, targetFolder, presentInServers, serverMapLRU)

  if (!entityMetadata.content) {
    throw new Error(`The entity ${entityId} does not contain .content`)
  }

  await Promise.all(
    entityMetadata.content.map((content) =>
      downloadFileWithRetries(content.hash, targetFolder, presentInServers, serverMapLRU)
    )
  )

  return entityMetadata
}

/*
  getDeployedEntities: -> downloadContent() -> deployLocally()
    - fetch all snapshots // and pointer-changes
    - dedup
*/

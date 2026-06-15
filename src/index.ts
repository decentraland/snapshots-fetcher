import { ILoggerComponent } from '@well-known-components/interfaces'
import PQueue from 'p-queue'
import { downloadFileWithRetries } from './downloader'
import {
  ContentMapping,
  EntityHash,
  Server,
  SnapshotsFetcherComponents,
} from './types'
import { streamToBuffer } from './utils'

export { metricsDefinitions } from './metrics'
export { IDeployerComponent, SynchronizerComponent } from './types'
export { createSynchronizer } from './synchronizer'
export { getDeployedEntitiesStreamFromSnapshot, getDeployedEntitiesStreamFromPointerChanges } from './stream-entities'

// Default cap on content files downloaded in parallel per entity, so a huge content[] can't exhaust
// sockets / file descriptors. Overridable via downloadEntityAndContentFiles's last argument.
const DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY = 10

if (parseInt(process.version.split('.')[0]) < 16) {
  const { name } = require('../package.json')
  throw new Error(`In order to work, the package ${name} needs to run in Node v16 or newer to handle streams properly.`)
}

async function downloadProfileAvatars(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  logger: ILoggerComponent.ILogger,
  presentInServers: string[],
  _serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  concurrency: number,
  entityMetadata:
    {
      type: string
      metadata?: any
      content?: ContentMapping[] | undefined
    }) {
  const allAvatars: any[] = entityMetadata.metadata?.avatars ?? []
  const snapshots = allAvatars.flatMap(avatar => Object.values(avatar.avatar.snapshots ?? {}) as string[])
    .filter(snapshot => !!snapshot)
    .map(snapshot => {
      const matches = snapshot.match(/^http.*\/content\/contents\/(.*)/)
      return matches ? matches[1] : snapshot
    })
    .filter(snapshot => !entityMetadata.content || entityMetadata.content.find(content => content.hash === snapshot) === undefined)
  if (snapshots.length > 0) {
    logger.info(`Downloading snapshots ${snapshots} for fixing entity ${JSON.stringify(entityMetadata)}`)
    const downloadQueue = new PQueue({ concurrency })
    await Promise.all(
      snapshots.map(snapshot => downloadQueue.add(() => downloadFileWithRetries(
        components,
        snapshot,
        targetFolder,
        presentInServers,
        _serverMapLRU,
        maxRetries,
        waitTimeBetweenRetries
      ).catch(() => logger.info(`File ${snapshot} not available for download.`))
      ))
    )
  }
}

/**
 * Downloads an entity and its dependency files to a folder in the disk.
 *
 * Returns the parsed JSON file of the deployed entityHash
 * @param contentFilesConcurrency - Maximum number of content files to download in parallel for this
 *   entity. Defaults to {@link DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY} (10).
 * @public
 */
export async function downloadEntityAndContentFiles(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  entityId: EntityHash,
  presentInServers: string[],
  _serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  contentFilesConcurrency: number = DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY
): Promise<unknown> {
  const logger = components.logs.getLogger(`downloadEntityAndContentFiles)`)

  // download entity file
  await downloadFileWithRetries(
    components,
    entityId,
    targetFolder,
    presentInServers,
    _serverMapLRU,
    maxRetries,
    waitTimeBetweenRetries
  )

  const content = await components.storage.retrieve(entityId)

  if (!content) {
    throw new Error(`Entity file ${entityId} could not be retrieved from storage after download`)
  }

  const stream = await content.asStream()
  const buffer = await streamToBuffer(stream)
  const contentStream = buffer.toString()

  const entityMetadata: {
    type: string,
    metadata?: any,
    content?: Array<ContentMapping>
  } = JSON.parse(contentStream)

  if (entityMetadata.type === 'profile' && entityMetadata.metadata) {
    /*
     * Profiles can have some images referenced in the avatar snapshots that are not included in content section.
     * Why can this happen? Because a previous version of the profile did include those images in the content, and
     * later on a new version of the profile decided not to include it (perhaps to avoid uploading it again) but is
     * still referencing it in snapshots.
     * This fix downloads those files not referenced in content but that are anyway referenced from snapshots.
     * A proper fix needs to be added that validates and forces new deployments to include the files in content (even
     *  if no need to upload them again).
     */
    await downloadProfileAvatars(
      components,
      logger,
      presentInServers,
      _serverMapLRU,
      targetFolder,
      maxRetries,
      waitTimeBetweenRetries,
      contentFilesConcurrency,
      entityMetadata)
  }

  if (entityMetadata.content) {
    const downloadQueue = new PQueue({ concurrency: contentFilesConcurrency })
    await Promise.all(
      entityMetadata.content.map((content) =>
        downloadQueue.add(() =>
          downloadFileWithRetries(
            components,
            content.hash,
            targetFolder,
            presentInServers,
            _serverMapLRU,
            maxRetries,
            waitTimeBetweenRetries
          )
        )
      )
    )
  }

  return entityMetadata
}

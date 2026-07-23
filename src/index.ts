import { hashV0, hashV1 } from '@dcl/hashing'
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

/**
 * Verifies stored bytes against the content-addressed id that claims them. 'mismatch' proves the
 * local copy is not the content the id addresses (a truncated/partial or mis-keyed write); 'match'
 * proves it is byte-identical to what a re-download would return. An unknown scheme or a hashing
 * failure proves nothing either way.
 */
type HashVerification = 'match' | 'mismatch' | 'unverifiable'

async function verifyBufferHash(buffer: Uint8Array, entityId: string): Promise<HashVerification> {
  try {
    if (entityId.startsWith('Qm')) {
      return (await hashV0(buffer)) === entityId ? 'match' : 'mismatch'
    }
    if (entityId.startsWith('ba')) {
      return (await hashV1(buffer)) === entityId ? 'match' : 'mismatch'
    }
  } catch {
    // fall through: unprovable
  }
  return 'unverifiable'
}

if (parseInt(process.versions.node.split('.')[0], 10) < 22) {
  const { name } = require('../package.json')
  throw new Error(`In order to work, the package ${name} needs to run in Node v22 or newer to handle streams properly.`)
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

  // Read the bytes outside any destructive path. A failure here is a transient storage/read error,
  // not proof the stored bytes are corrupt, so it must not trigger an eviction.
  const stream = await content.asStream()
  const buffer = await streamToBuffer(stream)
  const contentStream = buffer.toString()

  // Enforce the content-addressed invariant BEFORE trusting the bytes at all. `downloadJob` skips
  // the download when `storage.exist(entityId)` is true, so a truncated/partial or mis-keyed local
  // file is served here unverified — and if it happens to parse as JSON it would otherwise be
  // processed as the wrong entity. A proven mismatch is the only ground for the destructive
  // eviction: hash-valid bytes would be re-downloaded byte-identical, so removing them never helps,
  // and a malformed (or malicious) remote feed advertising the hash of an already-cached non-JSON
  // content file must not be able to delete that legitimate content. Left in place, a corrupt copy
  // is a permanent poison pill: every retry re-reads the same bytes and re-fails. The eviction is
  // best-effort: if it fails, the descriptive error still wins and the next retry re-attempts it.
  const verification = await verifyBufferHash(buffer, entityId)
  if (verification === 'mismatch') {
    let evicted = false
    try {
      await components.storage.delete([entityId])
      evicted = true
    } catch {
      // keep evicted = false; the error thrown below reflects that the copy could not be removed
    }
    if (evicted) {
      components.metrics.increment('dcl_corrupt_entity_files_evicted_total')
    }
    const outcome = evicted
      ? 'removed the corrupt local copy so it can be re-downloaded'
      : 'could not remove the corrupt local copy; a later retry will attempt it again'
    throw new Error(`The stored entity file for ${entityId} failed content-hash verification; ${outcome}.`)
  }

  let entityMetadata: {
    type: string
    metadata?: any
    content?: Array<ContentMapping>
  }
  try {
    if (contentStream === '') {
      throw new Error('the stored entity file was empty')
    }
    entityMetadata = JSON.parse(contentStream)
  } catch (error: any) {
    // The bytes are hash-valid (or unverifiable) yet not entity JSON, so this is not local
    // corruption — surface an entity-scoped error without touching the stored file.
    const cause = error?.message ?? String(error)
    const kept =
      verification === 'match'
        ? 'the stored bytes match the content hash, so the local copy was kept'
        : 'the stored bytes could not be proven corrupt (unverifiable hash scheme), so the local copy was kept'
    throw new Error(`Failed to parse the downloaded entity file for ${entityId}; ${kept}. Cause: ${cause}`)
  }

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

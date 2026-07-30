import { hashV0, hashV1 } from '@dcl/hashing'
import { ILoggerComponent } from '@well-known-components/interfaces'
import PQueue from 'p-queue'
import { downloadFileWithRetries } from './downloader'
import { ContentMapping, EntityHash, Server, SnapshotsFetcherComponents, TransferLimits } from './types'
import { isValidContentHash, streamToBuffer, truncateForLog } from './utils'

// Guard the runtime before anything else in the package is evaluated. The name is inlined rather than
// read from package.json so this stays a plain import-time check with no filesystem access.
// Raised from 22 to 24 by @dcl/catalyst-storage@5, whose own engines field requires >=24.
const MINIMUM_NODE_MAJOR_VERSION = 24
if (parseInt(process.versions.node.split('.')[0], 10) < MINIMUM_NODE_MAJOR_VERSION) {
  throw new Error(
    `In order to work, the package @dcl/snapshots-fetcher needs to run in Node v${MINIMUM_NODE_MAJOR_VERSION} or newer to handle streams properly.`
  )
}

export { metricsDefinitions } from './metrics'
export { createSynchronizer } from './synchronizer'
export { getDeployedEntitiesStreamFromSnapshot, getDeployedEntitiesStreamFromPointerChanges } from './stream-entities'
// createSynchronizer requires a full SnapshotsFetcherComponents, so everything needed to build one
// has to be reachable from the package root. Without this, callers had to deep-import
// '@dcl/snapshots-fetcher/dist/job-queue-port' or reimplement IJobQueue themselves.
export { createJobQueue, IJobQueue } from './job-queue-port'
export { DEFAULT_TRANSFER_LIMITS, resolveTransferLimits } from './utils'
// Tagged @public but previously reachable only through a deep import into dist/.
export {
  decideSnapshotDeploymentFromProcessedSet,
  shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded
} from './deploy-entities'
export {
  ContentMapping,
  DeployableEntity,
  DeployedEntityStreamCommonOptions,
  EntityHash,
  IDeployerComponent,
  IDownloadQueue,
  IProcessedSnapshotStorageComponent,
  ISnapshotStorageComponent,
  Path,
  PointerChangesDeployedEntityStreamOptions,
  ResolvedTransferLimits,
  TransferLimits,
  ReconnectionOptions,
  Server,
  SnapshotDeployedEntityStreamOptions,
  SnapshotMetadata,
  SnapshotsFetcherComponents,
  SyncJob,
  SynchronizerComponent,
  SynchronizerConcurrencyOptions,
  SynchronizerOptions,
  TimeRange
} from './types'

// Default cap on content files downloaded in parallel per entity, so a huge content[] can't exhaust
// sockets / file descriptors. Overridable via downloadEntityAndContentFiles's last argument.
const DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY = 10

/**
 * Minimal scheduler contract for putting content transfers behind one caller-owned global limit.
 * The queue returned by {@link createJobQueue} satisfies it.
 * @public
 */
export type ContentDownloadScheduler = {
  scheduleJob<T>(fn: () => Promise<T>): Promise<T>
}

/**
 * A downloaded entity together with the verified bytes already read from storage.
 * @public
 */
export type DownloadedEntity = {
  entity: unknown
  entityFile: Buffer
}

function scheduleContentDownload<T>(
  scheduler: ContentDownloadScheduler | undefined,
  localQueue: PQueue,
  fn: () => Promise<T>
): Promise<T> {
  return scheduler ? scheduler.scheduleJob(fn) : localQueue.add(fn)
}

// Ceiling on the avatar snapshots a single profile can ask us to fetch. Far above any real profile
// (a handful per avatar), so it only bounds hostile or corrupt metadata.
const MAX_AVATAR_SNAPSHOTS_PER_ENTITY = 1000

// Ceiling on the content files one entity may ask us to download. content[] is remote, and every entry
// becomes a queued job in a single pass, so without a bound one entity sizes the work. Far above any real
// entity (scenes run to hundreds of assets), so it only trips on a hostile or corrupt manifest.
const MAX_CONTENT_FILES_PER_ENTITY = 25_000

/**
 * The distinct, valid hashes to fetch for an entity's content[].
 *
 * Deduplicated because a manifest may name the same hash repeatedly: the download layer already collapses
 * concurrent duplicates, but each one still costs a queue slot and a closure here.
 *
 * Invalid entries are rejected rather than skipped. Every file in content[] is part of the entity, so
 * quietly dropping one would report success for an entity we did not fully fetch — the download call
 * already refused these, this just fails earlier and names the entity. Exceeding the cap is refused for
 * the same reason: truncating would silently leave files behind.
 */
function contentHashesToDownload(content: unknown[], entityId: EntityHash): string[] {
  if (content.length > MAX_CONTENT_FILES_PER_ENTITY) {
    throw new Error(
      `Entity ${entityId} declares ${content.length} content files, above the maximum of ${MAX_CONTENT_FILES_PER_ENTITY}`
    )
  }
  const hashes = new Set<string>()
  for (const entry of content) {
    const hash = (entry as ContentMapping | undefined | null)?.hash
    if (typeof hash !== 'string' || !isValidContentHash(hash)) {
      // Truncated for the same reason as the downloader's: an entry that failed validation can be any
      // length the manifest chose.
      throw new Error(
        `Entity ${entityId} declares an invalid content file hash: ${truncateForLog(String(JSON.stringify(hash)))}`
      )
    }
    hashes.add(hash)
  }
  return Array.from(hashes)
}

// Ceiling on an entity file read fully into memory. Entity documents are JSON manifests measured in
// KB, but the download cap alone permits a single file of 1 GiB and entities are read concurrently —
// the eviction path holds two copies of one at a time. Far above any real entity, so it only bounds
// a hostile or corrupt file.
const MAX_ENTITY_FILE_SIZE_IN_BYTES = 50 * 1024 * 1024 // 50 MiB

/**
 * Verifies stored bytes against the content-addressed id that claims them. 'mismatch' proves the
 * local copy is not the content the id addresses (a truncated/partial or mis-keyed write); 'match'
 * proves it is byte-identical to what a re-download would return. An unknown scheme, a
 * syntactically invalid CID or a hashing failure proves nothing either way — a computed hash can
 * never equal a malformed id, so treating such ids as verifiable would turn every one of them into
 * grounds for destructive eviction.
 */
type HashVerification = 'match' | 'mismatch' | 'unverifiable'

// hashV0 emits Qm + 44 base58btc chars; hashV1 emits ba + 57 lowercase base32 chars (sha256 CIDv1,
// the only shapes content entities use). Ids outside these shapes are unverifiable, not mismatched.
const HASH_V0_CID = /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/
const HASH_V1_CID = /^ba[a-z2-7]{57}$/

async function verifyBufferHash(buffer: Uint8Array, entityId: string): Promise<HashVerification> {
  try {
    if (HASH_V0_CID.test(entityId)) {
      return (await hashV0(buffer)) === entityId ? 'match' : 'mismatch'
    }
    if (HASH_V1_CID.test(entityId)) {
      return (await hashV1(buffer)) === entityId ? 'match' : 'mismatch'
    }
  } catch {
    // fall through: unprovable
  }
  return 'unverifiable'
}

// Serializes evictions per entity (same pattern as the module-scoped download-job map above). Two
// callers can read the same corrupt copy concurrently; without serialization + re-verification, the
// first eviction plus a retry can heal the cache with a fresh good copy that the second (stale)
// caller then deletes based on the old bytes it already read.
const inflightEvictions = new Map<string, Promise<unknown>>()
function withEvictionLock<T>(entityId: string, fn: () => Promise<T>): Promise<T> {
  const prev = inflightEvictions.get(entityId) ?? Promise.resolve()
  const run = prev.then(fn, fn)
  const guard = run.then(
    () => undefined,
    () => undefined
  )
  inflightEvictions.set(entityId, guard)
  void guard.then(() => {
    if (inflightEvictions.get(entityId) === guard) inflightEvictions.delete(entityId)
  })
  return run
}

/**
 * Evicts the stored entity file after its bytes failed hash verification, and returns a
 * human-readable outcome for the caller's error message. Runs under a per-entity lock and
 * re-verifies the bytes CURRENTLY in storage — not the ones the caller read earlier — so a copy
 * that was already healed (evicted and re-downloaded hash-valid) by a concurrent caller is never
 * deleted on stale evidence.
 */
async function evictCorruptEntityFile(
  components: Pick<SnapshotsFetcherComponents, 'storage' | 'metrics'>,
  entityId: string
): Promise<string> {
  return withEvictionLock(entityId, async () => {
    try {
      const current = await components.storage.retrieve(entityId)
      if (!current) {
        // A concurrent eviction already removed it.
        return 'the corrupt local copy was already removed; a later retry will re-download it'
      }
      const currentBuffer = await streamToBuffer(await current.asStream(), MAX_ENTITY_FILE_SIZE_IN_BYTES)
      if ((await verifyBufferHash(currentBuffer, entityId)) !== 'mismatch') {
        return 'the local copy has since been replaced by a hash-valid one, which was kept'
      }
    } catch {
      // The re-read failed (transient storage error): deleting on stale evidence alone is not safe.
      return 'the local copy could not be re-verified; it was kept and a later retry will re-check it'
    }
    try {
      await components.storage.delete([entityId])
    } catch {
      return 'could not remove the corrupt local copy; a later retry will attempt it again'
    }
    components.metrics.increment('dcl_corrupt_entity_files_evicted_total')
    return 'removed the corrupt local copy so it can be re-downloaded'
  })
}

/**
 * Collects the avatar snapshot hashes that are referenced by a profile but absent from its
 * content[]. Entity metadata is remote, untrusted and not schema-validated here, so every hop is
 * shape-checked: a single malformed profile must not abort the download of an otherwise valid
 * entity. Unexpected shapes contribute no hashes rather than throwing.
 */
function avatarSnapshotHashesFrom(entityMetadata: { metadata?: any; content?: ContentMapping[] | undefined }): {
  hashes: string[]
  truncated: boolean
} {
  const allAvatars: unknown = entityMetadata.metadata?.avatars
  if (!Array.isArray(allAvatars)) {
    return { hashes: [], truncated: false }
  }

  const declaredContentHashes = new Set(
    (Array.isArray(entityMetadata.content) ? entityMetadata.content : []).map((content) => content?.hash)
  )

  // Collected with the cap enforced as we go, rather than expanding everything and slicing at the end.
  // avatars[] has no declared limit and the entity file may be MAX_ENTITY_FILE_SIZE_IN_BYTES, so a
  // flatMap over every avatar's snapshots materialised the whole attacker-controlled expansion first and
  // only then discarded it. Stopping at the cap means the intermediate never exceeds it either.
  const hashes: string[] = []
  const alreadyCollected = new Set<string>()
  // Reported rather than swallowed, so an operator can tell "this profile had no missing snapshots" from
  // "this profile declared more than we are willing to fetch, and the rest were dropped".
  let truncated = false

  for (const avatar of allAvatars) {
    if (hashes.length >= MAX_AVATAR_SNAPSHOTS_PER_ENTITY) {
      truncated = true
      break
    }
    const snapshots: unknown = avatar?.avatar?.snapshots
    // Must be an object of named snapshots. Object.values on a *string* yields one entry per character,
    // so a long string would expand into a download job per character — a ~50 MB metadata value is
    // millions of queued jobs and gigabytes of heap, from a profile whose hash the attacker controls and
    // which therefore passes verification.
    if (!snapshots || typeof snapshots !== 'object') {
      continue
    }
    for (const declared of Object.values(snapshots)) {
      if (hashes.length >= MAX_AVATAR_SNAPSHOTS_PER_ENTITY) {
        truncated = true
        break
      }
      if (typeof declared !== 'string' || declared.length === 0) {
        continue
      }
      const matches = declared.match(/^http.*\/content\/contents\/(.*)/)
      const hash = matches ? matches[1] : declared
      // Deduplicated for the same reason content[] is: a profile naming one snapshot repeatedly would
      // otherwise spend the whole budget on jobs for a single hash.
      if (declaredContentHashes.has(hash) || alreadyCollected.has(hash)) {
        continue
      }
      alreadyCollected.add(hash)
      hashes.push(hash)
    }
  }

  return { hashes, truncated }
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
  transferLimits: TransferLimits | undefined,
  entityId: EntityHash,
  entityMetadata: {
    type: string
    metadata?: any
    content?: ContentMapping[] | undefined
  },
  downloadQueue: PQueue,
  scheduler?: ContentDownloadScheduler
) {
  const { hashes: snapshots, truncated } = avatarSnapshotHashesFrom(entityMetadata)
  if (truncated) {
    logger.warn('Profile declared more avatar snapshots than will be fetched; the rest were dropped.', {
      entityId,
      limit: String(MAX_AVATAR_SNAPSHOTS_PER_ENTITY)
    })
  }
  if (snapshots.length === 0) {
    return
  }

  // Only the ids, never the metadata: a profile carries the owner's avatar fields and ethAddress, and
  // serialising the whole document put that in an info-level log line for every affected profile.
  logger.info('Downloading avatar snapshots missing from the entity content', {
    entityId,
    snapshots: truncateForLog(snapshots.join(','))
  })
  await Promise.all(
    snapshots.map((snapshot) =>
      scheduleContentDownload(scheduler, downloadQueue, () =>
        downloadFileWithRetries(
          components,
          snapshot,
          targetFolder,
          presentInServers,
          _serverMapLRU,
          maxRetries,
          waitTimeBetweenRetries,
          undefined,
          transferLimits
        ).catch(() =>
          // Truncated: the value comes from remote profile metadata, and this path is reached precisely
          // when it was unusable, so its length is the manifest's choice.
          logger.info('Avatar snapshot not available for download.', { entityId, snapshot: truncateForLog(snapshot) })
        )
      )
    )
  )
}

async function downloadEntityAndContentFilesWithResult(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  entityId: EntityHash,
  presentInServers: string[],
  serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  contentFilesConcurrency: number,
  transferLimits?: TransferLimits,
  scheduler?: ContentDownloadScheduler
): Promise<DownloadedEntity> {
  // Checked before any work, not where the queue is built: p-queue rejects a bad concurrency with its own
  // TypeError, and by then the entity file and the profile-avatar fallbacks have already been downloaded.
  // A caller passing 0 deserves to be told in this package's terms, before spending requests.
  if (!Number.isInteger(contentFilesConcurrency) || contentFilesConcurrency < 1) {
    throw new Error(`contentFilesConcurrency must be an integer >= 1, got ${contentFilesConcurrency}`)
  }
  const downloadQueue = new PQueue({ concurrency: contentFilesConcurrency })

  const logger = components.logs.getLogger(`downloadEntityAndContentFiles)`)

  // download entity file
  await downloadFileWithRetries(
    components,
    entityId,
    targetFolder,
    presentInServers,
    serverMapLRU,
    maxRetries,
    waitTimeBetweenRetries,
    undefined,
    transferLimits
  )

  const content = await components.storage.retrieve(entityId)

  if (!content) {
    throw new Error(`Entity file ${entityId} could not be retrieved from storage after download`)
  }

  // Read the bytes outside any destructive path. A failure here is a transient storage/read error,
  // not proof the stored bytes are corrupt, so it must not trigger an eviction.
  const stream = await content.asStream()
  const buffer = await streamToBuffer(stream, MAX_ENTITY_FILE_SIZE_IN_BYTES)

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
    const outcome = await evictCorruptEntityFile(components, entityId)
    throw new Error(`The stored entity file for ${entityId} failed content-hash verification; ${outcome}.`)
  }

  // Decode only bytes that survived (or could not be subjected to) hash verification.
  const contentStream = buffer.toString()

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
  } catch (error: unknown) {
    // The bytes are hash-valid (or unverifiable) yet not entity JSON, so this is not local
    // corruption — surface an entity-scoped error without touching the stored file.
    const cause = error instanceof Error ? error.message : String(error)
    const kept =
      verification === 'match'
        ? 'the stored bytes match the content hash, so the local copy was kept'
        : 'the stored bytes could not be proven corrupt (unverifiable hash scheme), so the local copy was kept'
    throw new Error(`Failed to parse the downloaded entity file for ${entityId}; ${kept}. Cause: ${cause}`)
  }

  // Checked before any download work, because the alternative is worse than a TypeError: every consumer
  // of content[] guards with Array.isArray and falls back to "no content", so an entity declaring an
  // object, a string or an explicit null here would be reported as fully downloaded while its
  // dependencies were never fetched — and then deployed as complete.
  //
  // null is rejected along with the rest: @dcl/schemas declares content as a required array on Entity, so
  // a present-but-null field is not something a conforming server sends. Only its absence means "no
  // content declared", and absence is the one case we can read as empty without guessing.
  const declaredContent: unknown = entityMetadata.content
  if (declaredContent !== undefined && !Array.isArray(declaredContent)) {
    throw new Error(
      `Entity ${entityId} declares a content field that is not an array: ${
        declaredContent === null ? 'null' : typeof declaredContent
      }. Refusing to treat it as having no content files.`
    )
  }

  // Resolved before any download work, including the profile-avatar fallback below. Validation lives in
  // here, so computing it late meant a manifest with one unusable content hash could spend the whole
  // bounded avatar budget first and only then be rejected — work for an entity that was never going to
  // be accepted.
  const hashesToDownload = Array.isArray(declaredContent) ? contentHashesToDownload(declaredContent, entityId) : []

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
      serverMapLRU,
      targetFolder,
      maxRetries,
      waitTimeBetweenRetries,
      contentFilesConcurrency,
      transferLimits,
      entityId,
      entityMetadata,
      downloadQueue,
      scheduler
    )
  }

  if (hashesToDownload.length > 0) {
    await Promise.all(
      hashesToDownload.map((hash) =>
        scheduleContentDownload(scheduler, downloadQueue, () =>
          downloadFileWithRetries(
            components,
            hash,
            targetFolder,
            presentInServers,
            serverMapLRU,
            maxRetries,
            waitTimeBetweenRetries,
            undefined,
            transferLimits
          )
        )
      )
    )
  }

  return { entity: entityMetadata, entityFile: buffer }
}

/**
 * Downloads an entity and its dependency files to storage and returns its parsed JSON document.
 *
 * @remarks When the locally stored entity file fails content-hash verification, the corrupt copy is
 * evicted and the call throws so a retry can download a clean copy.
 * @param contentFilesConcurrency - Per-entity limit used when no global scheduler is supplied.
 * @param scheduler - Optional caller-owned scheduler that globally bounds content transfers.
 * @public
 */
export async function downloadEntityAndContentFiles(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  entityId: EntityHash,
  presentInServers: string[],
  serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  contentFilesConcurrency: number = DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY,
  transferLimits?: TransferLimits,
  scheduler?: ContentDownloadScheduler
): Promise<unknown> {
  return (
    await downloadEntityAndContentFilesWithResult(
      components,
      entityId,
      presentInServers,
      serverMapLRU,
      targetFolder,
      maxRetries,
      waitTimeBetweenRetries,
      contentFilesConcurrency,
      transferLimits,
      scheduler
    )
  ).entity
}

/**
 * Downloads an entity and its content while returning the verified entity bytes so an immediate
 * deploy does not retrieve and read the same storage item again.
 * @public
 */
export async function downloadEntityAndContentFilesWithBuffer(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  entityId: EntityHash,
  presentInServers: string[],
  serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  contentFilesConcurrency: number = DEFAULT_ENTITY_FILE_DOWNLOAD_CONCURRENCY,
  transferLimits?: TransferLimits,
  scheduler?: ContentDownloadScheduler
): Promise<DownloadedEntity> {
  return downloadEntityAndContentFilesWithResult(
    components,
    entityId,
    presentInServers,
    serverMapLRU,
    targetFolder,
    maxRetries,
    waitTimeBetweenRetries,
    contentFilesConcurrency,
    transferLimits,
    scheduler
  )
}

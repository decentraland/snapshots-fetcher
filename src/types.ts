import { IContentStorageComponent } from '@dcl/catalyst-storage'
import { SyncDeployment } from '@dcl/schemas'
import { IBaseComponent, ILoggerComponent, IMetricsComponent } from '@well-known-components/interfaces'
import { IFetchComponent } from '@dcl/core-commons'
import { ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { IJobQueue } from './job-queue-port'
import { metricsDefinitions } from './metrics'

/**
 * @public
 */
export type EntityHash = string

/**
 * @public
 */
export type Server = string

/**
 * @public
 */
export type Path = string

/**
 * @public
 */
export type ContentMapping = { file: string; hash: string }

/**
 * The only part of IJobQueue this package actually calls. Requiring just this lets callers supply
 * their own bounded scheduler without implementing the queue methods nothing here uses; the queue
 * returned by createJobQueue satisfies it as-is.
 * @public
 */
export type IDownloadQueue = Pick<IJobQueue, 'scheduleJobWithRetries'>

/**
 * Components needed by the DeploymentsFetcher to work
 * @public
 */
export type SnapshotsFetcherComponents = {
  metrics: IMetricsComponent<keyof typeof metricsDefinitions>
  fetcher: IFetchComponent
  downloadQueue: IDownloadQueue
  logs: ILoggerComponent
  storage: IContentStorageComponent
  processedSnapshotStorage: IProcessedSnapshotStorageComponent
  snapshotStorage: ISnapshotStorageComponent
}

/**
 * Bounds applied to an individual HTTP transfer.
 *
 * Supplied per call, on the options bag, rather than once on the components bag: a components container
 * is a fixed set of members — this package's own test harness proxies it and throws on any name it does
 * not know — so an optional configuration field read from it is not safe to assume present. Config is
 * not a component.
 *
 * The defaults are the values this package used before they were configurable, so omitting the field, or
 * any field within it, changes nothing.
 * @public
 */
export type TransferLimits = {
  /**
   * Deadline for a **JSON request's** body read — `/snapshots` and `/pointer-changes`. Refreshed on every
   * chunk received, so it is an inactivity deadline rather than a total one. Content-file downloads have
   * their own, `downloadInactivityTimeoutInMs`, because a file legitimately takes longer than a JSON
   * document and one value cannot serve both. Must be an integer >= 1.
   * @defaultValue 15000
   */
  requestTimeoutInMs?: number
  /**
   * Deadline for a content-file download, measured from the last byte received on the socket rather than
   * from the start, so a large file making steady progress is never cut off. Must be an integer >= 1.
   * @defaultValue 30000
   */
  downloadInactivityTimeoutInMs?: number
  /**
   * Hard ceiling on the bytes of a single content file. Applied on **both sides** of a gzip boundary: to
   * the decompressed bytes written to disk, which is the gzip-bomb bound, and to the compressed response
   * as well.
   *
   * The compressed bound is needed because a peer can stream valid gzip indefinitely while producing no
   * decompressed output at all — concatenated empty gzip members are legal — so a bound only on the
   * decompressed side never receives a byte to measure. For real content it never binds: gzip exceeds its
   * input only for incompressible data, and then by about 0.03%. A failure names which side tripped.
   *
   * Must be an integer >= 1.
   * @defaultValue 1073741824
   */
  maxDownloadedFileSizeInBytes?: number
  /**
   * Minimum rate a transfer must average, once it has been running longer than
   * `transferRateGracePeriodInMs`, to be allowed to continue. This is what stops a peer trickling bytes
   * to hold a slot indefinitely: the inactivity deadline above only asks whether bytes are still
   * arriving, this asks whether they add up to progress.
   *
   * Lower it on a constrained link. Must be an integer >= 0; **0 disables the check**, which restores
   * the pre-3.0.0 behaviour of allowing an arbitrarily slow transfer to continue as long as it keeps
   * sending something.
   * @defaultValue 4096
   */
  minTransferRateInBytesPerSecond?: number
  /**
   * Ceiling on how many pages a single paginated call (`/pointer-changes`) will follow before giving up.
   *
   * A hostile or broken server can keep advertising a `next` link, and each page is another request to the
   * same path, so this bounds the amplification one poll can produce. The default is far above any
   * legitimate page count — at ~1000 entries per page it allows 10M deployments in one poll — so lower it
   * if you would rather cap the amplification tightly than tolerate an unusually deep backlog in a single
   * poll.
   *
   * There is deliberately no wall-clock budget for a paginated call. Pagination progress is not committed
   * until the poll completes, so aborting a long one throws the poll's work away and resumes from the last
   * confirmed timestamp — a node with a genuinely deep backlog would restart the same walk forever rather
   * than catch up. A page count degrades the same way, which is why the default is set where no real
   * backlog reaches it. Must be an integer >= 1.
   * @defaultValue 10000
   */
  maxPagesPerPaginatedCall?: number
  /**
   * How long a transfer runs before its rate is judged at all. Small responses finish well inside this,
   * and earlier samples are dominated by connection setup rather than throughput. Raise it alongside
   * lowering the floor if your peers are slow to get going. Must be an integer >= 0.
   * @defaultValue 60000
   */
  transferRateGracePeriodInMs?: number
}

/**
 * {@link TransferLimits} with every default filled in.
 * @public
 */
export type ResolvedTransferLimits = Required<TransferLimits>

export type DeployableEntity = SyncDeployment & {
  markAsDeployed?: () => Promise<void>
  snapshotHash?: string
}

/**
 * A component that handles deployments. The scheduleEntityDeployment function should be idempotent, since
 * it can be called several times with the same entity.
 * @public
 */
export type IDeployerComponent = {
  /**
   * awaiting scheduleEntityDeployment does not imply that the entity was deployed. To be marked the entity as deployed, it needs
   * to be called the function #markAsDeployed. This is useful for asynchronous deployers that uses, for example,
   * queues to deploy entities.
   */
  scheduleEntityDeployment(entity: DeployableEntity, contentServers: string[]): Promise<void>
  /**
   * onIdle returns a promise that should be resolved once every scheduleEntityDeployment(...) job has
   * finished and there are no more queued jobs.
   */
  onIdle(): Promise<void>
  /**
   * Before sending entities to schedule deployments from snapshots, this function will be called to warm up the deployer.
   * This migth be useful for cases where a warmup could improve the performance of the deployments. For example, filling up a
   * bloom filter with the already deployed entities in the specified #timeRanges.
   */
  prepareForDeploymentsIn(timeRanges: TimeRange[]): Promise<void>
}

/**
 * @deprecated
 */
export type DownloadEntitiesOptions = {
  catalystServers: string[]
  concurrency: number
  jobTimeout: number
  isEntityPresentLocally: (entityId: string) => Promise<boolean>
  contentFolder: string
  components: SnapshotsFetcherComponents
  /**
   * Entity types to fetch
   */
  entityTypes: string[]
}

/**
 * @deprecated
 */
export type DeployedEntityStreamOptions = {
  fromTimestamp?: number
  tmpDownloadFolder: string

  // - Configures pointer-changes polling
  // - When pointerChangesWaitTime == 0, the polling is disabled and the stream
  //   ends right after finishing the first iteration
  pointerChangesWaitTime: number

  // retry http requests
  requestRetryWaitTime: number
  requestMaxRetries: number

  /**
   * Delete downloaded snapshot files after usage
   * Default: true
   */
  deleteSnapshotAfterUsage?: boolean
}

/**
 * @public
 */
export type CatalystDeploymentStreamComponent = ExponentialFallofRetryComponent & {
  getGreatesProcessedTimestamp(): number
}

/**
 * @deprecated
 */
export type DeploymentHandler = (deployment: SyncDeployment, server: string) => Promise<void>

/**
 * @deprecated
 */
export type CatalystDeploymentStreamOptions = DeployedEntityStreamOptions & {
  reconnectTime: number
  /**
   * 1.1 by default
   */
  reconnectRetryTimeExponent?: number
  /**
   * defaults to one day
   */
  maxReconnectionTime?: number
}

/**
 * @public
 */
export type SynchronizerOptions = SnapshotDeployedEntityStreamOptions &
  PointerChangesDeployedEntityStreamOptions & {
    bootstrapReconnection: ReconnectionOptions
    syncingReconnection: ReconnectionOptions
    /**
     * Tunes how much work the synchronizer runs at once. Omit it, or any of its fields, to keep the
     * defaults.
     */
    concurrency?: SynchronizerConcurrencyOptions
  }

/**
 * Bounds on the work the synchronizer performs in parallel. The right values depend on the deployment:
 * available bandwidth, how fast the deployer drains, and any rate limits the remote content servers
 * apply. Every field must be an integer >= 1.
 * @public
 */
export type SynchronizerConcurrencyOptions = {
  /**
   * Snapshots being streamed and deployed at the same time. Each one holds an open snapshot file and
   * feeds entities to the deployer, so raising this multiplies both bandwidth and deployer pressure.
   * @defaultValue 10
   */
  snapshotDeployments?: number
  /**
   * Decisions about whether a snapshot needs deploying, evaluated at the same time. Each one may hit
   * `snapshotStorage`, so this mostly bounds load on that component.
   * @defaultValue 10
   */
  snapshotChecks?: number
}

/**
 * @public
 */
export type ReconnectionOptions = {
  reconnectTime: number
  /**
   * 1.1 by default
   */
  reconnectRetryTimeExponent?: number
  /**
   * defaults to one day
   */
  maxReconnectionTime?: number
}

/**
 * @public
 */
export type SnapshotDeployedEntityStreamOptions = DeployedEntityStreamCommonOptions & {
  // retry http requests
  requestRetryWaitTime: number
  requestMaxRetries: number

  tmpDownloadFolder: string
  /**
   * Delete downloaded snapshot files after usage
   * Default: true
   */
  deleteSnapshotAfterUsage?: boolean
}

export type PointerChangesDeployedEntityStreamOptions = DeployedEntityStreamCommonOptions & {
  // - Configures pointer-changes polling
  // - When pointerChangesWaitTime == 0, the polling is disabled and the stream
  //   ends right after finishing the first iteration
  pointerChangesWaitTime: number
}

/**
 * @public
 */
export type DeployedEntityStreamCommonOptions = {
  fromTimestamp?: number
  /**
   * Bounds on the individual HTTP transfers this stream performs. Omit it, or any of its fields, to keep
   * the defaults. See {@link TransferLimits}.
   */
  transferLimits?: TransferLimits
}

/**
 * @public
 */
export type ISnapshotStorageComponent = {
  has(snapshotHash: string): Promise<boolean>
}

/**
 * @public
 */
export type IProcessedSnapshotStorageComponent = {
  /**
   * It receives a list of snapshot hashes L and returns a set with hashes of the snapshots that were processed from L.
   * @param snapshotHashes - The list of snapshots hashes to be filtered.
   */
  filterProcessedSnapshotsFrom(snapshotHashes: string[]): Promise<Set<string>>
  /**
   * It receives a snapshot hash and marks it as processed.
   * @param snapshotHash - The snapshot hash to be saved as processed
   */
  markSnapshotAsProcessed(snapshotHash: string): Promise<void>
}

export type SyncJob = {
  onInitialBootstrapFinished(cb: () => Promise<void>): Promise<void>
  onSyncFinished(): Promise<void>
}

/**
 * @public
 */
export type SynchronizerComponent = IBaseComponent & {
  syncWithServers(contentServers: Set<string>): Promise<SyncJob>
}

/**
 * @public
 */
export type SnapshotMetadata = {
  hash: string
  timeRange: TimeRange
  replacedSnapshotHashes?: string[]
  /**
   * Informational only — nothing in this package reads either field.
   *
   * Kept required. They were briefly made optional so the type would match exactly what
   * isValidSnapshotMetadata verifies, but that validation was then removed (rejecting a snapshot over a
   * field we never read would silently stop us syncing from that server), so the optionality bought
   * nothing while breaking every downstream consumer that reads these as numbers.
   *
   * The residual inaccuracy is that a server omitting them yields `undefined` at runtime. Addressing
   * that properly means a separate validated-remote-response type rather than weakening this public
   * one; it is not worth a source-compatibility break for two fields this package ignores.
   */
  numberOfEntities: number
  generationTimestamp: number
}

/**
 * @public
 */
export type TimeRange = {
  initTimestamp: number
  endTimestamp: number
}

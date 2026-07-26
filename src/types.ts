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

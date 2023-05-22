import { IContentStorageComponent } from '@dcl/catalyst-storage'
import { SyncDeployment } from '@dcl/schemas'
import { IFetchComponent } from '@well-known-components/http-server'
import { IBaseComponent, ILoggerComponent, IMetricsComponent } from '@well-known-components/interfaces'
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
 * Components needed by the DeploymentsFetcher to work
 * @public
 */
export type SnapshotsFetcherComponents = {
  metrics: IMetricsComponent<keyof typeof metricsDefinitions>
  fetcher: IFetchComponent
  downloadQueue: IJobQueue
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
  numberOfEntities: number
  replacedSnapshotHashes?: string[]
  generationTimestamp: number
}

/**
 * @public
 */
export type TimeRange = {
  initTimestamp: number
  endTimestamp: number
}

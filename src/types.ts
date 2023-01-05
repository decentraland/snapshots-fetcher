import { IFetchComponent } from '@well-known-components/http-server'
import { IBaseComponent, ILoggerComponent, IMetricsComponent } from '@well-known-components/interfaces'
import { ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { IJobQueue } from './job-queue-port'
import { metricsDefinitions } from './metrics'
import { IContentStorageComponent } from '@dcl/catalyst-storage'
import { SyncDeployment } from '@dcl/schemas'

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
}

export type DeployableEntity = SyncDeployment & {
  markAsDeployed?: () => Promise<void>,
  snapshotHash?: string
}

/**
 * A component that handles deployments. The deployEntity function should be idempotent, since
 * it can be called several times with the same entity.
 * @public
 */
export type IDeployerComponent = {
  /**
   * awaiting deployEntity might not imply that the entity was deployed. To be marked the entity as deployed, it needs
   * to be called the function #markAsDeployed. This is useful for asynchronous deployers that uses, for example,
   * queues to deploy entities.
   */
  deployEntity(entity: DeployableEntity, contentServers: string[]): Promise<void>
  /**
   * onIdle returns a promise that should be resolved once every deployEntity(...) job has
   * finished and there are no more queued jobs.
   */
  onIdle(): Promise<void>
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
export type SynchronizerOptions =
  SnapshotDeployedEntityStreamOptions &
  PointerChangesDeployedEntityStreamOptions & {
    bootstrapReconnection: ReconnectionOptions,
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
export type IProcessedSnapshotStorageComponent = {
  processedFrom(snapshotHashes: string[]): Promise<Set<string>>
  saveProcessed(snapshotHash: string): Promise<void>
}

/**
 * @internal
 */
export type IProcessedSnapshotsComponent = {
  shouldProcessSnapshotAndMarkAsProcessedIfNeeded(snapshotHash: string, replacedSnapshotHashes: string[][]): Promise<boolean>
  startStreamOf(snapshotHash: string): Promise<void>
  endStreamOf(snapshotHash: string, numberOfEntitiesStreamed: number): Promise<void>
  entityProcessedFrom(snapshotHash: string): Promise<void>
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
 * @internal
 */
export type Snapshot = {
  hash: string
  lastIncludedDeploymentTimestamp: number
  replacedSnapshotHashes?: string[] | undefined
}

/**
 * @internal
 */
export type SnapshotInfo = {
  snapshotHash: string
  greatestEndTimestamp: number,
  replacedSnapshotHashes: string[][],
  servers: Set<string>
}

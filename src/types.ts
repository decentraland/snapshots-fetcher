import { IFetchComponent } from '@well-known-components/http-server'
import { ILoggerComponent, IMetricsComponent } from '@well-known-components/interfaces'
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
  processedSnapshots: IProcessedSnapshotsComponent
}

/**
 * A component that handles deployments. The deployEntity function should be idempotent, since
 * it can be called several times with the same entity.
 * @public
 */
export type IDeployerComponent = {
  deployEntity(entity: SyncDeployment & { snapshotHash?: string }, contentServers: string[]): Promise<void>
  /**
   * onIdle returns a promise that should be resolved once every deployEntity(...) job has
   * finished and there are no more queued jobs.
   */
  onIdle(): Promise<void>
}

/**
 * @public
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
 * @public
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
 * @public
 */
export type DeploymentHandler = (deployment: SyncDeployment, server: string) => Promise<void>

/**
 * @public
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

export type IProcessedSnapshotStorageComponent = {
  processedFrom(snapshotHashes: string[]): Promise<Set<string>>
  saveProcessed(snapshotHash: string): Promise<void>
}

export type IProcessedSnapshotsComponent = {
  someGroupWasProcessed(snapshotGroups: string[][]): Promise<boolean>
  startStreamOf(snapshotHash: string): Promise<void>
  endStreamOf(snapshotHash: string, numberOfEntitiesStreamed: number): Promise<void>
  entityProcessedFrom(snapshotHash: string): Promise<void>
}

/**
 * @public
 */
export type SynchronizerComponent = {
  syncFromSnapshots(contentServers: Set<string>): Promise<void>
  syncFromPointerChanges(contentServers: Set<string>): Promise<void>
  syncWithServers(contentServers: Set<string>): Promise<void>
}

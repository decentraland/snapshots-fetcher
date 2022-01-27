import { IFetchComponent } from '@well-known-components/http-server'
import { ILoggerComponent, IMetricsComponent } from '@well-known-components/interfaces'
import { ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { IJobQueue } from './job-queue-port'
import { metricsDefinitions } from './metrics'
import { Readable } from 'stream'

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
  storage: ContentStorage
}

/**
 * A component that handles deployments. The deployEntity function should be idempotent, since
 * it can be called several times with the same entity.
 * @public
 */
export type IDeployerComponent = {
  deployEntity(entity: RemoteEntityDeployment, contentServers: string[]): Promise<void>
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
  deployAction: (entity: EntityDeployment) => Promise<any>
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
  contentServer: string
  fromTimestamp?: number
  tmpDownloadFolder: string

  // configure pointer-changes polling
  pointerChangesWaitTime: number

  // retry http requests
  requestRetryWaitTime: number
  requestMaxRetries: number
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
export type DeploymentHandler = (deployment: RemoteEntityDeployment, server: string) => Promise<void>

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

/**
 * @public
 */
export type RemoteEntityDeployment = {
  entityType: string
  entityId: string
  localTimestamp: number
  authChain: any[]
}

/**
 * @public
 */
export type EntityDeployment = {
  entityId: string
  entityType: string
  content: Array<ContentMapping>
  auditInfo: { authChain: any[] }
}

export type ContentEncoding = 'gzip'

export interface ContentStorage {
  exist(ids: string): Promise<boolean>
  storeStream(id: string, fileStream: Readable): Promise<void>
  retrieve(id: string): Promise<ContentItem | undefined>
}

export interface ContentItem {
  asStream(): Promise<Readable>
}

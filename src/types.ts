import { IFetchComponent } from '@well-known-components/http-server'
import { IBaseComponent, ILoggerComponent } from '@well-known-components/interfaces'
import { ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { IJobQueue } from './job-queue-port'

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
  fetcher: IFetchComponent
  downloadQueue: IJobQueue
  logger: ILoggerComponent
}

/**
 * @public
 */
export type EntityDeployment = {
  entityId: string
  entityType: string
  content: Array<ContentMapping>
  auditInfo: any
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
  contentFolder: string

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
  onDeployment(cb: DeploymentHandler): void
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

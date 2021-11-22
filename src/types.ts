import { IFetchComponent } from '@well-known-components/http-server'
import { IJobQueue } from './job-queue-port'

export type SnapshotData = [EntityHash, Pointers][]
export type Pointer = string
export type Pointers = Pointer[]
export type EntityHash = string
export type Server = string
export type Path = string
export type Timestamp = number

/**
 * @public
 */
export type ContentMapping = { file: string; hash: string }

/**
 * @public
 */
export type Entity = { id: string; content: undefined | ContentMapping[] }

/**
 * Components needed by the DeploymentsFetcher to work
 * @public
 */
export type SnapshotsFetcherComponents = {
  fetcher: IFetchComponent
  downloadQueue: IJobQueue
}

/**
 * @public
 */
export type EntityDeployment = {
  entityId: string
  entityType: string
  content: Array<{ key: string; hash: string }>
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
export type RemoteEntityDeployment = {
  entityType: string
  entityId: string
  localTimestamp: number
  authChain: any[]
}

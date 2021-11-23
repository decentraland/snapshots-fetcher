import { IBaseComponent } from '@well-known-components/interfaces'
import { fetchPointerChanges, getEntityById, getGlobalSnapshot } from './client'
import { downloadFileWithRetries } from './downloader'
import { createExponentialFallofRetry } from './exponential-fallof-retry'
import { processDeploymentsInFile } from './file-processor'
import {
  CatalystDeploymentStreamComponent,
  CatalystDeploymentStreamOptions,
  DeployedEntityStreamOptions,
  DeploymentHandler,
  EntityDeployment,
  EntityHash,
  RemoteEntityDeployment,
  Server,
  SnapshotsFetcherComponents,
} from './types'
import { coerceEntityDeployment, pickLeastRecentlyUsedServer, sleep } from './utils'

if (parseInt(process.version.split('.')[0]) < 16) {
  const { name } = require('../package.json')
  throw new Error(`In order to work, the package ${name} needs to run in Node v16 or newer to handle streams properly.`)
}

/**
 * Downloads an entity and its dependency files to a folder in the disk.
 * @public
 */
export async function downloadEntityAndContentFiles(
  components: Pick<SnapshotsFetcherComponents, 'fetcher'>,
  entityId: EntityHash,
  presentInServers: string[],
  serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<EntityDeployment> {
  // download entity metadata + audit info
  const serverToUse = pickLeastRecentlyUsedServer(presentInServers, serverMapLRU)
  const entityMetadata = await getEntityById(components, entityId, serverToUse)

  // download entity file
  await downloadFileWithRetries(
    entityId,
    targetFolder,
    presentInServers,
    serverMapLRU,
    maxRetries,
    waitTimeBetweenRetries
  )

  if (!entityMetadata.content) {
    throw new Error(`The entity ${entityId} does not contain .content`)
  }

  await Promise.all(
    entityMetadata.content.map((content) =>
      downloadFileWithRetries(
        content.hash,
        targetFolder,
        presentInServers,
        serverMapLRU,
        maxRetries,
        waitTimeBetweenRetries
      )
    )
  )

  return entityMetadata
}

/**
 * Gets a stream of all the entities deployed to a server.
 * Includes all the entities that are already present in the server.
 * Accepts a fromTimestamp option to filter out previous deployments.
 *
 * @public
 */
export async function* getDeployedEntitiesStream(
  components: SnapshotsFetcherComponents,
  options: DeployedEntityStreamOptions
): AsyncIterable<RemoteEntityDeployment> {
  // the minimum timestamp we are looking for
  const genesisTimestamp = options.fromTimestamp || 0

  // the greatest timestamp we processed
  let greatestProcessedTimestamp = genesisTimestamp

  // 1. get the hash of the latest snapshot in the remote server, retry 10 times
  const { hash, lastIncludedDeploymentTimestamp } = await getGlobalSnapshot(
    components,
    options.contentServer,
    options.requestMaxRetries
  )

  // 2. download the snapshot file if it contains deployments
  //    in the range we are interested (>= genesisTimestamp)
  if (lastIncludedDeploymentTimestamp > genesisTimestamp) {
    // 2.1. download the shapshot file if needed
    const snapshotFilename = await downloadFileWithRetries(
      hash,
      options.contentFolder,
      [options.contentServer],
      new Map(),
      options.requestMaxRetries,
      options.requestRetryWaitTime
    )

    // 2.2. open the snapshot file and process line by line
    const deploymentsInFile = processDeploymentsInFile(snapshotFilename)
    for await (const rawDeployment of deploymentsInFile) {
      const deployment = coerceEntityDeployment(rawDeployment)
      if (!deployment) continue
      // selectively ignore deployments by localTimestamp
      if (deployment.localTimestamp >= genesisTimestamp) {
        yield deployment
      }
      // update greatest processed timestamp
      if (deployment.localTimestamp > greatestProcessedTimestamp) {
        greatestProcessedTimestamp = deployment.localTimestamp
      }
    }
  }

  // 3. fetch the /pointer-changes of the remote server using the last timestamp from the previous step
  do {
    // 3.1. download pointer changes and yield
    const pointerChanges = fetchPointerChanges(components, options.contentServer, greatestProcessedTimestamp)
    for await (const rawDeployment of pointerChanges) {
      const deployment = coerceEntityDeployment(rawDeployment)
      if (!deployment) continue
      // selectively ignore deployments by localTimestamp
      if (deployment.localTimestamp >= genesisTimestamp) {
        yield deployment
      }
      // update greatest processed timestamp
      if (deployment.localTimestamp > greatestProcessedTimestamp) {
        greatestProcessedTimestamp = deployment.localTimestamp
      }
    }

    // 3.2 repeat (3) if waitTime > 0
    await sleep(options.pointerChangesWaitTime)
  } while (options.pointerChangesWaitTime > 0)
}

export function createCatalystDeploymentStream(
  components: SnapshotsFetcherComponents,
  options: CatalystDeploymentStreamOptions
): IBaseComponent & CatalystDeploymentStreamComponent {
  let logs = components.logger.getLogger(`CatalystDeploymentStream(${options.contentServer})`)
  let greatestProcessedTimestamp = options.fromTimestamp || 0

  const handlers: DeploymentHandler[] = []

  const exponentialFallofRetryComponent = createExponentialFallofRetry(logs, {
    action,
    retryTime: options.reconnectTime,
    retryTimeExponent: 1.1,
  })

  async function action() {
    const deployments = getDeployedEntitiesStream(components, {
      ...options,
      fromTimestamp: greatestProcessedTimestamp,
    })

    for await (const deployment of deployments) {
      // if the stream is closed then we should not process more deployments
      if (exponentialFallofRetryComponent.isStopped()) {
        logs.debug('Canceling running stream')
        return
      }

      for (const cb of handlers) {
        await cb(deployment, options.contentServer)
      }

      // update greatest processed timestamp
      if (deployment.localTimestamp > greatestProcessedTimestamp) {
        greatestProcessedTimestamp = deployment.localTimestamp
      }
    }
  }

  return {
    ...exponentialFallofRetryComponent,
    onDeployment(cb: DeploymentHandler) {
      handlers.push(cb)
    },
  }
}

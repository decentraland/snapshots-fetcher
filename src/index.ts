import { ILoggerComponent } from '@well-known-components/interfaces'
import { fetchPointerChanges, getSnapshots } from './client'
import { downloadFileWithRetries } from './downloader'
import { createExponentialFallofRetry, ExponentialFallofRetryComponent } from './exponential-fallof-retry'
import { processDeploymentsInFile } from './file-processor'
import { createJobLifecycleManagerComponent, IJobWithLifecycle } from './job-lifecycle-manager'
import {
  CatalystDeploymentStreamComponent,
  CatalystDeploymentStreamOptions,
  ContentMapping,
  DeployedEntityStreamOptions,
  EntityHash,
  IDeployerComponent,
  Server,
  SnapshotsFetcherComponents,
} from './types'
import { contentServerMetricLabels, sleep, streamToBuffer } from './utils'
import { SyncDeployment } from '@dcl/schemas'

export { metricsDefinitions } from './metrics'
export { IDeployerComponent } from './types'
export { createProcessedSnapshotsComponent } from './processed-snapshots'

if (parseInt(process.version.split('.')[0]) < 16) {
  const { name } = require('../package.json')
  throw new Error(`In order to work, the package ${name} needs to run in Node v16 or newer to handle streams properly.`)
}

async function downloadProfileAvatars(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  logger: ILoggerComponent.ILogger,
  presentInServers: string[],
  _serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number,
  entityMetadata:
    {
      type: string
      metadata?: any
      content?: ContentMapping[] | undefined
    }) {
  const allAvatars: any[] = entityMetadata.metadata?.avatars ?? []
  const snapshots = allAvatars.flatMap(avatar => Object.values(avatar.avatar.snapshots ?? {}) as string[])
    .filter(snapshot => !!snapshot)
    .map(snapshot => {
      const matches = snapshot.match(/^http.*\/content\/contents\/(.*)/)
      return matches ? matches[1] : snapshot
    })
    .filter(snapshot => !entityMetadata.content || entityMetadata.content.find(content => content.hash === snapshot) === undefined)
  if (snapshots.length > 0) {
    logger.info(`Downloading snapshots ${snapshots} for fixing entity ${JSON.stringify(entityMetadata)}`)
    await Promise.all(
      snapshots.map(snapshot => downloadFileWithRetries(
        components,
        snapshot,
        targetFolder,
        presentInServers,
        _serverMapLRU,
        maxRetries,
        waitTimeBetweenRetries
      ).catch(() => logger.info(`File ${snapshot} not available for download.`))
      )
    )
  }
}

/**
 * Downloads an entity and its dependency files to a folder in the disk.
 *
 * Returns the parsed JSON file of the deployed entityHash
 * @public
 */
export async function downloadEntityAndContentFiles(
  components: Pick<SnapshotsFetcherComponents, 'fetcher' | 'logs' | 'metrics' | 'storage'>,
  entityId: EntityHash,
  presentInServers: string[],
  _serverMapLRU: Map<Server, number>,
  targetFolder: string,
  maxRetries: number,
  waitTimeBetweenRetries: number
): Promise<unknown> {
  const logger = components.logs.getLogger(`downloadEntityAndContentFiles)`)

  // download entity file
  await downloadFileWithRetries(
    components,
    entityId,
    targetFolder,
    presentInServers,
    _serverMapLRU,
    maxRetries,
    waitTimeBetweenRetries
  )

  const content = await components.storage.retrieve(entityId)

  let contentStream = ''

  if (content) {
    const stream = await content.asStream()
    const buffer = await streamToBuffer(stream)
    contentStream = await buffer.toString()
  }

  const entityMetadata: {
    type: string,
    metadata?: any,
    content?: Array<ContentMapping>
  } = JSON.parse(contentStream)

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
      _serverMapLRU,
      targetFolder,
      maxRetries,
      waitTimeBetweenRetries,
      entityMetadata)
  }

  if (entityMetadata.content) {
    await Promise.all(
      entityMetadata.content.map((content) =>
        downloadFileWithRetries(
          components,
          content.hash,
          targetFolder,
          presentInServers,
          _serverMapLRU,
          maxRetries,
          waitTimeBetweenRetries
        )
      )
    )
  }

  return entityMetadata
}

/**
 * Gets a stream of all the entities deployed to all the servers that are part of a snapshot.
 * Includes all the entities that are already present in the server.
 * Accepts a fromTimestamp option to filter out previous deployments.
 *
 * @public
 */
export async function* getDeployedEntitiesStreamFromSnapshots(
  components: SnapshotsFetcherComponents,
  options: DeployedEntityStreamOptions,
  contentServers: string[]
): AsyncIterable<SyncDeployment & {
  snapshotHash: string
  servers: string[]
}> {
  const logs = components.logs.getLogger('getDeployedEntitiesStream')
  // the minimum timestamp we are looking for
  const genesisTimestamp = options.fromTimestamp || 0

  // 1. get the hashes of the latest snapshots in the remote server, retry 10 times
  const serversSnapshotsBySnapshotHash: Map<string, {
    snapshot: {
      hash: string
      lastIncludedDeploymentTimestamp: number
      replacedSnapshotHashes?: string[] | undefined
    },
    server: string
  }[]> = new Map()
  for (const server of contentServers) {
    const snapshots = await getSnapshots(components, server, options.requestMaxRetries)
    for (const snapshot of snapshots) {
      const servers = serversSnapshotsBySnapshotHash.get(snapshot.hash) ?? []
      servers.push({ snapshot, server })
      serversSnapshotsBySnapshotHash.set(snapshot.hash, servers)
    }
  }

  for (const [snapshotHash, snapshotsWithServer] of serversSnapshotsBySnapshotHash.entries()) {
    logs.debug('Snapshot found.', { hash: snapshotHash, contentServers: JSON.stringify(snapshotsWithServer.map(s => s.server)) })
    const snapshotIsAfterGensisTimestampInSomeServer = snapshotsWithServer.some(sn => sn.snapshot.lastIncludedDeploymentTimestamp > genesisTimestamp)
    const replacedSnapshotHashes = snapshotsWithServer.map(s => s.snapshot.replacedSnapshotHashes ?? [])
    const serversWithThisSnapshot = snapshotsWithServer.map(s => s.server)
    const shouldStreamSnapshot =
      snapshotIsAfterGensisTimestampInSomeServer &&
      !await components.processedSnapshots.someGroupWasProcessed([[snapshotHash], ...replacedSnapshotHashes])

    if (shouldStreamSnapshot) {
      try {
        // 2.1. download the snapshot file if needed
        await downloadFileWithRetries(
          components,
          snapshotHash,
          options.tmpDownloadFolder,
          serversWithThisSnapshot,
          new Map(),
          options.requestMaxRetries,
          options.requestRetryWaitTime
        )

        // 2.2. open the snapshot file and process line by line
        const deploymentsInFile = processDeploymentsInFile(snapshotHash, components, logs)
        await components.processedSnapshots.startStreamOf(snapshotHash)
        let numberOfStreamedEntities = 0
        for await (const deployment of deploymentsInFile) {

          const deploymentTimestamp = 'entityTimestamp' in deployment ? deployment.entityTimestamp : deployment.localTimestamp

          if (deploymentTimestamp >= genesisTimestamp) {
            components.metrics.increment('dcl_entities_deployments_processed_total')
            numberOfStreamedEntities++
            yield {
              ...deployment,
              snapshotHash,
              servers: serversWithThisSnapshot
            }
          }
        }
        await components.processedSnapshots.endStreamOf(snapshotHash, numberOfStreamedEntities)
      } finally {
        if (options.deleteSnapshotAfterUsage !== false) {
          try {
            await components.storage.delete([snapshotHash])
          } catch (err: any) {
            logs.error(err)
          }
        }
      }
    }
  }

  logs.info('End streaming snapshots.')
}

export async function* getDeployedEntitiesStreamFromPointerChanges(
  components: SnapshotsFetcherComponents,
  options: DeployedEntityStreamOptions,
  contentServer: string
) {
  const logs = components.logs.getLogger(`pointerChangesStream(${contentServer})`)
  // fetch the /pointer-changes of the remote server using the last timestamp from the previous step with a grace period of 20 min
  const genesisTimestamp = options.fromTimestamp || 0
  let greatestLocalTimestampProcessed = genesisTimestamp
  do {
    // 1. download pointer changes and yield
    const pointerChanges = fetchPointerChanges(components, contentServer, greatestLocalTimestampProcessed, logs)
    for await (const deployment of pointerChanges) {

      // selectively ignore deployments by localTimestamp
      if (deployment.localTimestamp >= genesisTimestamp) {
        components.metrics.increment('dcl_entities_deployments_processed_total')
        yield deployment
      }

      // update greatest processed local timestamp
      if (deployment.localTimestamp) {
        greatestLocalTimestampProcessed = Math.max(greatestLocalTimestampProcessed, deployment.localTimestamp)
      }
    }

    await sleep(options.pointerChangesWaitTime)
  } while (options.pointerChangesWaitTime > 0)
}

/**
 * This function returns a JobWithLifecycle that runs forever if well configured.
 * In pseudocode it does something like this
 *
 * ```ts
 * while (jobRunning) {
 *   getDeployedEntitiesStream.map(components.deployer.deployEntity)
 * }
 * ```
 *
 * @public
 */
export function createCatalystPointerChangesDeploymentStream(
  components: SnapshotsFetcherComponents & { deployer: IDeployerComponent },
  contentServer: string,
  options: CatalystDeploymentStreamOptions
): IJobWithLifecycle & CatalystDeploymentStreamComponent {
  const logs = components.logs.getLogger(`CatalystDeploymentStream(${contentServer})`)

  let greatestProcessedTimestamp = options.fromTimestamp || 0

  const metricsLabels = contentServerMetricLabels(contentServer)

  const exponentialFallofRetryComponent = createExponentialFallofRetry(logs, {
    async action() {
      try {
        components.metrics.increment('dcl_deployments_stream_reconnection_count', metricsLabels)

        const deployments = getDeployedEntitiesStreamFromPointerChanges(
          components,
          { ...options, fromTimestamp: greatestProcessedTimestamp },
          contentServer)

        for await (const deployment of deployments) {
          // if the stream is closed then we should not process more deployments
          if (exponentialFallofRetryComponent.isStopped()) {
            logs.debug('Canceling running stream')
            return
          }

          await components.deployer.deployEntity(deployment, [contentServer])

          // update greatest processed timestamp
          if (deployment.localTimestamp > greatestProcessedTimestamp) {
            greatestProcessedTimestamp = deployment.localTimestamp
          }
        }
      } catch (e: any) {
        // we don't log the exception here because createExponentialFallofRetry(logger, options) receives the logger
        components.metrics.increment('dcl_deployments_stream_failure_count', metricsLabels)
        throw e
      }
    },
    retryTime: options.reconnectTime,
    retryTimeExponent: options.reconnectRetryTimeExponent ?? 1.1,
    maxInterval: options.maxReconnectionTime,
  })

  return {
    // exponentialFallofRetryComponent contains start and stop methods used to control this job
    ...exponentialFallofRetryComponent,
    getGreatesProcessedTimestamp() {
      return greatestProcessedTimestamp
    },
  }
}

import { ILoggerComponent } from '@well-known-components/interfaces'
import { fetchPointerChanges } from './client'
import { downloadFileWithRetries } from './downloader'
import { createExponentialFallofRetry } from './exponential-fallof-retry'
import { processDeploymentsInFile } from './file-processor'
import { IJobWithLifecycle } from './job-lifecycle-manager'
import { getDeployedEntitiesStreamFromPointerChanges, getDeployedEntitiesStreamFromSnapshot } from './stream-entities'
import {
  CatalystDeploymentStreamComponent,
  ContentMapping,
  EntityHash,
  IDeployerComponent,
  PointerChangesDeployedEntityStreamOptions,
  ReconnectionOptions,
  Server,
  SnapshotDeployedEntityStreamOptions,
  SnapshotsFetcherComponents,
} from './types'
import { contentServerMetricLabels, sleep, streamToBuffer } from './utils'

export { metricsDefinitions } from './metrics'
export { IDeployerComponent, SynchronizerComponent } from './types'
export { createSynchronizer } from './synchronizer'
export { getDeployedEntitiesStreamFromSnapshot, getDeployedEntitiesStreamFromPointerChanges } from './stream-entities'

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
 * This function streams and deploys the entities of a snapshot. When the deployer marks all the entities as deployed,
 * it saved the snapshot as processed.
 * @public
 */
export async function deployEntitiesFromSnapshot(
  components: Pick<SnapshotsFetcherComponents, 'metrics' | 'logs' | 'storage' | 'processedSnapshotStorage' | 'snapshotStorage'> & {
    deployer: IDeployerComponent
  },
  options: SnapshotDeployedEntityStreamOptions,
  snapshotHash: string,
  servers: Set<string>,
  shouldStopStream: () => boolean) {
  const logger = components.logs.getLogger('asdf')
  const stream = getDeployedEntitiesStreamFromSnapshot(components, options, snapshotHash, servers)
  let snapshotWasCompletelyStreamed = false
  let numberOfStreamedEntities = 0
  let numberOfProcessedEntities = 0
  async function saveIfStreamEndedAndAllEntitiesWereProcessed() {
    if (snapshotWasCompletelyStreamed && numberOfStreamedEntities == numberOfProcessedEntities) {
      await components.processedSnapshotStorage.saveAsProcessed(snapshotHash)
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'saved' })
    }
  }
  for await (const entity of stream) {
    if (shouldStopStream()) {
      logger.debug('Canceling running sync snapshots stream')
      return
    }
    // schedule the deployment in the deployer. the await DOES NOT mean that the entity was deployed entirely
    // if the deployer is not synchronous. For example, the batchDeployer used in the catalyst just add it in a queue.
    // Once the entity is truly deployed, it should call the method 'markAsDeployed'
    await components.deployer.deployEntity({
      ...entity,
      markAsDeployed: async function () {
        components.metrics.increment('dcl_entities_deployments_processed_total', { source: 'snapshots' })
        numberOfProcessedEntities++
        await saveIfStreamEndedAndAllEntitiesWereProcessed()
      },
      snapshotHash
    }, entity.servers)
    numberOfStreamedEntities++
  }
  snapshotWasCompletelyStreamed = true
  components.metrics.increment('dcl_processed_snapshots_total', { state: 'stream_end' })
  logger.info('Stream ended.', { snapshotHash })
  await saveIfStreamEndedAndAllEntitiesWereProcessed()
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
 * @deprecated
 */
export function createCatalystPointerChangesDeploymentStream(
  components: SnapshotsFetcherComponents & { deployer: IDeployerComponent },
  contentServer: string,
  options: ReconnectionOptions & PointerChangesDeployedEntityStreamOptions
): IJobWithLifecycle & CatalystDeploymentStreamComponent {
  const logs = components.logs.getLogger(`pointerChangesDeploymentStream(${contentServer})`)

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

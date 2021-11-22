import { fetchPointerChanges, getGlobalSnapshot } from './client'
import { downloadFileWithRetries } from './downloader'
import { processDeploymentsInFile } from './processor'
import { RemoteEntityDeployment, SnapshotsFetcherComponents } from './types'
import { sleep } from './utils'

export { downloadEntityAndContentFiles } from './entities'

/**
 * Gets a stream of all the entities deployed to a server.
 * Includes all the entities that are already present in the server.
 * Accepts a fromTimestamp option to filter out previous deployments.
 *
 * @public
 */
export async function* getDeployedEntitiesStream(
  components: SnapshotsFetcherComponents,
  options: {
    contentServer: string
    fromTimestamp?: number
    contentFolder: string
    waitTime: number
  }
): AsyncIterable<RemoteEntityDeployment> {
  // the minimum timestamp we are looking for
  const genesisTimestamp = options.fromTimestamp || 0

  // the greatest timestamp we processed
  let greatestProcessedTimestamp = 0

  // 1. get the hash of the latest snapshot in the remote server, retry 10 times
  const { hash, lastIncludedDeploymentTimestamp } = await getGlobalSnapshot(
    components,
    options.contentServer,
    10 /* retries */
  )

  // 2. download the snapshot file if it contains deployments
  //    in the range we are interested (>= genesisTimestamp)
  if (lastIncludedDeploymentTimestamp > genesisTimestamp) {
    // 2.1. download the shapshot file if needed
    const snapshotFilename = await downloadFileWithRetries(
      hash,
      options.contentFolder,
      [options.contentServer],
      new Map()
    )

    // 2.2. open the snapshot file and process line by line
    const deploymentsInFile = processDeploymentsInFile(snapshotFilename)
    for await (const deployment of deploymentsInFile) {
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
    for await (const deployment of pointerChanges) {
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
    await sleep(options.waitTime)
  } while (options.waitTime > 0)
}

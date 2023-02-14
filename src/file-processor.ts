import { SnapshotsFetcherComponents } from './types'
import { createInterface } from 'readline'
import { PointerChangesSyncDeployment, SnapshotSyncDeployment, SyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'

async function* processLineByLine(input: NodeJS.ReadableStream) {
  yield* createInterface({
    input,
    crlfDelay: Infinity
  })
}

/**
 * Reads line by line from a file in the disk.
 * Parses every line and yields RemoteEntityDeployment.
 * @public
 */
export async function* processDeploymentsInFile(
  file: string,
  components: Pick<SnapshotsFetcherComponents, 'storage'>,
  logger: ILoggerComponent.ILogger
): AsyncIterable<SyncDeployment> {
  const fileContent = await components.storage.retrieve(file)

  if (!fileContent) {
    throw new Error(`The file ${file} does not exist`)
  }

  const stream = await fileContent!.asStream()

  try {
    yield* processDeploymentsInStream(stream, logger)
  } finally {
    stream.destroy()
  }
}

/**
 * Reads line by line from a stream.
 * Parses every line and yields RemoteEntityDeployment.
 * @public
 */
export async function* processDeploymentsInStream(
  stream: NodeJS.ReadableStream,
  logger: ILoggerComponent.ILogger
): AsyncIterable<SyncDeployment> {
  for await (const line of processLineByLine(stream)) {
    const theLine = line.trim()
    if (theLine.startsWith('{') && theLine.endsWith('}')) {
      const parsedLine = JSON.parse(theLine)
      if (SnapshotSyncDeployment.validate(parsedLine)) {
        yield parsedLine
      } else if (PointerChangesSyncDeployment.validate(parsedLine)) {
        yield parsedLine
      } else {
        const errors = {
          ...SnapshotSyncDeployment.validate.errors,
          ...PointerChangesSyncDeployment.validate.errors
        }
        logger.error('ERROR: Invalid entity deployment in snapshot file', {
          deployment: parsedLine,
          error: JSON.stringify(errors)
        })
      }
    }
  }
}

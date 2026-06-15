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
// Maximum number of invalid-line errors logged per snapshot file, so a single corrupt file can't
// flood the logs.
const MAX_LINE_ERRORS_TO_LOG = 100

export async function* processDeploymentsInStream(
  stream: NodeJS.ReadableStream,
  logger: ILoggerComponent.ILogger
): AsyncIterable<SyncDeployment> {
  let lineErrorsLogged = 0
  function logLineError(message: string, extra: Record<string, string>) {
    if (lineErrorsLogged >= MAX_LINE_ERRORS_TO_LOG) {
      return
    }
    lineErrorsLogged++
    logger.error(message, extra)
    if (lineErrorsLogged === MAX_LINE_ERRORS_TO_LOG) {
      logger.error('Too many invalid lines in snapshot file, suppressing further line errors', {
        suppressedAfter: String(MAX_LINE_ERRORS_TO_LOG)
      })
    }
  }

  for await (const line of processLineByLine(stream)) {
    const theLine = line.trim()
    if (theLine.startsWith('{') && theLine.endsWith('}')) {
      let parsedLine: any
      try {
        parsedLine = JSON.parse(theLine)
      } catch (error: any) {
        // A single malformed line should not abort processing of the whole snapshot file.
        logLineError('ERROR: Could not parse line in snapshot file', {
          line: theLine,
          error: error?.message ?? JSON.stringify(error)
        })
        continue
      }
      if (SnapshotSyncDeployment.validate(parsedLine)) {
        yield parsedLine
      } else if (PointerChangesSyncDeployment.validate(parsedLine)) {
        yield parsedLine
      } else {
        logLineError('ERROR: Invalid entity deployment in snapshot file', {
          deployment: JSON.stringify(parsedLine),
          snapshotErrors: JSON.stringify(SnapshotSyncDeployment.validate.errors),
          pointerChangesErrors: JSON.stringify(PointerChangesSyncDeployment.validate.errors)
        })
      }
    }
  }
}

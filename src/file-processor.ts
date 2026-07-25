import { SnapshotsFetcherComponents } from './types'
import { createInterface } from 'readline'
import { SnapshotSyncDeployment, SyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'

const CURLY_OPEN = 0x7b // {
const CURLY_CLOSE = 0x7d // }

/**
 * True when the string already looks like a JSON object literal. charCodeAt instead of
 * startsWith/endsWith because this runs once or twice per line of a snapshot that can hold millions
 * of them.
 */
function isBraceDelimited(line: string): boolean {
  return line.length > 1 && line.charCodeAt(0) === CURLY_OPEN && line.charCodeAt(line.length - 1) === CURLY_CLOSE
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

  // Iterate the readline interface directly. Wrapping it in an extra async generator added a promise
  // and a microtask per line, which on a multi-million-entity snapshot is pure overhead.
  const lines = createInterface({ input: stream, crlfDelay: Infinity })

  for await (const line of lines) {
    // trim() allocates a new string for every line. Snapshot lines are written without padding, so
    // only pay for it when the raw line is not already a JSON object literal.
    const theLine = isBraceDelimited(line) ? line : line.trim()
    if (isBraceDelimited(theLine)) {
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
      // One check accepts both shapes. PointerChangesSyncDeployment requires everything
      // SnapshotSyncDeployment requires plus localTimestamp, and neither forbids extra properties, so
      // every valid pointer-changes deployment is also a valid snapshot deployment. A follow-up
      // `else if (PointerChangesSyncDeployment.validate(...))` could therefore never be reached.
      if (SnapshotSyncDeployment.validate(parsedLine)) {
        yield parsedLine
      } else {
        logLineError('ERROR: Invalid entity deployment in snapshot file', {
          deployment: JSON.stringify(parsedLine),
          errors: JSON.stringify(SnapshotSyncDeployment.validate.errors)
        })
      }
    }
  }
}

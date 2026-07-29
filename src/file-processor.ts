import { SnapshotsFetcherComponents } from './types'
import { createInterface } from 'readline'
import { Transform } from 'stream'
import { SnapshotSyncDeployment, SyncDeployment } from '@dcl/schemas'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { isUsableTimestamp, truncateForLog } from './utils'

const CURLY_OPEN = 0x7b // {
const CURLY_CLOSE = 0x7d // }
const LINE_FEED = 0x0a // \n

/**
 * Tally of the lines a snapshot file could not contribute as deployments.
 *
 * A snapshot with a non-zero count is incomplete: those entities were never streamed, so recording
 * the snapshot as processed would retire it permanently with entities missing.
 * @public
 */
export type SnapshotStreamReport = {
  unusableLines: number
}

/**
 * A snapshot line is a single deployment; the largest legitimate ones are a few KB. readline imposes
 * no line bound, so a file that contains no newline at all is accumulated into one string until the
 * heap is exhausted (V8 hard-fails past ~512 MB anyway). Fail the snapshot instead of the process.
 */
const MAX_SNAPSHOT_LINE_LENGTH_IN_BYTES = 10 * 1024 * 1024 // 10 MiB

/**
 * Fails the pipeline once more than maxBytes have flowed through it without a newline.
 */
function createLineLengthLimiter(maxBytes: number): Transform {
  let bytesSinceNewline = 0
  return new Transform({
    transform(chunk, _encoding, callback) {
      const buffer: Buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk))
      const lastNewline = buffer.lastIndexOf(LINE_FEED)
      bytesSinceNewline = lastNewline === -1 ? bytesSinceNewline + buffer.length : buffer.length - lastNewline - 1
      if (bytesSinceNewline > maxBytes) {
        callback(new Error(`Snapshot line exceeds the maximum allowed length of ${maxBytes} bytes`))
        return
      }
      // Forward the original chunk so the downstream encoding is untouched.
      callback(null, chunk)
    }
  })
}

/**
 * True when the string already looks like a JSON object literal. charCodeAt instead of
 * startsWith/endsWith because this runs once or twice per line of a snapshot that can hold millions
 * of them.
 */
function isBraceDelimited(line: string): boolean {
  return line.length > 1 && line.charCodeAt(0) === CURLY_OPEN && line.charCodeAt(line.length - 1) === CURLY_CLOSE
}

/**
 * Lines that carry no deployment but are part of the format: the `### Decentraland json snapshot`
 * header, and blank padding. Anything else that is not a deployment is a line we failed to read, and
 * must be counted as such rather than skipped in silence.
 */
function isSnapshotFraming(line: string): boolean {
  return line.length === 0 || line.startsWith('###')
}

/**
 * Reads line by line from a file in the disk.
 * Parses every line and yields RemoteEntityDeployment.
 * @public
 */
export async function* processDeploymentsInFile(
  file: string,
  components: Pick<SnapshotsFetcherComponents, 'storage'>,
  logger: ILoggerComponent.ILogger,
  report?: SnapshotStreamReport
): AsyncIterable<SyncDeployment> {
  const fileContent = await components.storage.retrieve(file)

  if (!fileContent) {
    throw new Error(`The file ${file} does not exist`)
  }

  const stream = await fileContent!.asStream()

  try {
    yield* processDeploymentsInStream(stream, logger, report)
  } finally {
    stream.destroy()
  }
}

// Maximum number of invalid-line errors logged per snapshot file, so a single corrupt file can't
// flood the logs.
const MAX_LINE_ERRORS_TO_LOG = 100

/**
 * Reads line by line from a stream.
 * Parses every line and yields RemoteEntityDeployment.
 *
 * @param report - Optional tally, incremented for every line that could not be read as a deployment.
 *   Callers that record snapshots as processed must pass one and check it.
 * @public
 */
export async function* processDeploymentsInStream(
  stream: NodeJS.ReadableStream,
  logger: ILoggerComponent.ILogger,
  report?: SnapshotStreamReport
): AsyncIterable<SyncDeployment> {
  let lineErrorsLogged = 0
  // The payload is built by a callback rather than passed in: the JSON.stringify calls that make up
  // these entries are the expensive part, and evaluating them as arguments meant a snapshot on an
  // incompatible schema paid for millions of them to emit MAX_LINE_ERRORS_TO_LOG lines.
  function reportUnusableLine(message: string, buildExtra: () => Record<string, string>) {
    if (report) {
      report.unusableLines++
    }
    if (lineErrorsLogged >= MAX_LINE_ERRORS_TO_LOG) {
      return
    }
    lineErrorsLogged++
    logger.error(message, buildExtra())
    if (lineErrorsLogged === MAX_LINE_ERRORS_TO_LOG) {
      logger.error('Too many invalid lines in snapshot file, suppressing further line errors', {
        suppressedAfter: String(MAX_LINE_ERRORS_TO_LOG)
      })
    }
  }

  const lineLengthLimiter = createLineLengthLimiter(MAX_SNAPSHOT_LINE_LENGTH_IN_BYTES)
  // pipe() does NOT forward the source's errors to the destination, so this bridge is what keeps a
  // failing storage read from becoming an unhandled 'error' event — i.e. a process crash — instead of a
  // rejected snapshot. readline used to sit directly on the source and got this for free; inserting the
  // limiter is what took it away, so it has to be put back explicitly.
  const forwardSourceError = (error: Error) => lineLengthLimiter.destroy(error)
  stream.on('error', forwardSourceError)
  // Iterate the readline interface directly. Wrapping it in an extra async generator added a promise
  // and a microtask per line, which on a multi-million-entity snapshot is pure overhead.
  const lines = createInterface({ input: stream.pipe(lineLengthLimiter), crlfDelay: Infinity })

  try {
    for await (const line of lines) {
      // trim() allocates a new string for every line. Snapshot lines are written without padding, so
      // only pay for it when the raw line is not already a JSON object literal.
      const theLine = isBraceDelimited(line) ? line : line.trim()
      if (!isBraceDelimited(theLine)) {
        // A truncated final line, or a snapshot framed differently than we expect, used to be dropped
        // here without a trace — indistinguishable from a genuinely empty snapshot.
        if (!isSnapshotFraming(theLine)) {
          reportUnusableLine('ERROR: Unrecognized line in snapshot file', () => ({ line: truncateForLog(theLine) }))
        }
        continue
      }

      let parsedLine: any
      try {
        parsedLine = JSON.parse(theLine)
      } catch (error: any) {
        // A single malformed line should not abort processing of the whole snapshot file.
        reportUnusableLine('ERROR: Could not parse line in snapshot file', () => ({
          line: truncateForLog(theLine),
          error: error?.message ?? JSON.stringify(error)
        }))
        continue
      }
      // Read while parsedLine is still untyped: the schema guard below narrows it to the snapshot shape,
      // which does not declare localTimestamp — only pointer-changes-shaped lines carry one.
      const lineEntityTimestamp: unknown = parsedLine.entityTimestamp
      const lineLocalTimestamp: unknown = parsedLine.localTimestamp
      // One check accepts both shapes. PointerChangesSyncDeployment requires everything
      // SnapshotSyncDeployment requires plus localTimestamp, and neither forbids extra properties, so
      // every valid pointer-changes deployment is also a valid snapshot deployment. A follow-up
      // `else if (PointerChangesSyncDeployment.validate(...))` could therefore never be reached.
      if (!SnapshotSyncDeployment.validate(parsedLine)) {
        reportUnusableLine('ERROR: Invalid entity deployment in snapshot file', () => ({
          deployment: truncateForLog(JSON.stringify(parsedLine)),
          errors: JSON.stringify(SnapshotSyncDeployment.validate.errors)
        }))
        continue
      }
      // The schema bounds these no more than it does on /pointer-changes: it rejects Infinity and
      // negatives, but 1e308, an above-2^53 value, a fraction or a far-future instant are all schema-valid.
      // Unlike the other paths this one does not feed our own high-water mark, but the entity is handed
      // to the consumer's deployer with that timestamp on it, and a deployer treating it as the entity's
      // version would let one poisoned line shadow every later legitimate update to those pointers.
      // Counted as unusable, so the snapshot stays unmarked and is retried rather than half-applied.
      if (
        !isUsableTimestamp(lineEntityTimestamp) ||
        (lineLocalTimestamp !== undefined && !isUsableTimestamp(lineLocalTimestamp))
      ) {
        reportUnusableLine('ERROR: Implausible timestamp in entity deployment in snapshot file', () => ({
          deployment: truncateForLog(JSON.stringify(parsedLine)),
          entityTimestamp: String(lineEntityTimestamp),
          localTimestamp: String(lineLocalTimestamp)
        }))
        continue
      }
      yield parsedLine
    }
  } finally {
    // Detached explicitly: the stream belongs to the caller — processDeploymentsInFile owns one, but this
    // is @public and a consumer may hand in a long-lived stream of its own — so leaving a listener on it
    // would outlive this call.
    stream.off('error', forwardSourceError)
    lines.close()
    lineLengthLimiter.destroy()
  }
}

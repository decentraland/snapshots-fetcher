import { SnapshotsFetcherComponents } from './types'
import { createInterface } from 'readline'
import { PointerChangesSyncDeployment, SnapshotSyncDeployment, SyncDeployment } from '@dcl/schemas'

async function* processLineByLine(input: NodeJS.ReadableStream) {
  yield* createInterface({
    input,
    crlfDelay: Infinity,
  })
}

/**
 * Reads line by line from a file in the disk.
 * Parses every line and yields RemoteEntityDeployment.
 * @public
 */
export async function* processDeploymentsInFile(
  file: string,
  components: Pick<SnapshotsFetcherComponents, 'storage'>
): AsyncIterable<SyncDeployment> {
  const fileContent = await components.storage.retrieve(file)

  if (!fileContent) {
    throw new Error(`The file ${file} does not exist`)
  }

  const stream = await fileContent!.asStream()

  try {
    yield* processDeploymentsInStream(stream)
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
  stream: NodeJS.ReadableStream
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
        console.error('ERROR: Invalid entity deployment in snapshot file', parsedLine, errors)
      }
    }
  }
}

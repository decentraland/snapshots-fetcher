import { SnapshotsFetcherComponents } from './types'
import { createInterface } from 'readline'
import { coerceEntityDeployment } from './utils'
import { DeploymentWithAuthChain } from '@dcl/schemas'

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
): AsyncIterable<DeploymentWithAuthChain> {
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
): AsyncIterable<DeploymentWithAuthChain> {
  for await (const line of processLineByLine(stream)) {
    const theLine = line.trim()
    if (theLine.startsWith('{') && theLine.endsWith('}')) {
      const deployment = coerceEntityDeployment(JSON.parse(theLine))
      if (deployment) {
        yield deployment
      }
    }
  }
}

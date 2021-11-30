import { RemoteEntityDeployment } from './types'
import { createInterface } from 'readline'
import { createReadStream } from 'fs'
import { checkFileExists, coerceEntityDeployment } from './utils'

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
export async function* processDeploymentsInFile(file: string): AsyncIterable<RemoteEntityDeployment> {
  if (!(await checkFileExists(file))) {
    throw new Error(`The file ${file} does not exist`)
  }

  const stream = createReadStream(file)

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
): AsyncIterable<RemoteEntityDeployment> {
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

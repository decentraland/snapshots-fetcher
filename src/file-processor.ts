import { RemoteEntityDeployment } from './types'
import { createInterface } from 'readline'
import { createReadStream } from 'fs'
import { checkFileExists, coerceEntityDeployment } from './utils'

async function* processLineByLine(file: string) {
  if (!(await checkFileExists(file))) {
    throw new Error(`The file ${file} does not exist`)
  }

  const fileStream = createReadStream(file)

  yield* createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })
}

/**
 * Reads line by line from a file in the disk.
 * Parses every line and yields RemoteEntityDeployment.
 * @public
 */
export async function* processDeploymentsInFile(file: string): AsyncIterable<RemoteEntityDeployment> {
  for await (const line of processLineByLine(file)) {
    const theLine = line.trim()
    if (theLine.startsWith('{') && theLine.endsWith('}')) {
      const deployment = coerceEntityDeployment(JSON.parse(theLine))
      if (deployment) {
        yield deployment
      }
    }
  }
}

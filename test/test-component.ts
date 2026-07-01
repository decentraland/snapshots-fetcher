import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { readFileSync } from 'fs'
import { readdir, stat } from 'fs/promises'
import { resolve } from 'path'
import { Readable } from 'stream'
import { IProcessedSnapshotStorageComponent } from '../src/types'

export async function createStorageComponent(): Promise<IContentStorageComponent> {
  const rootFixturesDir = 'test/fixtures'

  const files = await readdir(rootFixturesDir)

  const mockFileSystem = createInMemoryStorage()

  async function reset() {
    return Promise.all(
      files.map(async (file) => {
        const fileName = resolve(rootFixturesDir, file)
        const stats = await stat(fileName)
        if (stats.isFile()) {
          await mockFileSystem.storeStream(file, Readable.from(readFileSync(fileName)))
        }
      })
    )
  }

  await reset()

  return mockFileSystem
}

export function createProcessedSnapshotStorageComponent(): IProcessedSnapshotStorageComponent {
  const processedSnapshots = new Set()

  return {
    async filterProcessedSnapshotsFrom(snapshotHashes: string[]) {
      return new Set(snapshotHashes.filter((h) => processedSnapshots.has(h)))
    },
    async markSnapshotAsProcessed(snapshotHash: string) {
      processedSnapshots.add(snapshotHash)
    }
  }
}

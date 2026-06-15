import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { IFetchComponent } from '@well-known-components/interfaces'
import { readFileSync } from 'fs'
import { readdir, stat } from 'fs/promises'
import { resolve } from 'path'
import { Readable } from 'stream'
import { IProcessedSnapshotStorageComponent } from '../src/types'

export function createFetchComponent(): IFetchComponent {
  return {
    // Bridge Node's native fetch to IFetchComponent (which is typed against node-fetch). The
    // node-fetch-specific init options (timeout/size) are ignored by native fetch, which is fine
    // for tests; the production fetcher honors them.
    async fetch(url: any, init?: any): Promise<any> {
      return globalThis.fetch(url, init)
    }
  }
}

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

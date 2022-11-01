import { IFetchComponent } from '@well-known-components/http-server'
import * as nodeFetch from 'node-fetch'
import { readFileSync } from 'fs'
import { readdir, stat } from 'fs/promises'
import { resolve } from 'path'
import { MockedStorage } from '@dcl/catalyst-storage/dist/MockedStorage'
import { IContentStorageComponent } from '@dcl/catalyst-storage'
import { IProcessedSnapshotsComponent, IProcessedSnapshotStorageComponent } from '../src/types'

export function createFetchComponent() {
  const fetch: IFetchComponent = {
    async fetch(url: nodeFetch.RequestInfo, init?: nodeFetch.RequestInit): Promise<nodeFetch.Response> {
      return nodeFetch.default(url, init)
    },
  }

  return fetch
}

export async function createStorageComponent(): Promise<IContentStorageComponent> {

  const rootFixturesDir = 'test/fixtures'

  const files = await readdir(rootFixturesDir)

  const mockFileSystem = new MockedStorage()

  async function reset() {
    return Promise.all(
      files.map(async (file) => {
        const fileName = resolve(rootFixturesDir, file)
        const stats = await stat(fileName)
        if (stats.isFile()) {
          mockFileSystem.storage.set(file, readFileSync(fileName))
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
    async processedFrom(snapshotHashes: string[]) {
      return new Set(snapshotHashes.filter(h => processedSnapshots.has(h)))
    },
    async saveProcessed(snapshotHash: string) {
      processedSnapshots.add(snapshotHash)
    }
  }
}

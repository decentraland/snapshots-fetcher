import { IFetchComponent } from '@well-known-components/http-server'
import * as nodeFetch from 'node-fetch'
import { ContentItem, ContentStorage } from '../src/types'
import { Readable } from 'stream'
import { readFileSync } from 'fs'
import { readdir, stat } from 'fs/promises'
import { resolve } from 'path'

export function createFetchComponent() {
  const fetch: IFetchComponent = {
    async fetch(url: nodeFetch.RequestInfo, init?: nodeFetch.RequestInit): Promise<nodeFetch.Response> {
      return nodeFetch.default(url, init)
    },
  }

  return fetch
}

export async function createStorageComponent(): Promise<ContentStorage> {
  const fs = new Map()

  const exist = async (ids: string[]) => {
    return new Map(ids.map((id) => [id, fs.get(id)!!]))
  }
  const storeExistingContentItem = async (currentFilePath: string, id: string) => {
    const content = readFileSync(currentFilePath, { encoding: 'utf-8' })
    fs.set(id, content)
  }

  const retrieve = async (id: string): Promise<ContentItem> => ({
    contentEncoding: async () => undefined,
    getLength: () => id.length,
    asStream: async (): Promise<Readable> => {
      const s = new Readable()
      s.push(fs.get(id))
      s.push(null)

      return s
    },
  })

  const rootFixturesDir = 'test/fixtures'

  const files = await readdir(rootFixturesDir)

  await Promise.all(
    files.map(async (file) => {
      const fileName = resolve(rootFixturesDir, file)
      const stats = await stat(fileName)
      if (stats.isFile()) {
        fs.set(file, readFileSync(fileName))
      }
    })
  )

  return {
    exist,
    storeExistingContentItem,
    retrieve,
  }
}

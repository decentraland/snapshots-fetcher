import { IFetchComponent } from '@well-known-components/http-server'
import * as nodeFetch from 'node-fetch'
import { ContentItem, ContentStorage } from '../src/types'
import { Readable } from 'stream'
import { readFileSync } from 'fs'
import { readdir, stat } from 'fs/promises'
import { resolve } from 'path'
import { streamToBuffer } from '../src/utils'

export function createFetchComponent() {
  const fetch: IFetchComponent = {
    async fetch(url: nodeFetch.RequestInfo, init?: nodeFetch.RequestInit): Promise<nodeFetch.Response> {
      return nodeFetch.default(url, init)
    },
  }

  return fetch
}

export async function createStorageComponent(): Promise<ContentStorage> {
  const fs = new Map<string, Buffer>()

  const exist = async (id: string) => {
    return !!fs.get(id)
  }
  const storeStream = async (id: string, fileStream: Readable) => {
    const content = await streamToBuffer(fileStream)
    fs.set(id, content)
  }

  const retrieve = async (id: string): Promise<ContentItem> => {
    if (!fs.get(id)) {
      return undefined
    }

    return {
      asStream: async (): Promise<Readable> => {
        return Readable.from(fs.get(id))
      },
    }
  }

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
    storeStream,
    retrieve,
  }
}

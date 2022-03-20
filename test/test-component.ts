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

  async function exist(id: string) {
    return !!fs.get(id)
  }

  async function storeStream(id: string, fileStream: Readable) {
    console.log(`> Storing file ${id}`)
    const content = await streamToBuffer(fileStream)
    fs.set(id, content)
  }

  async function retrieve(id: string): Promise<ContentItem> {
    const buffer = fs.get(id)

    if (!buffer) {
      return undefined
    }

    return {
      async asStream(): Promise<Readable> {
        return Readable.from(buffer)
      },
    }
  }

  const rootFixturesDir = 'test/fixtures'

  const files = await readdir(rootFixturesDir)

  async function reset() {
    return Promise.all(
      files.map(async (file) => {
        const fileName = resolve(rootFixturesDir, file)
        const stats = await stat(fileName)
        if (stats.isFile()) {
          fs.set(file, readFileSync(fileName))
        }
      })
    )
  }

  await reset()

  const ret: ContentStorage = {
    exist,
    storeStream,
    retrieve,
    async delete(ids: string[]) {
      // noop
    },
  }

  return Object.assign(ret, {
    fs,
  })
}

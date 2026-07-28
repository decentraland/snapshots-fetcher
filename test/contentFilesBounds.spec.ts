import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import { Readable } from 'stream'
import { downloadEntityAndContentFiles } from '../src'
import { test } from './components'

// A real, hash-addressed fixture, so the hash check on download passes.
const realContentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'

test('downloadEntityAndContentFiles when the entity content array is hostile or redundant', ({ components }) => {
  const contentFolder = resolve('downloads')
  let contentRequests: string[]

  async function storageWithEntity(entityId: string, content: unknown): Promise<IContentStorageComponent> {
    const storage = createInMemoryStorage()
    await storage.storeStream(entityId, Readable.from([Buffer.from(JSON.stringify({ type: 'scene', content }))]))
    return storage
  }

  async function download(entityId: string, storage: IContentStorageComponent) {
    return downloadEntityAndContentFiles(
      { fetcher: components.fetcher, logs: components.logs, metrics: components.metrics, storage },
      entityId,
      [await components.getBaseUrl()],
      new Map(),
      contentFolder,
      1, // maxRetries
      0, // waitTimeBetweenRetries
      10 // contentFilesConcurrency
    )
  }

  it('prepares the endpoints', () => {
    contentRequests = []
    components.router.get('/contents/:file', async (ctx) => {
      contentRequests.push(ctx.params.file)
      return { body: createReadStream('test/fixtures/' + ctx.params.file) }
    })
  })

  describe('when the same content hash is repeated many times', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      contentRequests = []
      storage = await storageWithEntity(
        'entityrepeatedcontenthashes',
        Array.from({ length: 50 }, (_unused, index) => ({ file: `file-${index}`, hash: realContentHash }))
      )
      await download('entityrepeatedcontenthashes', storage)
    })

    it('should download it once instead of queueing a job per entry', () => {
      expect(contentRequests).toEqual([realContentHash])
    })

    it('should still store the file', async () => {
      expect(await storage.exist(realContentHash)).toBe(true)
    })
  })

  describe('when an entry declares a hash that is not a content address', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      contentRequests = []
      storage = await storageWithEntity('entityinvalidcontenthash', [
        { file: 'good', hash: realContentHash },
        { file: 'evil', hash: '../../etc/passwd' }
      ])
    })

    it('should reject naming the entity rather than partially fetching it', async () => {
      await expect(download('entityinvalidcontenthash', storage)).rejects.toThrow(
        'Entity entityinvalidcontenthash declares an invalid content file hash'
      )
    })

    it('should not download anything, since the manifest cannot be satisfied in full', async () => {
      await download('entityinvalidcontenthash', storage).catch(() => undefined)

      expect(contentRequests).toEqual([])
    })
  })

  describe('when the content array is larger than one entity is allowed to declare', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      contentRequests = []
      storage = await storageWithEntity(
        'entitytoomanycontentfiles',
        Array.from({ length: 25_001 }, (_unused, index) => ({ file: `file-${index}`, hash: realContentHash }))
      )
    })

    it('should reject rather than silently truncating the list', async () => {
      await expect(download('entitytoomanycontentfiles', storage)).rejects.toThrow(
        'above the maximum of 25000'
      )
    })
  })
})

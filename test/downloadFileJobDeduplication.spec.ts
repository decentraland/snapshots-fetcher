import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { createReadStream, mkdirSync, rmSync } from 'fs'
import { resolve } from 'path'
import { downloadFileWithRetries } from '../src/downloader'
import { sleep } from '../src/utils'
import { test } from './components'

// A real hash-addressed fixture, so the post-download hash check passes.
const contentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'

test('downloadFileWithRetries when the same hash is requested concurrently', ({ components }) => {
  let requestCount: number

  it('prepares the endpoints', () => {
    requestCount = 0
    components.router.get('/contents/:file', async (ctx) => {
      requestCount++
      // Hold the response open so both callers are in flight at the same time.
      await sleep(40)
      return { body: createReadStream('test/fixtures/' + ctx.params.file) }
    })
  })

  describe('and both callers target different temp folders', () => {
    let storage: IContentStorageComponent
    let baseUrl: string

    beforeEach(async () => {
      requestCount = 0
      storage = createInMemoryStorage()
      baseUrl = await components.getBaseUrl()
      // Both folders must exist: whichever caller registers the shared job first is the one that
      // writes, and the assertion should be about the request count, not about a missing directory.
      mkdirSync(resolve('downloads/nested'), { recursive: true })
    })

    it('should transfer the file once, because storage is keyed by hash and not by path', async () => {
      await Promise.all([
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0),
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads/nested'), [baseUrl], new Map(), 3, 0)
      ])

      expect(requestCount).toBe(1)
    })

    it('should leave the file available in storage for both callers', async () => {
      await Promise.all([
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0),
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads/nested'), [baseUrl], new Map(), 3, 0)
      ])

      expect(await storage.exist(contentHash)).toBe(true)
    })
  })

  describe('and the target temp folder does not exist yet', () => {
    let storage: IContentStorageComponent
    let baseUrl: string
    let missingFolder: string

    beforeEach(async () => {
      requestCount = 0
      storage = createInMemoryStorage()
      baseUrl = await components.getBaseUrl()
      missingFolder = resolve('downloads/does-not-exist-yet/deeper')
      rmSync(resolve('downloads/does-not-exist-yet'), { recursive: true, force: true })
    })

    afterEach(() => {
      rmSync(resolve('downloads/does-not-exist-yet'), { recursive: true, force: true })
    })

    it('should create the folder instead of failing every download with ENOENT', async () => {
      await downloadFileWithRetries({ storage }, contentHash, missingFolder, [baseUrl], new Map(), 1, 0)

      expect(await storage.exist(contentHash)).toBe(true)
    })
  })
})

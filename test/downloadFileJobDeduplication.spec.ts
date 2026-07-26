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

  describe('and each caller holds a different storage component', () => {
    let firstStorage: IContentStorageComponent
    let secondStorage: IContentStorageComponent
    let baseUrl: string

    beforeEach(async () => {
      requestCount = 0
      firstStorage = createInMemoryStorage()
      secondStorage = createInMemoryStorage()
      baseUrl = await components.getBaseUrl()
    })

    it('should not share one job between them, since a job only stores into its own storage', async () => {
      await Promise.all([
        downloadFileWithRetries({ storage: firstStorage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0),
        downloadFileWithRetries({ storage: secondStorage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0)
      ])

      // Sharing would leave the joiner believing the download succeeded while its storage stayed empty.
      expect([await firstStorage.exist(contentHash), await secondStorage.exist(contentHash)]).toEqual([true, true])
    })

    it('should transfer the file once per storage', async () => {
      await Promise.all([
        downloadFileWithRetries({ storage: firstStorage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0),
        downloadFileWithRetries({ storage: secondStorage }, contentHash, resolve('downloads'), [baseUrl], new Map(), 3, 0)
      ])

      expect(requestCount).toBe(2)
    })
  })

  describe('and the job they share fails against its own candidate servers', () => {
    let storage: IContentStorageComponent
    let workingServer: string
    let brokenServer: string

    beforeEach(async () => {
      requestCount = 0
      storage = createInMemoryStorage()
      workingServer = await components.getBaseUrl()
      // Nothing listens here, so every attempt against it fails.
      brokenServer = 'http://127.0.0.1:1'
    })

    it('should let the joining caller retry with its own servers rather than inherit the failure', async () => {
      const [failing, joining] = await Promise.allSettled([
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [brokenServer], new Map(), 1, 0),
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [workingServer], new Map(), 2, 0)
      ])

      expect([failing.status, joining.status]).toEqual(['rejected', 'fulfilled'])
    })
  })

  describe('and a caller that fell back settles after a replacement job was registered', () => {
    let storage: IContentStorageComponent
    let workingServer: string
    let brokenServer: string

    beforeEach(async () => {
      requestCount = 0
      storage = createInMemoryStorage()
      workingServer = await components.getBaseUrl()
      brokenServer = 'http://127.0.0.1:1'
    })

    it('should not evict the replacement from the in-flight map', async () => {
      // The failing caller registers first; the joiner falls through and registers its own job. When
      // the first one finally settles it must clear only its own slot, or a later caller would miss
      // the replacement and start a duplicate transfer of the same file.
      await Promise.allSettled([
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [brokenServer], new Map(), 1, 0),
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [workingServer], new Map(), 2, 0)
      ])

      const requestsAfterFirstRound = requestCount

      // Already stored by the successful job, so this must short-circuit rather than transfer again.
      await downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [workingServer], new Map(), 2, 0)

      expect(requestCount).toBe(requestsAfterFirstRound)
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

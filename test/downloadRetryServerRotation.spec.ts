import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import { downloadFileWithRetries } from '../src/downloader'
import { test } from './components'

// A real hash-addressed fixture, so the post-download hash check passes.
const contentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'

test('downloadFileWithRetries when the first server it picks fails', ({ components }) => {
  const targetFolder = resolve('downloads')
  let requestsPerHost: Map<string, number>
  let failingHost: string

  it('prepares the endpoints', () => {
    requestsPerHost = new Map()

    components.router.get('/contents/:file', async (ctx) => {
      const host = ctx.request.headers.get('host') ?? 'unknown'
      requestsPerHost.set(host, (requestsPerHost.get(host) ?? 0) + 1)
      if (host === failingHost) {
        return { status: 503, body: 'this server is down' }
      }
      return { body: createReadStream('test/fixtures/' + ctx.params.file) }
    })
  })

  describe('and another server is available', () => {
    let storage: IContentStorageComponent
    let servers: string[]

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()
      const baseUrl = await components.getBaseUrl()
      const { host, origin } = new URL(baseUrl)
      // The same test server under two names, so "another server" is reachable while one host 503s.
      const alias = origin.replace('0.0.0.0', 'localhost')
      failingHost = host
      servers = [baseUrl, alias]
    })

    it('should retry against the other server and complete the download', async () => {
      await downloadFileWithRetries({ storage }, contentHash, targetFolder, servers, new Map(), 5, 0)

      expect(await storage.exist(contentHash)).toBe(true)
    })

    it('should stop retrying the server that failed', async () => {
      await downloadFileWithRetries({ storage }, contentHash, targetFolder, servers, new Map(), 5, 0)

      // The failing host is dropped from the candidate list after its first failure, so it is asked
      // at most once no matter how many retries remain.
      expect(requestsPerHost.get(failingHost) ?? 0).toBeLessThanOrEqual(1)
    })
  })

  describe('and it is the only server available', () => {
    let storage: IContentStorageComponent
    let servers: string[]

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()
      const baseUrl = await components.getBaseUrl()
      failingHost = new URL(baseUrl).host
      servers = [baseUrl]
    })

    it('should keep retrying it and then reject once the retries are exhausted', async () => {
      await expect(
        downloadFileWithRetries({ storage }, contentHash, targetFolder, servers, new Map(), 3, 0)
      ).rejects.toThrow()

      expect(requestsPerHost.get(failingHost)).toBe(3)
    })
  })
})

import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { resolve } from 'path'
import { downloadFileWithRetries } from '../src/downloader'

const contentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'
// Nothing listens here, so every attempt fails immediately.
const deadServer = 'http://127.0.0.1:1'

describe('downloadFileWithRetries', () => {
  let storage: IContentStorageComponent

  beforeEach(() => {
    storage = createInMemoryStorage()
  })

  describe('when the caller is already stopping', () => {
    it('should reject without attempting the download at all', async () => {
      await expect(
        downloadFileWithRetries(
          { storage },
          contentHash,
          resolve('downloads'),
          [deadServer],
          new Map(),
          10,
          0,
          () => true
        )
      ).rejects.toThrow('the caller asked to stop')
    })
  })

  describe('when the caller starts stopping partway through the retry ladder', () => {
    let attempts: number

    beforeEach(() => {
      attempts = 0
    })

    it('should abandon the remaining retries instead of waiting them out', async () => {
      // Reports "stopping" from the third attempt onwards. Without the stop check the ladder would run
      // all 10 attempts, which is what makes a shutdown wait out maxRetries x the request timeout.
      const shouldStop = () => attempts >= 3

      await expect(
        downloadFileWithRetries(
          { storage },
          contentHash,
          resolve('downloads'),
          [
            // Counted through the server picker: every attempt consumes one.
            deadServer
          ],
          new Map(),
          10,
          0,
          () => {
            const stopping = shouldStop()
            attempts++
            return stopping
          }
        )
      ).rejects.toThrow()

      // One check before the first attempt, then one per failure: stopping at the third means we are
      // far short of the ten attempts the ladder allows.
      expect(attempts).toBeLessThan(10)
    })
  })

  describe('when no stop predicate is supplied', () => {
    it('should exhaust the configured retries as before', async () => {
      await expect(
        downloadFileWithRetries({ storage }, contentHash, resolve('downloads'), [deadServer], new Map(), 2, 0)
      ).rejects.toThrow()

      expect(await storage.exist(contentHash)).toBe(false)
    })
  })
})

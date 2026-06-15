import { createInMemoryStorage } from '@dcl/catalyst-storage'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import { Readable } from 'stream'
import { downloadEntityAndContentFiles } from '../src'
import { sleep } from '../src/utils'
import { test } from './components'

// Real, hash-addressed fixtures (their contents hash to these ids), so the hash check on download passes.
const contentHashes = [
  'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6',
  'QmUiEzCQPxz5eHq7KXMrGq7PiM1fnZNvZg2sWELQnuYank',
  'Qma14gteWwHrn61kv4zkRABKzopWd5ZXBppHRhbqikfaev',
  'QmWxyDrJWABXjGonFpUxJD8YYzz2xiFNxupGwYKTbySaZD'
]

test('downloadEntityAndContentFiles content download concurrency', ({ components }) => {
  const contentFolder = resolve('downloads')
  // alphanumeric so it passes hash validation; pre-seeded in storage so the entity download is skipped
  const entityId = 'sceneentitydownloadconcurrencytest'
  let inFlight = 0
  let maxInFlight = 0

  it('prepares the endpoints', () => {
    components.router.get('/contents/:file', async (ctx) => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      // hold the request open so that any concurrent downloads would overlap and be counted
      await sleep(40)
      inFlight--
      return { body: createReadStream('test/fixtures/' + ctx.params.file) }
    })
  })

  describe('when a concurrency of 1 is configured', () => {
    it('should download the content files serially and still fetch all of them', async () => {
      const storage = createInMemoryStorage()
      const entity = {
        type: 'scene',
        content: contentHashes.map((hash, index) => ({ file: `file-${index}`, hash }))
      }
      await storage.storeStream(entityId, Readable.from([Buffer.from(JSON.stringify(entity))]))

      await downloadEntityAndContentFiles(
        { fetcher: components.fetcher, logs: components.logs, metrics: components.metrics, storage },
        entityId,
        [await components.getBaseUrl()],
        new Map(),
        contentFolder,
        10, // maxRetries
        0, // waitTimeBetweenRetries
        1 // contentFilesConcurrency
      )

      expect(maxInFlight).toEqual(1)
      for (const hash of contentHashes) {
        expect(await storage.exist(hash)).toBe(true)
      }
    })
  })
})

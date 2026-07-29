import { Readable } from 'stream'
import { fetchJson } from '../src/utils'
import { test } from './components'

// The cap in src/utils.ts is 50 MiB; stream just past it so the guard trips without buffering the
// whole body in the test process.
const MEGABYTE = 1024 * 1024
const BYTES_TO_SERVE = 51 * MEGABYTE
// Few, large chunks: the guard trips on the running total, so bigger pushes reach the limit with far
// less stream/socket overhead than 1 MiB at a time.
const CHUNK_SIZE = 8 * MEGABYTE

test('fetchJson when the server streams a body larger than the allowed maximum', ({ components }) => {
  it('prepares the endpoints', () => {
    components.router.get('/huge', async (): Promise<any> => {
      const chunk = Buffer.alloc(CHUNK_SIZE, 0x20)
      let sent = 0
      return {
        headers: { 'content-type': 'application/json' },
        body: new Readable({
          read() {
            if (sent >= BYTES_TO_SERVE) {
              this.push(null)
              return
            }
            sent += chunk.length
            this.push(chunk)
          }
        })
      }
    })
  })

  describe('when the body exceeds the maximum allowed size', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject naming the size limit instead of buffering the whole response', async () => {
      await expect(fetchJson(`${baseUrl}/huge`, components.fetcher)).rejects.toThrow(
        'Response body exceeds the maximum allowed size'
      )
    })
  })
})

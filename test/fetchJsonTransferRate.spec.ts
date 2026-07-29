import { Readable } from 'stream'
import { IFetchComponent } from '@dcl/core-commons'
import { fetchJson } from '../src/utils'

// A fetcher whose body arrives as a single small chunk. Everything about the timing is decided by the
// stubbed clock below, so this stays a unit test with no sockets and no real waiting.
function fetcherReturning(payload: string): IFetchComponent {
  return {
    fetch: async () =>
      new Response(Readable.toWeb(Readable.from([Buffer.from(payload)])) as ReadableStream, {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
  } as any
}

describe('fetchJson', () => {
  describe('when a peer sends headers and then delivers the body too slowly to count as progress', () => {
    let clockSpy: jest.SpyInstance
    let error: Error | undefined

    beforeEach(async () => {
      const realNow = Date.now()
      let calls = 0
      // The body read records its start on one Date.now() call and measures elapsed time on the next, so
      // stepping the clock a grace period per call is what makes a prompt body look like a stalled one.
      clockSpy = jest.spyOn(Date, 'now').mockImplementation(() => realNow + calls++ * 70_000)

      error = undefined
      try {
        await fetchJson('http://localhost/unused', fetcherReturning('{"ok":true}'))
      } catch (thrown: any) {
        error = thrown
      }
    })

    afterEach(() => {
      clockSpy.mockRestore()
    })

    it('should reject rather than buffer a body that is not really arriving', () => {
      expect(error).toBeDefined()
    })

    it('should reject with the transfer-rate failure, not a parse or timeout error', () => {
      expect(error!.message).toContain('below the minimum of 4096 bytes/s')
    })
  })

  describe('when a peer delivers the body promptly', () => {
    it('should parse and return it', async () => {
      await expect(fetchJson('http://localhost/unused', fetcherReturning('{"ok":true}'))).resolves.toEqual({
        ok: true
      })
    })
  })
})

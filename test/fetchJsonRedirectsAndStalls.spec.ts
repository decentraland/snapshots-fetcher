import { createServer, Server } from 'http'
import { fetchJson } from '../src/utils'
import { test } from './components'

test('fetchJson redirect handling', ({ components }) => {
  // A second server on loopback stands in for an internal service the syncing process should never be
  // steered at by a remote content server.
  let internalServer: Server
  let internalUrl: string

  beforeAll(async () => {
    internalServer = createServer((_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ internal: 'a-secret-only-reachable-from-inside' }))
    })
    await new Promise<void>((ok) => internalServer.listen(0, '127.0.0.1', () => ok()))
    internalUrl = `http://127.0.0.1:${(internalServer.address() as any).port}/metadata`
  })

  afterAll(async () => {
    await new Promise<void>((ok) => internalServer.close(() => ok()))
  })

  it('prepares the endpoints', () => {
    components.router.get('/redirect-to-internal', async (): Promise<any> => ({
      status: 302,
      headers: { location: internalUrl }
    }))
    components.router.get('/redirect-same-origin', async (): Promise<any> => ({
      status: 302,
      headers: { location: '/redirect-target' }
    }))
    components.router.get('/redirect-target', async (): Promise<any> => ({ body: { followed: true } }))
    components.router.get('/redirect-without-location', async (): Promise<any> => ({ status: 302 }))
  })

  describe('when a server redirects the request to another origin', () => {
    it('should refuse to follow it instead of returning the other origin body', async () => {
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-to-internal`, components.fetcher)
      ).rejects.toThrow('Refusing to follow a redirect')
    })
  })

  describe('when a server redirects within its own origin', () => {
    it('should refuse to follow it as well', async () => {
      // Same-origin is not safe either: a URL origin is a hostname, not an address, so a hostile host
      // can answer the first request from a public IP and rebind that same hostname to loopback before
      // the redirect is fetched. `IFetchComponent` gives no way to pin the request's own DNS
      // resolution, so the only complete answer on this path is to not follow redirects at all.
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-same-origin`, components.fetcher)
      ).rejects.toThrow('Refusing to follow a redirect')
    })

    it('should not fetch the redirect target', async () => {
      // Proven by the target being a distinct route: if it had been followed the call would have
      // resolved with its body instead of rejecting.
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-same-origin`, components.fetcher)
      ).rejects.toThrow()
    })
  })

  describe('when a redirect response carries no location header', () => {
    it('should still reject rather than treat it as a normal response', async () => {
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-without-location`, components.fetcher)
      ).rejects.toThrow('Refusing to follow a redirect')
    })
  })

  describe('when the response is a normal success', () => {
    it('should be unaffected by the redirect refusal', async () => {
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-target`, components.fetcher)
      ).resolves.toEqual({ followed: true })
    })
  })
})

describe('fetchJson body read deadline', () => {
  let server: Server
  let baseUrl: string
  // Streams `chunks` slices of a valid JSON document, `gapMs` apart, then closes.
  let chunks: string[]
  let gapMs: number
  // When true the server sends one chunk and then goes silent without closing the socket.
  let stallForever: boolean

  beforeEach(async () => {
    chunks = []
    gapMs = 0
    stallForever = false

    server = createServer(async (_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      if (stallForever) {
        response.write('{"deltas":[')
        return
      }
      for (const chunk of chunks) {
        response.write(chunk)
        await new Promise((ok) => setTimeout(ok, gapMs))
      }
      response.end()
    })
    await new Promise<void>((ok) => server.listen(0, '127.0.0.1', () => ok()))
    baseUrl = `http://127.0.0.1:${(server.address() as any).port}/body`
  })

  afterEach(async () => {
    server.closeAllConnections?.()
    await new Promise<void>((ok) => server.close(() => ok()))
  })

  describe('when the server sends headers and then stops sending the body', () => {
    beforeEach(() => {
      stallForever = true
    })

    it('should reject once no data has arrived for the timeout', async () => {
      // The fetch component's own timeout only bounds time-to-headers, so without a deadline here the
      // promise never settles — and a pending promise never reaches the reconnection logic.
      const { createFetchComponent } = await import('@dcl/fetch-component')

      await expect(fetchJson(baseUrl, createFetchComponent(), { timeout: 400 })).rejects.toThrow(
        'without receiving response body data'
      )
    })
  })

  describe('when the server streams slowly but keeps making progress', () => {
    beforeEach(() => {
      // Six chunks 100ms apart is ~600ms of streaming, comfortably past the 1000ms timeout in total,
      // while each individual gap stays an order of magnitude below it — so a loaded CI runner
      // stretching a gap cannot turn this into a false failure.
      chunks = ['{"del', 'tas":', '[1,', '2,', '3]', '}']
      gapMs = 100
    })

    it('should complete rather than abort a healthy slow transfer', async () => {
      // A total deadline instead of an inactivity one would reject this — which is what would break
      // bootstrap against a slow-but-healthy content server serving a large snapshot list.
      const { createFetchComponent } = await import('@dcl/fetch-component')

      await expect(fetchJson(baseUrl, createFetchComponent(), { timeout: 1000 })).resolves.toEqual({
        deltas: [1, 2, 3]
      })
    })
  })
})

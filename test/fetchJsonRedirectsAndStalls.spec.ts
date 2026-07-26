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
      ).rejects.toThrow('Refusing to follow a cross-origin redirect')
    })
  })

  describe('when a server redirects within its own origin', () => {
    it('should follow it and return the final body', async () => {
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-same-origin`, components.fetcher)
      ).resolves.toEqual({ followed: true })
    })
  })

  describe('when a redirect response carries no location header', () => {
    it('should reject naming the missing location', async () => {
      await expect(
        fetchJson(`${await components.getBaseUrl()}/redirect-without-location`, components.fetcher)
      ).rejects.toThrow('without a location')
    })
  })
})

describe('fetchJson when a server sends headers and then stops sending the body', () => {
  let stalledServer: Server
  let stalledUrl: string

  beforeEach(async () => {
    stalledServer = createServer((_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      // A partial document, then silence — the socket is never closed.
      response.write('{"deltas":[')
    })
    await new Promise<void>((ok) => stalledServer.listen(0, '127.0.0.1', () => ok()))
    stalledUrl = `http://127.0.0.1:${(stalledServer.address() as any).port}/stalled`
  })

  afterEach(async () => {
    stalledServer.closeAllConnections?.()
    await new Promise<void>((ok) => stalledServer.close(() => ok()))
  })

  it('should time out reading the body rather than hanging forever', async () => {
    // The fetch component's own timeout only bounds time-to-headers, so without a deadline on the body
    // read this promise never settles — and a pending promise never reaches the reconnection logic.
    const { createFetchComponent } = await import('@dcl/fetch-component')

    await expect(fetchJson(stalledUrl, createFetchComponent(), { timeout: 500 })).rejects.toThrow(
      'while reading the response body'
    )
  })
})

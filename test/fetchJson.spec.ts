import { fetchJson } from '../src/utils'
import { test } from './components'

test('fetchJson', ({ components }) => {
  it('prepares the endpoints', () => {
    components.router.get('/empty', async (): Promise<any> => ({ status: 204 }))
    components.router.get('/not-found', async (): Promise<any> => ({ status: 404, body: 'nope' }))
    components.router.get('/server-error', async (): Promise<any> => ({ status: 500, body: 'boom' }))
    components.router.get('/ok', async (): Promise<any> => ({ body: { hello: 'world' } }))
  })

  describe('when the response has no body', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject reporting the empty body rather than a JSON parse error', async () => {
      await expect(fetchJson(`${baseUrl}/empty`, components.fetcher)).rejects.toThrow('The response body was empty')
    })
  })

  describe('when the server answers with a client error', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject reporting the status code', async () => {
      await expect(fetchJson(`${baseUrl}/not-found`, components.fetcher)).rejects.toThrow('Status code was: 404')
    })
  })

  describe('when the server answers with a server error', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject reporting the status code', async () => {
      await expect(fetchJson(`${baseUrl}/server-error`, components.fetcher)).rejects.toThrow('Status code was: 500')
    })
  })

  describe('when the server answers with a JSON document', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should resolve with the parsed body', async () => {
      await expect(fetchJson(`${baseUrl}/ok`, components.fetcher)).resolves.toEqual({ hello: 'world' })
    })
  })
})

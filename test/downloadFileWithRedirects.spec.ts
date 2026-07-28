import { createTestMetricsComponent } from '@dcl/metrics'
import { resolve } from 'path'
import { metricsDefinitions } from '../src/metrics'
import { saveContentFileToDisk, streamToBuffer } from '../src/utils'
import { test } from './components'

test('downloadFile redirect handling', ({ components }) => {
  const metrics = createTestMetricsComponent(metricsDefinitions)
  const contentFolder = resolve('downloads')
  const content = Buffer.from('redirect-target-content', 'utf-8')

  it('prepares the endpoints', () => {
    // Chained redirects: /start -> /sub/second -> (relative) "third".
    // The relative "third" must resolve against /sub/second (the redirecting URL), i.e. /sub/third,
    // NOT against the original /start. /third is wired to 404 to catch the old (buggy) base behavior.
    components.router.get('/start', async () => ({ status: 302, headers: { location: '/sub/second' } }))
    components.router.get('/sub/second', async () => ({ status: 302, headers: { location: 'third' } }))
    components.router.get('/sub/third', async () => ({ body: content.toString() }))
    components.router.get('/third', async () => ({ status: 404 }))

    // Redirect to an unsupported (non-http) protocol.
    components.router.get('/redirect-to-file', async () => ({
      status: 302,
      headers: { location: 'file:///etc/passwd' }
    }))

    // The other standard redirect codes, all common in front of object storage.
    components.router.get('/see-other', async () => ({ status: 303, headers: { location: '/target' } }))
    components.router.get('/temporary', async () => ({ status: 307, headers: { location: '/target' } }))
    components.router.get('/permanent', async () => ({ status: 308, headers: { location: '/target' } }))
    components.router.get('/target', async () => ({ body: content.toString() }))

    // 300 carries a list of choices, not the content, and a redirect without a Location is unusable.
    components.router.get('/multiple-choices', async () => ({ status: 300, body: 'not the content' }))
    components.router.get('/redirect-without-location', async () => ({ status: 302 }))

    // A redirect that also carries a sizeable body, which is thrown away rather than read.
    components.router.get('/redirect-with-body', async () => ({
      status: 302,
      headers: { location: '/target' },
      body: 'x'.repeat(512 * 1024)
    }))
  })

  describe('when a redirect response also carries a body', () => {
    it('should follow it and store the target content, not the discarded redirect body', async () => {
      await saveContentFileToDisk(
        { metrics, storage: components.storage },
        (await components.getBaseUrl()) + '/redirect-with-body',
        resolve(contentFolder, 'redirect-with-body'),
        'redirect-with-body',
        false
      )

      const stored = await streamToBuffer(await (await components.storage.retrieve('redirect-with-body'))!.asStream())
      expect(stored).toEqual(content)
    })
  })

  describe.each([
    ['303 See Other', '/see-other'],
    ['307 Temporary Redirect', '/temporary'],
    ['308 Permanent Redirect', '/permanent']
  ])('when the server answers with %s', (_label: string, path: string) => {
    it('should follow it and download the content', async () => {
      const hash = `redirect${path.replace(/\W/g, '')}`
      await saveContentFileToDisk(
        { metrics, storage: components.storage },
        (await components.getBaseUrl()) + path,
        resolve(contentFolder, hash),
        hash,
        false
      )

      const stored = await streamToBuffer(await (await components.storage.retrieve(hash))!.asStream())
      expect(stored).toEqual(content)
    })
  })

  describe('when the server answers with 300 Multiple Choices', () => {
    it('should reject rather than storing the choices document as the content', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/multiple-choices',
          resolve(contentFolder, 'multiple-choices'),
          'multiple-choices',
          false
        )
      ).rejects.toThrow('status: 300')
    })
  })

  describe('when a redirect status arrives without a Location header', () => {
    it('should reject instead of treating the empty body as the content', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/redirect-without-location',
          resolve(contentFolder, 'no-location'),
          'no-location',
          false
        )
      ).rejects.toThrow('status: 302')
    })
  })

  describe('when a relative redirect is followed', () => {
    it('should resolve it against the redirecting URL and download the content', async () => {
      await saveContentFileToDisk(
        { metrics, storage: components.storage },
        (await components.getBaseUrl()) + '/start',
        resolve(contentFolder, 'redirect-chain'),
        'redirect-chain',
        false
      )

      const stored = await streamToBuffer(await (await components.storage.retrieve('redirect-chain'))!.asStream())
      expect(stored).toEqual(content)
    })
  })

  describe('when a redirect points to a non-http(s) protocol', () => {
    it('should reject with an unsupported protocol error', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/redirect-to-file',
          resolve(contentFolder, 'unsupported-protocol'),
          'unsupported-protocol',
          false
        )
      ).rejects.toThrow('Unsupported protocol')
    })
  })
})

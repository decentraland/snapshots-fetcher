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

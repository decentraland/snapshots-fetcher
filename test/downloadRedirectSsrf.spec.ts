import { createTestMetricsComponent } from '@dcl/metrics'
import { resolve } from 'path'
import { metricsDefinitions } from '../src/metrics'
import { isNonPublicAddress, saveContentFileToDisk } from '../src/utils'
import { test } from './components'

describe('isNonPublicAddress', () => {
  describe('when the address is reachable only from inside a network', () => {
    it.each([
      ['10.0.0.1', 'RFC1918 10/8'],
      ['172.16.5.4', 'RFC1918 172.16/12'],
      ['172.31.255.255', 'RFC1918 172.16/12 upper bound'],
      ['192.168.1.1', 'RFC1918 192.168/16'],
      ['127.0.0.1', 'loopback'],
      ['169.254.169.254', 'link-local cloud metadata'],
      ['0.0.0.0', 'this network'],
      ['100.64.0.1', 'carrier-grade NAT'],
      ['224.0.0.1', 'multicast'],
      ['::1', 'IPv6 loopback'],
      ['fe80::1', 'IPv6 link-local'],
      ['fd00::1', 'IPv6 unique local'],
      ['::ffff:10.0.0.1', 'IPv4-mapped private address']
    ])('should return true for %s (%s)', (address) => {
      expect(isNonPublicAddress(address)).toBe(true)
    })
  })

  describe('when the address is publicly routable', () => {
    it.each([['8.8.8.8'], ['1.1.1.1'], ['172.15.0.1'], ['172.32.0.1'], ['192.167.0.1'], ['99.99.99.99'], ['2606:4700::1']])(
      'should return false for %s',
      (address) => {
        expect(isNonPublicAddress(address)).toBe(false)
      }
    )
  })
})

test('downloadFile when a remote server redirects the download elsewhere', ({ components }) => {
  const metrics = createTestMetricsComponent(metricsDefinitions)
  const contentFolder = resolve('downloads')

  it('prepares the endpoints', () => {
    // The cloud metadata endpoint, as a literal IP. Nothing resolves, so only the direct hostname
    // check can catch this one.
    components.router.get('/to-metadata', async () => ({
      status: 302,
      headers: { location: 'http://169.254.169.254/latest/meta-data/iam/security-credentials/' }
    }))
    // A private address behind a hostname. The test server binds 0.0.0.0, so `localhost` is a
    // *different* host that resolves to loopback — which is what the DNS guard has to catch.
    components.router.get('/to-loopback-hostname', async () => ({
      status: 302,
      headers: { location: `http://localhost:${new URL(await components.getBaseUrl()).port}/whatever` }
    }))
  })

  describe('and the redirect points at a link-local address', () => {
    it('should refuse to follow it instead of issuing the request', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/to-metadata',
          resolve(contentFolder, 'ssrf-metadata'),
          'ssrf-metadata',
          false
        )
      ).rejects.toThrow('not a public address')
    })
  })

  describe('and the redirect points at a hostname that resolves to a private address', () => {
    it('should refuse to follow it', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/to-loopback-hostname',
          resolve(contentFolder, 'ssrf-hostname'),
          'ssrf-hostname',
          false
        )
      ).rejects.toThrow('not a public address')
    })
  })
})

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
      ['192.0.0.1', 'IETF protocol assignments'],
      ['192.0.2.1', 'IPv4 documentation TEST-NET-1'],
      ['192.88.99.1', 'deprecated 6to4 relay anycast'],
      ['198.18.0.1', 'IPv4 benchmarking'],
      ['198.51.100.1', 'IPv4 documentation TEST-NET-2'],
      ['203.0.113.1', 'IPv4 documentation TEST-NET-3'],
      ['224.0.0.1', 'multicast'],
      ['::1', 'IPv6 loopback'],
      ['::', 'IPv6 unspecified'],
      ['fe80::1', 'IPv6 link-local'],
      ['febf::1', 'IPv6 link-local upper bound'],
      ['fc00::1', 'IPv6 unique local'],
      ['fd00::1', 'IPv6 unique local'],
      ['ff02::1', 'IPv6 multicast'],
      ['64:ff9b:1::1', 'IPv6 local translation'],
      ['100::1', 'IPv6 discard-only'],
      ['100:0:0:1::1', 'IPv6 dummy prefix'],
      ['2001:2::1', 'IPv6 benchmarking'],
      ['2001:db8::1', 'IPv6 documentation'],
      ['2002::1', 'IPv6 6to4'],
      ['3fff::1', 'IPv6 documentation'],
      ['5f00::1', 'IPv6 segment-routing SIDs'],
      ['::ffff:10.0.0.1', 'IPv4-mapped private address, dotted form'],
      // WHATWG URL rewrites every IPv4-mapped literal into compressed hex, so these are the forms
      // that actually reach the guard. Asserting only the dotted form above is what let a bypass
      // through CI previously.
      ['::ffff:7f00:1', 'IPv4-mapped 127.0.0.1 as URL emits it'],
      ['::ffff:a9fe:a9fe', 'IPv4-mapped 169.254.169.254 (cloud metadata) as URL emits it'],
      ['::ffff:a00:1', 'IPv4-mapped 10.0.0.1 as URL emits it'],
      ['::7f00:1', 'deprecated IPv4-compatible loopback'],
      ['0:0:0:0:0:ffff:7f00:1', 'fully expanded IPv4-mapped loopback'],
      ['not-an-ip', 'unparseable input is refused rather than trusted']
    ])('should return true for %s (%s)', (address) => {
      expect(isNonPublicAddress(address)).toBe(true)
    })
  })

  describe('when the address is publicly routable', () => {
    it.each([
      ['8.8.8.8'],
      ['1.1.1.1'],
      ['172.15.0.1'],
      ['172.32.0.1'],
      ['192.0.0.9'],
      ['192.0.0.10'],
      ['192.167.0.1'],
      ['99.99.99.99'],
      ['64:ff9b::808:808'],
      ['2001:1::1'],
      ['2001:3::1'],
      ['2001:4:112::1'],
      ['2001:20::1'],
      ['2001:30::1'],
      ['2606:4700::1']
    ])(
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
    // The bracketed IPv4-mapped form of 127.0.0.1. URL normalises it to [::ffff:7f00:1].
    components.router.get('/to-mapped-loopback', async () => ({
      status: 302,
      headers: { location: `http://[::ffff:127.0.0.1]:${new URL(await components.getBaseUrl()).port}/whatever` }
    }))
    // Location values the URL parser rejects outright.
    components.router.get('/to-unparseable', async () => ({ status: 302, headers: { location: 'http://[' } }))
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

  describe('and the redirect uses the IPv4-mapped IPv6 form of a private address', () => {
    it('should refuse to follow it, not just the dotted-quad spelling', async () => {
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/to-mapped-loopback',
          resolve(contentFolder, 'ssrf-mapped'),
          'ssrf-mapped',
          false
        )
      ).rejects.toThrow('not a public address')
    })
  })

  describe('and the redirect location cannot be parsed as a URL', () => {
    it('should reject rather than crash the process with an uncaught TypeError', async () => {
      // The redirect is handled inside the HTTP response listener, so a throw there escapes the
      // enclosing Promise and becomes an uncaughtException.
      await expect(
        saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/to-unparseable',
          resolve(contentFolder, 'ssrf-unparseable'),
          'ssrf-unparseable',
          false
        )
      ).rejects.toThrow('Invalid redirect location')
    })
  })
})

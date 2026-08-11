/**
 * Circuit-breaker per peer — behaviour tests for the module-level state in downloader.ts.
 *
 * Strategy:
 *  - Direct manipulation of the exported `peerCircuitBreaker` map lets us set up pre-tripped
 *    state without running through the full retry ladder.
 *  - End-to-end round-trips via `downloadFileWithRetries` verify that the circuit breaker is
 *    actually wired into the download path (trip and reset behaviour).
 *  - `clearPeerCircuitBreaker()` is called in `afterEach` so no state leaks between cases.
 *
 * Test-server convention:
 *  - requests to 'localhost:PORT' → serve the fixture file (good peer)
 *  - requests to '0.0.0.0:PORT'  → return 503 (bad peer)
 *  The test server listens on 0.0.0.0, so both hostnames reach the same socket.
 */

import { createInMemoryStorage } from '@dcl/catalyst-storage'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import {
  CIRCUIT_BREAKER_COOLDOWN_MS,
  CIRCUIT_BREAKER_THRESHOLD,
  clearPeerCircuitBreaker,
  downloadFileWithRetries,
  peerCircuitBreaker
} from '../src/downloader'
import { test } from './components'

// A real hash-addressed fixture whose bytes are on disk so the hash check can pass.
const FIXTURE_HASH = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'

test('circuit breaker per peer', ({ components }) => {
  const targetFolder = resolve('downloads')
  let requestsPerHost: Map<string, number>
  let badPeer: string  // 0.0.0.0:PORT — always returns 503
  let goodPeer: string // localhost:PORT — serves the fixture

  it('prepares the test endpoint and peer URLs', async () => {
    requestsPerHost = new Map()

    const baseUrl = await components.getBaseUrl()
    const { origin } = new URL(baseUrl)
    badPeer = baseUrl
    goodPeer = origin.replace('0.0.0.0', 'localhost')

    components.router.get('/contents/:hash', async (ctx) => {
      const host = ctx.request.headers.get('host') ?? 'unknown'
      requestsPerHost.set(host, (requestsPerHost.get(host) ?? 0) + 1)
      // Serve the real fixture only from 'localhost'; everything else returns 503.
      if (!host.startsWith('localhost')) {
        return { status: 503, body: 'simulated bad peer' }
      }
      const file = `test/fixtures/${ctx.params.hash}`
      return { body: createReadStream(file) }
    })
  })

  afterEach(() => {
    clearPeerCircuitBreaker()
  })

  // ---------------------------------------------------------------------------
  // Exported constants
  // ---------------------------------------------------------------------------

  describe('exported constants', () => {
    it('CIRCUIT_BREAKER_THRESHOLD is a positive integer', () => {
      expect(Number.isInteger(CIRCUIT_BREAKER_THRESHOLD)).toBe(true)
      expect(CIRCUIT_BREAKER_THRESHOLD).toBeGreaterThan(0)
    })

    it('CIRCUIT_BREAKER_COOLDOWN_MS is a positive number', () => {
      expect(CIRCUIT_BREAKER_COOLDOWN_MS).toBeGreaterThan(0)
    })
  })

  // ---------------------------------------------------------------------------
  // Pre-tripped circuit: a tripped peer is excluded from downloads
  // ---------------------------------------------------------------------------

  describe('when a peer circuit is pre-tripped', () => {
    let storage: ReturnType<typeof createInMemoryStorage>

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()

      // Pre-trip the circuit for the bad peer so no retry is needed to observe the exclusion.
      peerCircuitBreaker.set(badPeer, {
        consecutiveFailures: 0,
        cooledUntil: Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS
      })
    })

    it('does not issue any requests to the tripped peer', async () => {
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 3, 0)

      expect(requestsPerHost.get(new URL(badPeer).host) ?? 0).toBe(0)
    })

    it('completes the download successfully via the healthy peer', async () => {
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 3, 0)

      expect(await storage.exist(FIXTURE_HASH)).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // All-peers-tripped fallback: when every peer is tripped, try all of them
  // ---------------------------------------------------------------------------

  describe('when all peers are tripped simultaneously', () => {
    let storage: ReturnType<typeof createInMemoryStorage>

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()

      // Trip BOTH peers so the fallback path is exercised.
      peerCircuitBreaker.set(badPeer, { consecutiveFailures: 0, cooledUntil: Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS })
      peerCircuitBreaker.set(goodPeer, { consecutiveFailures: 0, cooledUntil: Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS })
    })

    it('falls back to trying all peers rather than stalling', async () => {
      // With both peers tripped the fallback must still make progress rather than returning
      // immediately or hanging.  The download uses goodPeer (localhost) which serves the file.
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 5, 0)

      expect(await storage.exist(FIXTURE_HASH)).toBe(true)
    })

    it('issues at least one network request even when all peers are tripped', async () => {
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 5, 0)

      const badRequests = requestsPerHost.get(new URL(badPeer).host) ?? 0
      const goodRequests = requestsPerHost.get(new URL(goodPeer).host) ?? 0
      expect(badRequests + goodRequests).toBeGreaterThan(0)
    })
  })

  // ---------------------------------------------------------------------------
  // Natural trip: circuit trips after N consecutive failures
  // ---------------------------------------------------------------------------

  describe('when a peer fails repeatedly', () => {
    let storage: ReturnType<typeof createInMemoryStorage>

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()
    })

    it('accumulates consecutive failures and trips the circuit', async () => {
      // Drive CIRCUIT_BREAKER_THRESHOLD failures by downloading against the bad peer only,
      // each time with maxRetries=1 (one attempt per call) so each call adds one failure.
      for (let i = 0; i < CIRCUIT_BREAKER_THRESHOLD; i++) {
        try {
          await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer], new Map(), 1, 0)
        } catch {
          // expected — the only peer is bad
        }
      }

      const state = peerCircuitBreaker.get(badPeer)
      expect(state).toBeDefined()
      expect(state!.cooledUntil).toBeGreaterThan(Date.now())
    })

    it('excludes the tripped peer from subsequent downloads', async () => {
      // Trip the bad peer naturally.
      for (let i = 0; i < CIRCUIT_BREAKER_THRESHOLD; i++) {
        try {
          await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer], new Map(), 1, 0)
        } catch {}
      }

      // The circuit is now tripped; a download using both peers should bypass the bad one entirely.
      requestsPerHost.clear()
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 3, 0)

      expect(requestsPerHost.get(new URL(badPeer).host) ?? 0).toBe(0)
      expect(await storage.exist(FIXTURE_HASH)).toBe(true)
    })

    it('does not trip the circuit below the threshold', async () => {
      // One fewer than threshold — should NOT trip.
      for (let i = 0; i < CIRCUIT_BREAKER_THRESHOLD - 1; i++) {
        try {
          await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer], new Map(), 1, 0)
        } catch {}
      }

      const state = peerCircuitBreaker.get(badPeer)
      // Either no state, or the state's cooledUntil is still 0 (not tripped yet).
      const isTripped = state !== undefined && state.cooledUntil > Date.now()
      expect(isTripped).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // Reset on success: a successful download clears the failure count
  // ---------------------------------------------------------------------------

  describe('when a previously failing peer succeeds', () => {
    let storage: ReturnType<typeof createInMemoryStorage>

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()
    })

    it('clears the circuit state on a successful download', async () => {
      // Plant some failure history (below the trip threshold) for the good peer.
      peerCircuitBreaker.set(goodPeer, { consecutiveFailures: CIRCUIT_BREAKER_THRESHOLD - 1, cooledUntil: 0 })

      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [goodPeer], new Map(), 3, 0)

      // Success must remove the circuit state for this peer.
      expect(peerCircuitBreaker.has(goodPeer)).toBe(false)
    })

    it('does not affect other peers when one succeeds', async () => {
      // The bad peer has some failure history.
      peerCircuitBreaker.set(badPeer, { consecutiveFailures: 2, cooledUntil: 0 })

      // Download only via the good peer — this should succeed and clear goodPeer's state,
      // but leave badPeer's state untouched.
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [goodPeer], new Map(), 3, 0)

      const badPeerState = peerCircuitBreaker.get(badPeer)
      expect(badPeerState).toBeDefined()
      expect(badPeerState!.consecutiveFailures).toBe(2) // untouched
    })
  })

  // ---------------------------------------------------------------------------
  // Cooldown expiry: an expired cooldown re-admits the peer as a candidate
  // ---------------------------------------------------------------------------

  describe('when the cooldown window expires', () => {
    let storage: ReturnType<typeof createInMemoryStorage>

    beforeEach(async () => {
      storage = createInMemoryStorage()
      requestsPerHost = new Map()
    })

    it('re-admits the peer after its cooldown expires and allows a successful download', async () => {
      // goodPeer had its circuit tripped, but the cooldown has already passed (epoch 1 = far past).
      peerCircuitBreaker.set(goodPeer, { consecutiveFailures: 0, cooledUntil: 1 })

      // Download using only goodPeer.  Its cooldown is expired, so it is included in candidates.
      // The download should succeed (goodPeer serves the fixture).
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [goodPeer], new Map(), 3, 0)

      // The download succeeded → goodPeer was re-admitted and its circuit was reset on success.
      expect(await storage.exist(FIXTURE_HASH)).toBe(true)
      expect(peerCircuitBreaker.has(goodPeer)).toBe(false) // success cleared the state
    })

    it('excludes a peer with an active (non-expired) cooldown', async () => {
      // badPeer has an ACTIVE cooldown that has not yet expired.
      peerCircuitBreaker.set(badPeer, { consecutiveFailures: 0, cooledUntil: Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS })

      // With badPeer excluded, only goodPeer is in candidates — download succeeds cleanly.
      requestsPerHost.clear()
      await downloadFileWithRetries({ storage }, FIXTURE_HASH, targetFolder, [badPeer, goodPeer], new Map(), 3, 0)

      expect(requestsPerHost.get(new URL(badPeer).host) ?? 0).toBe(0)
      expect(await storage.exist(FIXTURE_HASH)).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // clearPeerCircuitBreaker: resets all state
  // ---------------------------------------------------------------------------

  describe('clearPeerCircuitBreaker()', () => {
    it('removes all entries from the map', () => {
      peerCircuitBreaker.set('http://peer-a.test', { consecutiveFailures: 3, cooledUntil: 0 })
      peerCircuitBreaker.set('http://peer-b.test', { consecutiveFailures: 0, cooledUntil: Date.now() + 1000 })

      clearPeerCircuitBreaker()

      expect(peerCircuitBreaker.size).toBe(0)
    })
  })
})

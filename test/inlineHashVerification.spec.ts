/**
 * Tests for the inline SHA-256 hash computation and verification added to utils.ts.
 *
 * Three areas covered:
 *
 * 1. `verifyHashFromInlineDigest` — pure unit tests.
 *    Verifies the three-valued return (true/false/null) for Qm (CIDv0), raw ba (CIDv1 codec
 *    0x55), DAG-PB ba (CIDv1 codec 0x70), and unknown formats.
 *
 * 2. `createInlineHasher` — unit tests.
 *    Verifies the Transform is transparent (bytes pass through unchanged) and that the
 *    SHA-256 digest is correct for single-chunk, multi-chunk, and empty streams.
 *
 * 3. `saveContentFileToDisk` integration tests.
 *    Exercises the full download pipeline end-to-end: good Qm content, corrupt Qm content
 *    (detected inline without re-reading), good raw-ba content, corrupt raw-ba content,
 *    and a DAG-PB CID that falls through to the `assertHash` fallback.
 *
 * The test server distinguishes a "good" peer by host: requests reaching the server under
 * 'localhost:PORT' are served normally; under '0.0.0.0:PORT' certain endpoints return 503.
 */

import { createTestMetricsComponent } from '@dcl/metrics'
import { hashV0, hashV1 } from '@dcl/hashing'
import { createInMemoryStorage } from '@dcl/catalyst-storage'
import * as crypto from 'crypto'
import { resolve } from 'path'
import { pipeline } from 'stream/promises'
import { Readable, Writable } from 'stream'
import {
  createInlineHasher,
  saveContentFileToDisk,
  verifyHashFromInlineDigest
} from '../src/utils'
import { metricsDefinitions } from '../src/metrics'
import { test } from './components'

// ---------------------------------------------------------------------------
// Test vectors
// ---------------------------------------------------------------------------

// Any 'bafy...' CIDv1 has version=0x01 and codec=0x70 (dag-pb) in its first two bytes,
// which the `verifyHashFromInlineDigest` function cannot verify from a raw SHA-256 digest.
// This structurally valid CIDv1 does not correspond to any real content — it is used only
// to test the "return null for DAG-PB" branch.
const DAG_PB_CID_V1 = 'bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354'

// ---------------------------------------------------------------------------
// 1. verifyHashFromInlineDigest — pure unit tests (no network)
// ---------------------------------------------------------------------------

describe('verifyHashFromInlineDigest', () => {
  const content = Buffer.from('verifyHashFromInlineDigest test payload')
  const sha256Digest = crypto.createHash('sha256').update(content).digest()

  const otherContent = Buffer.from('completely different payload')
  const otherDigest = crypto.createHash('sha256').update(otherContent).digest()

  // ---- CIDv0 (Qm prefix) ---------------------------------------------------

  it('returns true for the correct CIDv0 (Qm) hash', async () => {
    const qmHash = await hashV0(content)
    expect(verifyHashFromInlineDigest(sha256Digest, qmHash)).toBe(true)
  })

  it('returns false when the digest does not match the CIDv0 hash', async () => {
    const qmHash = await hashV0(content)
    // otherDigest is from a different content — cannot match the CIDv0 of `content`.
    expect(verifyHashFromInlineDigest(otherDigest, qmHash)).toBe(false)
  })

  it('returns false when the Qm hash belongs to different content', async () => {
    const qmHashOfOther = await hashV0(otherContent)
    expect(verifyHashFromInlineDigest(sha256Digest, qmHashOfOther)).toBe(false)
  })

  // ---- CIDv1 raw (ba prefix, codec 0x55) -----------------------------------

  it('returns true for the correct raw CIDv1 (ba codec 0x55) hash', async () => {
    const baHash = await hashV1(content) // small content → raw CIDv1 (codec 0x55)
    expect(verifyHashFromInlineDigest(sha256Digest, baHash)).toBe(true)
  })

  it('returns false when the digest does not match the raw CIDv1 hash', async () => {
    const baHash = await hashV1(content)
    expect(verifyHashFromInlineDigest(otherDigest, baHash)).toBe(false)
  })

  it('returns false when the ba hash belongs to different content', async () => {
    const baHashOfOther = await hashV1(otherContent)
    expect(verifyHashFromInlineDigest(sha256Digest, baHashOfOther)).toBe(false)
  })

  // ---- CIDv1 dag-pb (ba prefix, codec 0x70) --------------------------------

  it('returns null for a DAG-PB CIDv1 (bafy prefix = codec 0x70)', () => {
    // SHA-256 alone cannot reconstruct a DAG-PB Merkle root — must fall back to assertHash.
    expect(verifyHashFromInlineDigest(sha256Digest, DAG_PB_CID_V1)).toBeNull()
  })

  // ---- Unknown / malformed -------------------------------------------------

  it('returns null for an unknown hash prefix', () => {
    expect(verifyHashFromInlineDigest(sha256Digest, 'unknownhash123abc')).toBeNull()
  })

  it('returns null for an empty string', () => {
    expect(verifyHashFromInlineDigest(sha256Digest, '')).toBeNull()
  })

  it('returns null for a malformed ba hash with invalid base32 characters', () => {
    // '0' and '1' are not in the RFC 4648 base32 alphabet.
    expect(verifyHashFromInlineDigest(sha256Digest, 'ba01invalidbase32')).toBeNull()
  })

  it('returns null for a ba hash that is too short to contain a codec byte', () => {
    // 'ba' alone decodes to very few bytes — not enough for version + codec.
    expect(verifyHashFromInlineDigest(sha256Digest, 'baa')).toBeNull()
  })

  // ---- Round-trip consistency with createInlineHasher ----------------------

  it('is consistent with createInlineHasher for Qm hashes', async () => {
    const hasher = createInlineHasher()
    await pipeline(Readable.from([content]), hasher, new Writable({ write(_c, _e, cb) { cb() } }))

    const qmHash = await hashV0(content)
    expect(verifyHashFromInlineDigest(hasher.getSha256Digest(), qmHash)).toBe(true)
  })

  it('is consistent with createInlineHasher for raw CIDv1 hashes', async () => {
    const hasher = createInlineHasher()
    await pipeline(Readable.from([content]), hasher, new Writable({ write(_c, _e, cb) { cb() } }))

    const baHash = await hashV1(content)
    expect(verifyHashFromInlineDigest(hasher.getSha256Digest(), baHash)).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// 2. createInlineHasher — unit tests (no network)
// ---------------------------------------------------------------------------

describe('createInlineHasher', () => {
  function devNull(): Writable {
    return new Writable({ write(_chunk, _enc, cb) { cb() } })
  }

  const content = Buffer.from('createInlineHasher test payload bytes')

  it('passes all bytes through unchanged (transparent transform)', async () => {
    const hasher = createInlineHasher()
    const received: Buffer[] = []
    await pipeline(
      Readable.from([content]),
      hasher,
      new Writable({
        write(chunk, _enc, cb) {
          received.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
          cb()
        }
      })
    )
    expect(Buffer.concat(received)).toEqual(content)
  })

  it('computes the correct SHA-256 digest for a single chunk', async () => {
    const hasher = createInlineHasher()
    await pipeline(Readable.from([content]), hasher, devNull())

    const expected = crypto.createHash('sha256').update(content).digest()
    expect(hasher.getSha256Digest()).toEqual(expected)
  })

  it('computes the correct SHA-256 digest for a multi-chunk stream', async () => {
    const parts = [Buffer.from('chunk one — '), Buffer.from('chunk two — '), Buffer.from('chunk three')]
    const hasher = createInlineHasher()
    await pipeline(Readable.from(parts), hasher, devNull())

    const expected = crypto.createHash('sha256')
      .update(parts[0]).update(parts[1]).update(parts[2]).digest()
    expect(hasher.getSha256Digest()).toEqual(expected)
  })

  it('computes the correct SHA-256 digest for an empty stream', async () => {
    const hasher = createInlineHasher()
    await pipeline(Readable.from([]), hasher, devNull())

    const expected = crypto.createHash('sha256').digest()
    expect(hasher.getSha256Digest()).toEqual(expected)
  })

  it('computes the correct SHA-256 digest for large content (many chunks)', async () => {
    // 1 000 chunks of 1 KiB each → 1 MiB total — exercises update() across many calls.
    const chunk = Buffer.alloc(1024, 0x42)
    const chunks = Array.from({ length: 1000 }, () => chunk)
    const hasher = createInlineHasher()
    await pipeline(Readable.from(chunks), hasher, devNull())

    const hCtx = crypto.createHash('sha256')
    for (const c of chunks) hCtx.update(c)
    expect(hasher.getSha256Digest()).toEqual(hCtx.digest())
  })

  it('throws if getSha256Digest is called before the stream ends', () => {
    const hasher = createInlineHasher()
    // The stream pipeline has not been started yet, so digest is not available.
    expect(() => hasher.getSha256Digest()).toThrow('called before the stream ended')
  })

  it('digest produced matches hashV0 (Qm hash) via verifyHashFromInlineDigest', async () => {
    const hasher = createInlineHasher()
    await pipeline(Readable.from([content]), hasher, devNull())

    const qmHash = await hashV0(content)
    expect(verifyHashFromInlineDigest(hasher.getSha256Digest(), qmHash)).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// 3. saveContentFileToDisk integration — full download pipeline
// ---------------------------------------------------------------------------

test('saveContentFileToDisk with inline hash verification', ({ components }) => {
  const contentFolder = resolve('downloads')
  const metrics = createTestMetricsComponent(metricsDefinitions)

  // Test content — small enough to produce a raw CIDv1 (codec 0x55) from hashV1.
  const content = Buffer.from('hello world, this is a small test payload for inline hash verification')

  let qmHash: string
  let baHash: string
  let baseUrl: string

  it('computes hashes and registers test endpoints', async () => {
    qmHash = await hashV0(content)
    baHash = await hashV1(content)
    baseUrl = await components.getBaseUrl()

    // Correct content at the expected hash path.
    components.router.get(`/contents/${qmHash}`, async () => ({
      body: Buffer.from(content)
    }))
    components.router.get(`/contents/${baHash}`, async () => ({
      body: Buffer.from(content)
    }))

    // Corrupt content — bytes that will NOT match any hash of `content`.
    components.router.get('/contents/corrupt', async () => ({
      body: Buffer.from('CORRUPT BYTES: these do not match any expected hash')
    }))

    // Arbitrary content behind a DAG-PB CIDv1.
    // verifyHashFromInlineDigest returns null (cannot verify DAG-PB from SHA-256 alone),
    // so the pipeline falls back to assertHash, which will also reject it.
    components.router.get('/contents/dagpb', async () => ({
      body: Buffer.from('arbitrary content behind a dag-pb cid')
    }))
  })

  // ---- Qm (CIDv0) happy path -----------------------------------------------

  it('succeeds when the Qm hash matches the downloaded content (inline verification)', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `qm-good-${Date.now()}`)

    await saveContentFileToDisk(
      { metrics, storage },
      `${baseUrl}/contents/${qmHash}`,
      filename,
      qmHash
    )

    expect(await storage.exist(qmHash)).toBe(true)
  })

  // ---- Qm (CIDv0) failure: corrupt content caught inline -------------------

  it('fails with "hashes do not match" when Qm content is corrupt (caught inline)', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `qm-corrupt-${Date.now()}`)

    await expect(
      saveContentFileToDisk(
        { metrics, storage },
        `${baseUrl}/contents/corrupt`,
        filename,
        qmHash // expects the good hash, but server returns corrupt bytes
      )
    ).rejects.toThrow('hashes do not match')
  })

  it('does not store a file when the Qm hash check fails', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `qm-norestore-${Date.now()}`)

    try {
      await saveContentFileToDisk(
        { metrics, storage },
        `${baseUrl}/contents/corrupt`,
        filename,
        qmHash
      )
    } catch {}

    expect(await storage.exist(qmHash)).toBe(false)
  })

  // ---- ba (CIDv1 raw) happy path -------------------------------------------

  it('succeeds when the ba hash matches the downloaded content (inline verification)', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `ba-good-${Date.now()}`)

    await saveContentFileToDisk(
      { metrics, storage },
      `${baseUrl}/contents/${baHash}`,
      filename,
      baHash
    )

    expect(await storage.exist(baHash)).toBe(true)
  })

  // ---- ba (CIDv1 raw) failure: corrupt content ----------------------------

  it('fails with "hashes do not match" when ba content is corrupt', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `ba-corrupt-${Date.now()}`)

    await expect(
      saveContentFileToDisk(
        { metrics, storage },
        `${baseUrl}/contents/corrupt`,
        filename,
        baHash
      )
    ).rejects.toThrow('hashes do not match')
  })

  // ---- DAG-PB CIDv1 fallback path ------------------------------------------

  it('falls back to assertHash for a DAG-PB CIDv1 (verifyHashFromInlineDigest returns null)', async () => {
    // verifyHashFromInlineDigest returns null for codec 0x70 CIDs because the SHA-256 of the
    // raw bytes is NOT the DAG-PB root hash.  The pipeline must fall back to assertHash, which
    // will also fail here because the content does not match this CID.
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `dagpb-${Date.now()}`)

    await expect(
      saveContentFileToDisk(
        { metrics, storage },
        `${baseUrl}/contents/dagpb`,
        filename,
        DAG_PB_CID_V1
      )
    ).rejects.toThrow('hashes do not match')
  })

  it('does not store a file when the DAG-PB fallback assertHash fails', async () => {
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `dagpb-norestore-${Date.now()}`)

    try {
      await saveContentFileToDisk(
        { metrics, storage },
        `${baseUrl}/contents/dagpb`,
        filename,
        DAG_PB_CID_V1
      )
    } catch {}

    expect(await storage.exist(DAG_PB_CID_V1)).toBe(false)
  })

  // ---- checkHash=false: no verification at all ----------------------------

  it('succeeds without verifying when checkHash is false', async () => {
    // checkHash=false bypasses both the inline hasher and assertHash.
    // The corrupt bytes are stored without complaint.
    const storage = createInMemoryStorage()
    const filename = resolve(contentFolder, `nohash-${Date.now()}`)

    await saveContentFileToDisk(
      { metrics, storage },
      `${baseUrl}/contents/corrupt`,
      filename,
      qmHash,
      false // skip all hash verification
    )

    // The file is stored (though its bytes are wrong) — verifying that `false` really skips checks.
    expect(await storage.exist(qmHash)).toBe(true)
  })
})

import * as zlib from 'zlib'
import { Readable } from 'stream'
import future from 'fp-future'
import { hashV1 } from '@dcl/hashing'
import { downloadFileWithRetries } from '../src/downloader'
import { sleep } from '../src/utils'
import { test } from './components'

test('downloadFileWithRetries when a server declares its content-encoding in mixed case', ({ components }) => {
  const targetFolder = 'downloads'
  let payload: Buffer
  let hash: string
  // The route is registered exactly once and reads these. Registering it per-describe does not work: the
  // router accumulates handlers within one suite and the first one registered wins, so every case would
  // have been served by whichever describe ran first — and would have passed for that reason rather than
  // for the behaviour under test.
  let servedEncoding: string
  let servedBody: Buffer

  beforeEach(async () => {
    payload = Buffer.from('the-file-contents-that-must-survive-a-round-trip')
    hash = await hashV1(payload)
    await components.storage.delete([hash])
    servedEncoding = 'gzip'
    servedBody = zlib.gzipSync(payload)
    components.router.get('/contents/:file', async () => ({
      headers: { 'content-encoding': servedEncoding },
      body: servedBody
    }))
  })

  describe.each([
    ['GZip', 'GZip'],
    ['GZIP', 'GZIP'],
    ['gzip with surrounding whitespace', ' gzip '],
    // Legacy alias, still emitted by some servers.
    ['x-gzip', 'x-gzip']
  ])('and the header value is %s', (_label: string, headerValue: string) => {
    beforeEach(() => {
      servedEncoding = headerValue
      servedBody = zlib.gzipSync(payload)
    })

    // Matching the exact string 'gzip' wrote these to disk still compressed, so they failed hash
    // verification and burned the whole retry ladder against a server that was behaving correctly.
    it('should decompress it and store bytes matching the hash', async () => {
      await downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)

      await expect(components.storage.exist(hash)).resolves.toBe(true)
    })
  })

  describe('and the header declares identity', () => {
    beforeEach(() => {
      servedEncoding = 'identity'
      servedBody = payload
    })

    it('should treat it as no coding applied rather than as one to undo', async () => {
      await downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)

      await expect(components.storage.exist(hash)).resolves.toBe(true)
    })
  })

  describe.each([
    ['a coding this client cannot undo', 'br'],
    ['several codings layered together', 'gzip, br']
  ])('and the header declares %s', (_label: string, headerValue: string) => {
    beforeEach(() => {
      servedEncoding = headerValue
      servedBody = payload
    })

    // Previously these were written to disk undecoded and surfaced as a hash mismatch once the retries
    // were spent, which says nothing about the real cause.
    it('should fail naming the unsupported coding rather than as a hash mismatch', async () => {
      await expect(
        downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)
      ).rejects.toThrow('unsupported content-encoding')
    })
  })
})

test('downloadFileWithRetries when a second caller joins an in-flight download', ({ components }) => {
  const targetFolder = 'downloads'
  let payload: Buffer
  let hash: string
  let requestCount: number
  let firstRequestStarted: ReturnType<typeof future<void>>
  let releaseResponse: ReturnType<typeof future<void>>
  let outcome: unknown[]

  beforeEach(async () => {
    payload = Buffer.from('x'.repeat(4096))
    hash = await hashV1(payload)
    await components.storage.delete([hash])
    requestCount = 0
    firstRequestStarted = future<void>()
    releaseResponse = future<void>()

    components.router.get('/contents/:file', async () => {
      requestCount++
      firstRequestStarted.resolve()
      // Held open until both callers have been started, so the second one genuinely arrives while the
      // first transfer is in flight rather than after it has finished.
      await releaseResponse
      return { body: payload }
    })

    const generous = downloadFileWithRetries(
      components,
      hash,
      targetFolder,
      [await components.getBaseUrl()],
      new Map(),
      1,
      0,
      undefined,
      { maxDownloadedFileSizeInBytes: 1024 * 1024 }
    )

    // The barrier is the handler being entered, not a sleep: the request cannot happen before the first
    // caller has registered its in-flight job, so there is no timing left to guess at.
    await firstRequestStarted

    const stricter = downloadFileWithRetries(
      components,
      hash,
      targetFolder,
      [await components.getBaseUrl()],
      new Map(),
      1,
      0,
      undefined,
      // Would have refused the 4 KiB payload had it run its own transfer.
      { maxDownloadedFileSizeInBytes: 8 }
    )

    releaseResponse.resolve()
    outcome = await Promise.all([generous, stricter])
  })

  // Documents the de-duplication contract: the key is the hash alone, so a joiner inherits the bounds of
  // whoever started the transfer. A stricter second caller does not get its own transfer — the hash is a
  // content address, so a separate download would spend real bandwidth reaching a byte-identical result.
  it('should satisfy both callers rather than failing the stricter one on its own cap', () => {
    expect(outcome).toHaveLength(2)
  })

  it('should have made a single request for the two callers', () => {
    expect(requestCount).toEqual(1)
  })
})

test('downloadFileWithRetries when an unsupported-encoding response keeps streaming', ({ components }) => {
  const targetFolder = 'downloads'
  let hash: string
  let bytesPulledFromBody: number
  let bodyClosed: ReturnType<typeof future<void>>

  beforeEach(async () => {
    hash = await hashV1(Buffer.from('irrelevant, this response is never usable'))
    await components.storage.delete([hash])
    bytesPulledFromBody = 0
    bodyClosed = future<void>()

    // Declares a coding this client cannot undo, then streams without end. The generator counts what is
    // actually pulled from it, which is the observable difference between abandoning the body and draining
    // it: resume() would keep consuming this in the background long after the promise rejected.
    components.router.get('/contents/:file', async () => {
      const endless = new Readable({
        read() {
          bytesPulledFromBody += 1024
          this.push(Buffer.alloc(1024, 1))
        }
      })
      endless.on('close', () => bodyClosed.resolve())
      return { headers: { 'content-encoding': 'br' }, body: endless }
    })
  })

  it('should reject naming the unsupported coding', async () => {
    await expect(
      downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)
    ).rejects.toThrow('unsupported content-encoding')
  })

  it('should close the response body rather than leave it streaming', async () => {
    await expect(
      downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)
    ).rejects.toThrow()

    await expect(bodyClosed).resolves.toBeUndefined()
  })

  it('should stop pulling bytes from it once the download has been rejected', async () => {
    await expect(
      downloadFileWithRetries(components, hash, targetFolder, [await components.getBaseUrl()], new Map(), 1, 0)
    ).rejects.toThrow()

    const pulledWhenRejected = bytesPulledFromBody
    await sleep(300)

    // Draining leaves this climbing after the caller has moved on; abandoning it does not.
    expect(bytesPulledFromBody).toEqual(pulledWhenRejected)
  })
})

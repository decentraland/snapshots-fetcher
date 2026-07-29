import * as zlib from 'zlib'
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

  beforeEach(async () => {
    payload = Buffer.from('x'.repeat(4096))
    hash = await hashV1(payload)
    await components.storage.delete([hash])
    requestCount = 0
    components.router.get('/contents/:file', async () => {
      requestCount++
      await sleep(150)
      return { body: payload }
    })
  })

  // Documents the de-duplication contract: the key is the hash alone, so a joiner inherits the bounds of
  // whoever started the transfer. A stricter second caller does not get its own transfer — the hash is a
  // content address, so a separate download would spend real bandwidth reaching a byte-identical result.
  it('should serve both callers from one transfer, on the first caller"s limits', async () => {
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

    // Long enough for the first call to clear its storage check and register the in-flight job.
    await sleep(30)

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

    await expect(Promise.all([generous, stricter])).resolves.toBeDefined()
    expect(requestCount).toEqual(1)
  })
})

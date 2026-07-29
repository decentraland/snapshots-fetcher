import { Readable } from 'stream'
import { IFetchComponent } from '@dcl/core-commons'
import { hashV1 } from '@dcl/hashing'
import { downloadEntityAndContentFiles } from '../src'
import { DEFAULT_TRANSFER_LIMITS, fetchJson, resolveTransferLimits, tooSlowToContinue } from '../src/utils'
import { test } from './components'

describe('resolveTransferLimits', () => {
  describe('when nothing is supplied', () => {
    it('should return the values this package used before they were configurable', () => {
      expect(resolveTransferLimits()).toEqual({
        requestTimeoutInMs: 15_000,
        maxDownloadedFileSizeInBytes: 1024 * 1024 * 1024,
        minTransferRateInBytesPerSecond: 4096,
        transferRateGracePeriodInMs: 60_000
      })
    })
  })

  describe('when only one field is supplied', () => {
    it('should keep the defaults for every other field', () => {
      expect(resolveTransferLimits({ minTransferRateInBytesPerSecond: 512 })).toEqual({
        ...DEFAULT_TRANSFER_LIMITS,
        minTransferRateInBytesPerSecond: 512
      })
    })
  })

  describe.each([
    ['requestTimeoutInMs', 0, 1],
    ['requestTimeoutInMs', -1, 1],
    ['maxDownloadedFileSizeInBytes', 0, 1],
    ['minTransferRateInBytesPerSecond', -1, 0],
    ['transferRateGracePeriodInMs', -1, 0]
  ])('when %s is given %s', (field: string, value: number, minimum: number) => {
    it('should throw naming the field and the minimum it accepts', () => {
      expect(() => resolveTransferLimits({ [field]: value } as any)).toThrow(
        `transferLimits.${field} must be an integer >= ${minimum}, got ${value}`
      )
    })
  })

  describe.each([
    ['a fraction', 1.5],
    ['not a number', Number.NaN],
    ['infinite', Number.POSITIVE_INFINITY]
  ])('when a limit is %s', (_label: string, value: number) => {
    it('should throw rather than coerce it into a bound that cannot hold', () => {
      expect(() => resolveTransferLimits({ requestTimeoutInMs: value })).toThrow('must be an integer')
    })
  })

  describe('when the rate floor is set to zero', () => {
    it('should disable the check, so an arbitrarily slow transfer continues', () => {
      const limits = resolveTransferLimits({ minTransferRateInBytesPerSecond: 0 })
      const realNow = Date.now()
      const spy = jest.spyOn(Date, 'now').mockReturnValue(realNow + 86_400_000)

      try {
        // A full day elapsed with one byte transferred: refused at any non-zero floor.
        expect(tooSlowToContinue(1, realNow, limits)).toBeUndefined()
      } finally {
        spy.mockRestore()
      }
    })
  })
})

describe('fetchJson', () => {
  function fetcherReturning(payload: string): IFetchComponent {
    return {
      fetch: async () =>
        new Response(Readable.toWeb(Readable.from([Buffer.from(payload)])) as ReadableStream, { status: 200 })
    } as any
  }

  describe('when a caller raises the rate floor above what the peer delivers', () => {
    let clockSpy: jest.SpyInstance
    let error: Error | undefined

    beforeEach(async () => {
      const realNow = Date.now()
      let calls = 0
      clockSpy = jest.spyOn(Date, 'now').mockImplementation(() => realNow + calls++ * 2_000)

      error = undefined
      try {
        // A 1s grace period and a 1 MiB/s floor: this small body could never satisfy it, which is only
        // observable if the supplied limits actually reach the body reader.
        await fetchJson('http://localhost/unused', fetcherReturning('{"ok":true}'), {
          transferLimits: { minTransferRateInBytesPerSecond: 1024 * 1024, transferRateGracePeriodInMs: 1_000 }
        })
      } catch (thrown: any) {
        error = thrown
      }
    })

    afterEach(() => {
      clockSpy.mockRestore()
    })

    it('should reject using the supplied floor rather than the default', () => {
      expect(error?.message).toContain(`below the minimum of ${1024 * 1024} bytes/s`)
    })
  })

  describe('when the default floor would have rejected but the caller disabled it', () => {
    let clockSpy: jest.SpyInstance

    beforeEach(() => {
      const realNow = Date.now()
      let calls = 0
      clockSpy = jest.spyOn(Date, 'now').mockImplementation(() => realNow + calls++ * 70_000)
    })

    afterEach(() => {
      clockSpy.mockRestore()
    })

    it('should parse the body instead', async () => {
      await expect(
        fetchJson('http://localhost/unused', fetcherReturning('{"ok":true}'), {
          transferLimits: { minTransferRateInBytesPerSecond: 0 }
        })
      ).resolves.toEqual({ ok: true })
    })
  })
})

test('downloadEntityAndContentFiles when the caller tightens the download size cap', ({ components }) => {
  let entityId: string

  beforeEach(async () => {
    const entity = { type: 'profile', metadata: { avatars: [] }, content: [] }
    const bytes = Buffer.from(JSON.stringify(entity))
    entityId = await hashV1(bytes)
    // Serves the real entity bytes, so the download passes hash verification and the only thing that can
    // fail it is a transfer bound. Junk bytes would fail on the hash instead and prove nothing.
    components.router.get('/contents/:file', async () => ({ body: bytes.toString() }))
  })

  // The point of this one is the plumbing, not the policy: the cap has to survive five levels of call
  // chain (downloadEntityAndContentFiles -> downloadFileWithRetries -> downloadJob ->
  // saveContentFileToDisk -> downloadFile) to reach the stream, and every parameter along the way is
  // optional — so a wiring that type-checks can still silently fall back to the default at any hop.
  it('should enforce the configured cap rather than the 1 GiB default', async () => {
    await components.storage.delete([entityId])

    await expect(
      downloadEntityAndContentFiles(
        components,
        entityId,
        [await components.getBaseUrl()],
        new Map(),
        'downloads',
        1,
        0,
        1,
        { maxDownloadedFileSizeInBytes: 8 }
      )
    ).rejects.toThrow('exceeds the maximum allowed size of 8 bytes')
  })

  it('should use the 1 GiB default when no limits are supplied', async () => {
    await components.storage.delete([entityId])

    await expect(
      downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), 'downloads', 1, 0)
    ).resolves.toBeDefined()
  })
})

import { Readable } from 'stream'
import { IFetchComponent } from '@dcl/core-commons'
import { hashV1 } from '@dcl/hashing'
import { downloadEntityAndContentFiles } from '../src'
import {
  DEFAULT_TRANSFER_LIMITS,
  fetchJson,
  resolveTransferLimits,
  saveContentFileToDisk,
  tooSlowToContinue
} from '../src/utils'
import { test } from './components'

describe('resolveTransferLimits', () => {
  describe('when nothing is supplied', () => {
    it('should return the values this package used before they were configurable', () => {
      expect(resolveTransferLimits()).toEqual({
        requestTimeoutInMs: 15_000,
        downloadInactivityTimeoutInMs: 30_000,
        maxDownloadedFileSizeInBytes: 1024 * 1024 * 1024,
        minTransferRateInBytesPerSecond: 4096,
        transferRateGracePeriodInMs: 60_000,
        maxPagesPerPaginatedCall: 10_000
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
    ['transferRateGracePeriodInMs', -1, 0],
    ['maxPagesPerPaginatedCall', 0, 1]
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

  describe('when a field is present but explicitly undefined', () => {
    // Config assembled from env vars or optional options carries explicit undefined for an absent value
    // far more often than it omits the key, and a plain spread would overwrite the default with it and
    // then fail validation.
    it('should treat it as omitted rather than as an invalid value', () => {
      expect(resolveTransferLimits({ requestTimeoutInMs: undefined })).toEqual(DEFAULT_TRANSFER_LIMITS)
    })

    it('should still apply the other fields supplied alongside it', () => {
      expect(
        resolveTransferLimits({ requestTimeoutInMs: undefined, minTransferRateInBytesPerSecond: 512 })
      ).toEqual({ ...DEFAULT_TRANSFER_LIMITS, minTransferRateInBytesPerSecond: 512 })
    })
  })

  describe('when every field is explicitly undefined', () => {
    it('should return the defaults untouched', () => {
      expect(
        resolveTransferLimits({
          requestTimeoutInMs: undefined,
          downloadInactivityTimeoutInMs: undefined,
          maxDownloadedFileSizeInBytes: undefined,
          minTransferRateInBytesPerSecond: undefined,
          transferRateGracePeriodInMs: undefined,
          maxPagesPerPaginatedCall: undefined
        })
      ).toEqual(DEFAULT_TRANSFER_LIMITS)
    })
  })

  describe('when a limit name is misspelled', () => {
    // The failure mode this closes is specifically undetectable: the caller asked for a stricter bound,
    // silently got the permissive default, and nothing in the logs or the result said so.
    it('should reject rather than silently use the default for the limit they meant', () => {
      expect(() => resolveTransferLimits({ minTransferRateInBytesPerSec: 512 } as any)).toThrow(
        'transferLimits.minTransferRateInBytesPerSec is not a known limit'
      )
    })

    it('should list the names it does accept', () => {
      expect(() => resolveTransferLimits({ nonsense: 1 } as any)).toThrow(
        'Valid names: downloadInactivityTimeoutInMs, maxDownloadedFileSizeInBytes, maxPagesPerPaginatedCall, minTransferRateInBytesPerSecond, requestTimeoutInMs, transferRateGracePeriodInMs'
      )
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
  // Held at suite scope, not declared inside the beforeEach: the router accumulates handlers across
  // beforeEach runs and the first one registered wins, so a handler closing over a per-run binding would
  // serve the first test's bytes to every later test. Reading a suite-scoped binding keeps every
  // registration equivalent.
  let entityBytes: Buffer

  beforeEach(async () => {
    const entity = { type: 'profile', metadata: { avatars: [] }, content: [] }
    entityBytes = Buffer.from(JSON.stringify(entity))
    entityId = await hashV1(entityBytes)
    // Serves the real entity bytes, so the download passes hash verification and the only thing that can
    // fail it is a transfer bound. Junk bytes would fail on the hash instead and prove nothing.
    components.router.get('/contents/:file', async () => ({ body: entityBytes.toString() }))
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

  it('should fall back to the 1 GiB default with no limits supplied', async () => {
    await components.storage.delete([entityId])

    await expect(
      downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), 'downloads', 1, 0)
    ).resolves.toBeDefined()
  })
})

test('saveContentFileToDisk when a peer accepts the connection and then goes silent', ({ components }) => {
  let baseUrl: string

  beforeEach(async () => {
    baseUrl = await components.getBaseUrl()
    // Headers never sent, body never written: the socket simply idles. Only the inactivity deadline can
    // end this, so the wait is a direct measure of the configured value being used.
    components.router.get('/silent/:file', async () => new Promise(() => {}) as any)
  })

  it('should abort after the configured deadline rather than the hardcoded 30s', async () => {
    const startedAt = Date.now()

    await expect(
      saveContentFileToDisk(
        components,
        `${baseUrl}/silent/QmTestHash`,
        'downloads/QmTestHash',
        'QmTestHash',
        false,
        { downloadInactivityTimeoutInMs: 250 }
      )
    ).rejects.toThrow('Timeout while downloading')

    // Comfortably under the 30s default, so this cannot pass while the option is ignored.
    expect(Date.now() - startedAt).toBeLessThan(5_000)
  }, 40_000)
})

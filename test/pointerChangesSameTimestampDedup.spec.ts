import { AuthChain, AuthLinkType, PointerChangesSyncDeployment } from '@dcl/schemas'
import { boundaryRowFingerprint, getDeployedEntitiesStreamFromPointerChanges } from '../src/stream-entities'
import { test } from './components'

const authChain: AuthChain = [{ type: AuthLinkType.SIGNER, payload: '0x1', signature: '' }]

function idOf(suffix: string): string {
  return `ba${suffix.padStart(57, '0')}`
}

// Typed as the schema the stream validates against, so a fixture that drifts out of shape fails to compile
// rather than being silently discarded before de-duplication is ever reached — which is exactly how the
// first version of this spec failed.
function delta(suffix: string, localTimestamp: number): PointerChangesSyncDeployment {
  return {
    entityType: 'profile',
    entityId: idOf(suffix),
    entityTimestamp: localTimestamp,
    localTimestamp,
    authChain,
    pointers: ['0x1']
  }
}

// Same entity id and timestamp, different signer. entityId is the hash of the entity file and does not
// cover the authChain, so these are two distinct rows that no id-based key can tell apart.
function deltaSignedBy(suffix: string, localTimestamp: number, signer: string): PointerChangesSyncDeployment {
  return {
    ...delta(suffix, localTimestamp),
    authChain: [{ type: AuthLinkType.SIGNER, payload: signer, signature: '' }]
  }
}

type PointerChangesPage = {
  deltas: PointerChangesSyncDeployment[]
  pagination: { next?: string }
}

test('getDeployedEntitiesStreamFromPointerChanges when rows repeat an entity at one timestamp', ({ components }) => {
  let served: PointerChangesSyncDeployment[][]
  let pollsServed: number

  beforeEach(() => {
    pollsServed = 0
    served = []
    components.router.get('/pointer-changes', async () => {
      const body: PointerChangesPage = { deltas: served[pollsServed] ?? [], pagination: {} }
      pollsServed++
      return { body }
    })
  })

  async function streamUntil(stopAfterPolls: number): Promise<string[]> {
    const streamed: string[] = []
    for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
      components,
      { pointerChangesWaitTime: 1, fromTimestamp: 0 },
      await components.getBaseUrl(),
      // Consulted inside the delta loop too, so stopping at N ends the stream before poll N's rows are
      // consumed; every case here stops one poll later than the last one it cares about.
      () => pollsServed >= stopAfterPolls
    )) {
      streamed.push(deployment.entityId)
    }
    return streamed
  }

  describe('and one response carries two rows for the same entity at the same timestamp', () => {
    beforeEach(() => {
      // A suppression set that grew as the poll ran would treat the second row as a re-yield of the first.
      served = [[delta('1', 1000), delta('1', 1000)]]
    })

    it('should yield both rather than collapsing the second', async () => {
      await expect(streamUntil(2)).resolves.toEqual([idOf('1'), idOf('1')])
    })
  })

  describe('and a later poll re-returns the boundary row because `from` is inclusive', () => {
    beforeEach(() => {
      served = [[delta('1', 1000)], [delta('1', 1000), delta('2', 1000)]]
    })

    it('should skip the re-returned row but still yield the new entity at that timestamp', async () => {
      await expect(streamUntil(3)).resolves.toEqual([idOf('1'), idOf('2')])
    })
  })

  describe('and a later poll reveals an extra row for an entity already delivered at that timestamp', () => {
    beforeEach(() => {
      // Poll 1 delivers one row at t=1000. Poll 2 returns two rows for the same entity at the same
      // timestamp: the one already delivered, plus one the server had not shown before. Membership cannot
      // tell "seen once" from "seen twice", so a Set suppressed both and the extra row was lost.
      served = [[delta('1', 1000)], [delta('1', 1000), delta('1', 1000)]]
    })

    it('should suppress only the row it already delivered and yield the extra one', async () => {
      await expect(streamUntil(3)).resolves.toEqual([idOf('1'), idOf('1')])
    })
  })

  describe('and a later poll re-returns exactly what was already delivered', () => {
    beforeEach(() => {
      served = [
        [delta('1', 1000), delta('1', 1000)],
        [delta('1', 1000), delta('1', 1000)]
      ]
    })

    it('should yield nothing the second time, spending one allowance per delivered row', async () => {
      await expect(streamUntil(3)).resolves.toEqual([idOf('1'), idOf('1')])
    })
  })

  describe('and the later poll returns a new same-entity row before replaying the delivered one', () => {
    beforeEach(() => {
      // Poll 1 delivers the row signed by 0xA. Poll 2 returns the *new* row (0xB) first and the replay of
      // 0xA second — nothing guarantees a server replays rows in the order it first sent them. Keyed by
      // entity id, the new row spends 0xA's allowance and is suppressed, and the replay is then yielded:
      // the stream keeps its high-water timestamp and silently loses a legitimate pointer-change.
      served = [[deltaSignedBy('1', 1000, '0xA')], [deltaSignedBy('1', 1000, '0xB'), deltaSignedBy('1', 1000, '0xA')]]
    })

    it('should yield the new row rather than the replayed one', async () => {
      const signers: string[] = []
      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 1, fromTimestamp: 0 },
        await components.getBaseUrl(),
        () => pollsServed >= 3
      )) {
        signers.push(deployment.authChain[0].payload)
      }

      // Asserted on the signer, not the entity id: both rows share an id, so an id-only assertion cannot
      // tell the new row from the replay and would pass either way.
      expect(signers).toEqual(['0xA', '0xB'])
    })
  })

  describe('and one timestamp contains more distinct rows than the boundary tracker can retain', () => {
    let fetchSpy: jest.SpyInstance

    beforeEach(() => {
      served = [Array.from({ length: 10_001 }, (_, index) => delta(String(index), 1000))]
      fetchSpy = jest.spyOn(components.fetcher, 'fetch').mockImplementation(async () => {
        const body: PointerChangesPage = { deltas: served[pollsServed] ?? [], pagination: {} }
        pollsServed++
        return new Response(JSON.stringify(body))
      })
    })

    afterEach(() => {
      fetchSpy.mockRestore()
    })

    it('should fail the poll before yielding an untrackable row', async () => {
      await expect(streamUntil(2)).rejects.toThrow('maximum boundary rows tracked per poll is 10000')
    })
  })

  describe('and a schema-valid row has a pointer larger than the local structural limit', () => {
    let fetchSpy: jest.SpyInstance

    beforeEach(() => {
      served = [
        [
          {
            ...delta('1', 1000),
            pointers: ['x'.repeat(256 * 1024 + 1)]
          }
        ]
      ]
      fetchSpy = jest.spyOn(components.fetcher, 'fetch').mockImplementation(async () => {
        const body: PointerChangesPage = { deltas: served[pollsServed] ?? [], pagination: {} }
        pollsServed++
        return new Response(JSON.stringify(body))
      })
    })

    afterEach(() => {
      fetchSpy.mockRestore()
    })

    it('should fail the poll instead of sorting and hashing the attacker-sized field', async () => {
      await expect(streamUntil(2)).rejects.toThrow('pointer is 262145 bytes')
    })
  })
})

test('boundaryRowFingerprint', () => {
  describe('when a row exceeds the local fingerprint-material limit', () => {
    let deployment: PointerChangesSyncDeployment

    beforeEach(() => {
      const largeRemoteValue = 'x'.repeat(1024 * 1024)
      deployment = {
        ...deltaSignedBy('1', 1000, largeRemoteValue),
        pointers: [largeRemoteValue],
        authChain: [
          {
            type: AuthLinkType.SIGNER,
            payload: largeRemoteValue,
            signature: largeRemoteValue
          }
        ]
      }
    })

    afterEach(() => {
      deployment = undefined as any
    })

    it('should reject it before cloning or sorting its pointers', () => {
      expect(() => boundaryRowFingerprint(deployment)).toThrow('above the maximum')
    })
  })

  describe('when a row stays within every structural limit', () => {
    let fingerprint: string

    beforeEach(() => {
      fingerprint = boundaryRowFingerprint(deltaSignedBy('1', 1000, 'x'.repeat(128 * 1024)))
    })

    afterEach(() => {
      fingerprint = ''
    })

    it('should reduce the row to one fixed-size SHA-256 digest', () => {
      expect(fingerprint).toMatch(/^[A-Za-z0-9_-]{43}$/)
    })
  })

  describe('when equivalent rows list their pointers in a different order', () => {
    let firstFingerprint: string
    let reorderedFingerprint: string

    beforeEach(() => {
      const deployment: PointerChangesSyncDeployment = {
        ...delta('1', 1000),
        pointers: ['pointer-b', 'pointer-a']
      }
      firstFingerprint = boundaryRowFingerprint(deployment)
      reorderedFingerprint = boundaryRowFingerprint({
        ...deployment,
        pointers: ['pointer-a', 'pointer-b']
      })
    })

    afterEach(() => {
      firstFingerprint = ''
      reorderedFingerprint = ''
    })

    it('should produce the same digest', () => {
      expect(reorderedFingerprint).toBe(firstFingerprint)
    })
  })

  describe('when rows differ by an auth-chain signer', () => {
    let firstFingerprint: string
    let secondFingerprint: string

    beforeEach(() => {
      firstFingerprint = boundaryRowFingerprint(deltaSignedBy('1', 1000, '0xA'))
      secondFingerprint = boundaryRowFingerprint(deltaSignedBy('1', 1000, '0xB'))
    })

    afterEach(() => {
      firstFingerprint = ''
      secondFingerprint = ''
    })

    it('should produce different digests so a new row is not suppressed as a replay', () => {
      expect(secondFingerprint).not.toBe(firstFingerprint)
    })
  })
})

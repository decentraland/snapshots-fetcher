import { getDeployedEntitiesStreamFromPointerChanges } from '../src/stream-entities'
import { test } from './components'

const authChain = [{ type: 'SIGNER', payload: '0x1', signature: '' }]

// entityId must be a real content-hash shape and entityTimestamp is required, or the delta is discarded
// by PointerChangesSyncDeployment.validate before any de-duplication is reached.
function delta(suffix: string, localTimestamp: number) {
  return {
    entityType: 'profile',
    entityId: `ba${suffix.padStart(57, '0')}`,
    entityTimestamp: localTimestamp,
    localTimestamp,
    authChain,
    pointers: ['0x1']
  }
}

function idOf(suffix: string): string {
  return `ba${suffix.padStart(57, '0')}`
}

test('getDeployedEntitiesStreamFromPointerChanges when one response repeats an entity at the same timestamp', ({
  components
}) => {
  let served: any[][]
  let pollsServed: number

  beforeEach(() => {
    pollsServed = 0
    served = []
    components.router.get('/pointer-changes', async (): Promise<any> => {
      const body = { deltas: served[pollsServed] ?? [], pagination: {} }
      pollsServed++
      return { body }
    })
  })

  describe('and both rows are legitimate deltas for the same entity', () => {
    beforeEach(() => {
      // Two rows, same entityId, same localTimestamp, inside one response. A dedup set that grew as the
      // poll ran would treat the second as a re-yield of the first and drop it.
      served = [[delta('1', 1000), delta('1', 1000)]]
    })

    it('should yield both rather than collapsing the second', async () => {
      const streamed: string[] = []
      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 0, fromTimestamp: 0 },
        await components.getBaseUrl()
      )) {
        streamed.push(deployment.entityId)
      }

      expect(streamed).toEqual([idOf('1'), idOf('1')])
    })
  })

  describe('and a later poll re-returns the boundary row because `from` is inclusive', () => {
    beforeEach(() => {
      // Poll 1 delivers E1@1000. Poll 2 starts from 1000 inclusive and gets it again, plus a new entity
      // at the same timestamp that has genuinely never been delivered.
      served = [[delta('1', 1000)], [delta('1', 1000), delta('2', 1000)]]
    })

    it('should skip the re-returned row but still yield the new one', async () => {
      const streamed: string[] = []
      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 1, fromTimestamp: 0 },
        await components.getBaseUrl(),
        // Stops on the third poll, not the second: the predicate is consulted inside the delta loop too, so
        // stopping at 2 would end the stream before poll 2's rows were consumed.
        () => pollsServed >= 3
      )) {
        streamed.push(deployment.entityId)
      }

      expect(streamed).toEqual([idOf('1'), idOf('2')])
    })
  })
})

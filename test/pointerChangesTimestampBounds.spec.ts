import { AuthLinkType } from '@dcl/schemas'
import { getDeployedEntitiesStreamFromPointerChanges } from '../src/stream-entities'
import { test } from './components'

const authChain = [{ type: AuthLinkType.SIGNER, payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5', signature: '' }]

function delta(entityId: string, localTimestamp: number, entityTimestamp = 1) {
  return { entityType: 'profile', entityId, entityTimestamp, localTimestamp, authChain, pointers: ['0x1'] }
}

test('getDeployedEntitiesStreamFromPointerChanges when a delta carries an implausible timestamp', ({
  components
}) => {
  // Every value here is accepted by PointerChangesSyncDeployment.validate: the schema rejects Infinity and
  // negatives but bounds nothing above, and localTimestamp is what becomes the server's high-water mark.
  const farFuture = Date.now() + 400 * 24 * 60 * 60 * 1000
  const usable = delta('ba000000000000000000000000000000000000000000000000000000001', 5)

  it('prepares the endpoints', () => {
    components.router.get('/pointer-changes', async () => ({
      body: {
        deltas: [
          usable,
          delta('ba000000000000000000000000000000000000000000000000000000002', farFuture),
          delta('ba000000000000000000000000000000000000000000000000000000003', 1e308),
          delta('ba000000000000000000000000000000000000000000000000000000004', 9007199254740993),
          delta('ba000000000000000000000000000000000000000000000000000000005', 100.5),
          // entityTimestamp is checked too, since it is remote and compared against the genesis point.
          delta('ba000000000000000000000000000000000000000000000000000000006', 6, farFuture)
        ],
        pagination: {}
      }
    }))
  })

  describe('when the stream is drained', () => {
    let yielded: any[]

    beforeEach(async () => {
      yielded = []
      const stream = getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 0, fromTimestamp: 0 },
        await components.getBaseUrl()
      )
      for await (const deployment of stream) {
        yielded.push(deployment)
      }
    })

    it('should yield only the deployment with a plausible timestamp', () => {
      expect(yielded.map((deployment) => deployment.entityId)).toEqual([usable.entityId])
    })

    it('should never yield a timestamp that would pin the high-water mark out of reach', () => {
      expect(
        yielded.every(
          (deployment) =>
            Number.isSafeInteger(deployment.localTimestamp) && deployment.localTimestamp <= Date.now() + 86_400_000
        )
      ).toBe(true)
    })
  })
})

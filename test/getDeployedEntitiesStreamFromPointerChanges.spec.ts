import { AuthLinkType } from '@dcl/schemas'
import { getDeployedEntitiesStreamFromPointerChanges } from '../src/stream-entities'
import { test } from './components'

test('fetches a stream from pointer-changes after specific timestamp', ({ components, stubComponents }) => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  it('prepares the endpoints', () => {
    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == '10') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000010', entityTimestamp: 10, localTimestamp: 10, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000011', entityTimestamp: 11, localTimestamp: 11, authChain, pointers: ['0x1'] },
            ],
            pagination: {
              next: '?from=11&entityId=ba000000000000000000000000000000000000000000000000000000011',
            },
          },
        }
      }

      if (ctx.url.searchParams.get('from') != '11' && ctx.url.searchParams.get('entityId') != 'ba000000000000000000000000000000000000000000000000000000011') {
        throw new Error('pagination is not working properly')
      }

      return {
        body: {
          deltas: [
            { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000012', entityTimestamp: 12, localTimestamp: 12, authChain, pointers: ['0x1'] },
            { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000013', entityTimestamp: 13, localTimestamp: 13, authChain, pointers: ['0x1'] },
          ],
          pagination: {},
        },
      }
    })
  })

  it('fetches stream', async () => {
    const r = []
    const stream = getDeployedEntitiesStreamFromPointerChanges(
      components,
      {
        pointerChangesWaitTime: 0,
        fromTimestamp: 10
      },
      await components.getBaseUrl())

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000010', entityTimestamp: 10, localTimestamp: 10, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000011', entityTimestamp: 11, localTimestamp: 11, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000012', entityTimestamp: 12, localTimestamp: 12, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000013', entityTimestamp: 13, localTimestamp: 13, authChain, pointers: ['0x1'] },
    ])
  })
})

test('fetches a stream from pointer-changes from 0 if timestamp is not specified', ({ components, stubComponents }) => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  it('prepares the endpoints', () => {
    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == '0') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000000', entityTimestamp: 0, localTimestamp: 0, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, localTimestamp: 1, authChain, pointers: ['0x1'] },
            ],
            pagination: {
              next: '?from=1&entityId=ba000000000000000000000000000000000000000000000000000000001',
            },
          },
        }
      }

      if (ctx.url.searchParams.get('from') != '1' && ctx.url.searchParams.get('entityId') != 'ba000000000000000000000000000000000000000000000000000000001') {
        throw new Error('pagination is not working properly')
      }

      return {
        body: {
          deltas: [
            { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, localTimestamp: 2, authChain, pointers: ['0x1'] },
            { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, localTimestamp: 3, authChain, pointers: ['0x1'] },
          ],
          pagination: {},
        },
      }
    })
  })

  it('fetches stream', async () => {
    const r = []
    const stream = getDeployedEntitiesStreamFromPointerChanges(
      components,
      {
        pointerChangesWaitTime: 0,
      },
      await components.getBaseUrl())

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000000', entityTimestamp: 0, localTimestamp: 0, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, localTimestamp: 1, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, localTimestamp: 2, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, localTimestamp: 3, authChain, pointers: ['0x1'] },
    ])
  })
})

test('does not re-yield boundary deployments across polls', ({ components }) => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]
  const idA = 'ba000000000000000000000000000000000000000000000000000000010'
  const idB = 'ba000000000000000000000000000000000000000000000000000000011'

  it('prepares the endpoints', () => {
    // The poll re-fetches from the high-water timestamp (inclusive), so the deployment at that
    // timestamp comes back every cycle. from=1 re-returns idA (ts 1) alongside the new idB (ts 2).
    components.router.get('/pointer-changes', async (ctx) => {
      const from = ctx.url.searchParams.get('from')
      if (from === '0') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: idA, entityTimestamp: 1, localTimestamp: 1, authChain, pointers: ['0x1'] }
            ],
            pagination: {}
          }
        }
      }
      if (from === '1') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: idA, entityTimestamp: 1, localTimestamp: 1, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: idB, entityTimestamp: 2, localTimestamp: 2, authChain, pointers: ['0x1'] }
            ],
            pagination: {}
          }
        }
      }
      return {
        body: {
          deltas: [
            { entityType: 'profile', entityId: idB, entityTimestamp: 2, localTimestamp: 2, authChain, pointers: ['0x1'] }
          ],
          pagination: {}
        }
      }
    })
  })

  it('yields each deployment at the boundary timestamp only once', async () => {
    const yieldedEntityIds: string[] = []
    const stream = getDeployedEntitiesStreamFromPointerChanges(
      components,
      {
        pointerChangesWaitTime: 20,
        fromTimestamp: 0
      },
      await components.getBaseUrl()
    )

    for await (const deployment of stream) {
      yieldedEntityIds.push(deployment.entityId)
      // stop once the second poll has surfaced the new deployment
      if (deployment.entityId === idB) {
        break
      }
    }

    expect(yieldedEntityIds).toEqual([idA, idB])
  })
})

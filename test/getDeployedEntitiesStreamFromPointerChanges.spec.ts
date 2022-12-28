import { getDeployedEntitiesStreamFromPointerChanges } from '../src'
import { test } from './components'
import { AuthLinkType } from '@dcl/schemas'

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
              { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain, pointers: ['0x1'] },
            ],
            pagination: {
              next: '?from=11&entityId=Qm000011',
            },
          },
        }
      }

      if (ctx.url.searchParams.get('from') != '11' && ctx.url.searchParams.get('entityId') != 'Qm000011') {
        throw new Error('pagination is not working properly')
      }

      return {
        body: {
          deltas: [
            { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain, pointers: ['0x1'] },
            { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain, pointers: ['0x1'] },
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
      { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain, pointers: ['0x1'] },
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
              { entityType: 'profile', entityId: 'Qm000000', localTimestamp: 0, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain, pointers: ['0x1'] },
            ],
            pagination: {
              next: '?from=1&entityId=Qm000001',
            },
          },
        }
      }

      if (ctx.url.searchParams.get('from') != '1' && ctx.url.searchParams.get('entityId') != 'Qm000001') {
        throw new Error('pagination is not working properly')
      }

      return {
        body: {
          deltas: [
            { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain, pointers: ['0x1'] },
            { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain, pointers: ['0x1'] },
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
      { entityType: 'profile', entityId: 'Qm000000', localTimestamp: 0, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain, pointers: ['0x1'] },
    ])
  })
})

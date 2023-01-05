import { AuthLinkType } from '@dcl/schemas'
import { processDeploymentsInFile } from '../src/file-processor'
import { createStorageComponent } from './test-component'
import { test } from './components'
import { ILoggerComponent } from '@well-known-components/interfaces'

test('processor', ({ components, stubComponents }) => {

  describe('processor', () => {
    const authChain = [
      {
        type: AuthLinkType.SIGNER,
        payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
        signature: '',
      },
    ]
    let logger: ILoggerComponent.ILogger

    beforeAll(() => {
      logger = components.logs.getLogger('processor-test-logger')
    })

    it('emits every deployment ignoring empty lines', async () => {
      const r = []
      const stream = processDeploymentsInFile(
        'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
        { storage: await createStorageComponent() },
        logger
      )

      for await (const deployment of stream) {
        r.push(deployment)
      }

      expect(r).toEqual([
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000004', entityTimestamp: 4, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000005', entityTimestamp: 5, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000006', entityTimestamp: 6, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000007', entityTimestamp: 7, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000008', entityTimestamp: 8, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000009', entityTimestamp: 9, authChain, pointers: ['0x1'] },
      ])
    })

    it('fails on unexistent file', async () => {
      await expect(async () => {
        const stream = processDeploymentsInFile(
          'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu' + Math.random(),
          { storage: await createStorageComponent() },
          logger
        )
        for await (const c of stream) {
          // noop
        }
      }).rejects.toThrow('does not exist')
    })
  })
})

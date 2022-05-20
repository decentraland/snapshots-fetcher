import { AuthLinkType } from '@dcl/schemas'
import { processDeploymentsInFile } from '../src/file-processor'
import { createStorageComponent } from './test-component'

describe('processor', () => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  it('emits every deployment ignoring empty lines', async () => {
    const r = []
    const stream = processDeploymentsInFile('bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha', {
      storage: await createStorageComponent(),
    })

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000004', localTimestamp: 4, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000005', localTimestamp: 5, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000006', localTimestamp: 6, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000007', localTimestamp: 7, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000008', localTimestamp: 8, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000009', localTimestamp: 9, authChain, pointers: ['0x1'] },
    ])
  })

  it('fails on unexistent file', async () => {
    await expect(async () => {
      const stream = processDeploymentsInFile(
        'bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha' + Math.random(),
        {
          storage: await createStorageComponent(),
        }
      )
      for await (const c of stream) {
        // noop
      }
    }).rejects.toThrow('does not exist')
  })
})

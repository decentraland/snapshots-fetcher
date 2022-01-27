import { processDeploymentsInFile } from '../src/file-processor'
import { createStorageComponent } from './test-component'

describe('processor', () => {
  it('emits every deployment ignoring empty lines', async () => {
    const r = []
    const stream = processDeploymentsInFile('bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm', {
      storage: await createStorageComponent(),
    })

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000004', localTimestamp: 4, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000005', localTimestamp: 5, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000006', localTimestamp: 6, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000007', localTimestamp: 7, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000008', localTimestamp: 8, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000009', localTimestamp: 9, authChain: [] },
    ])
  })

  it('fails on unexistent file', async () => {
    await expect(async () => {
      const stream = processDeploymentsInFile(
        'bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm' + Math.random(),
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

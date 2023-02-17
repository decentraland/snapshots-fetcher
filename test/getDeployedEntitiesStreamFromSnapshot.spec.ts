import { getDeployedEntitiesStreamFromSnapshot } from '../src/stream-entities'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import Sinon from 'sinon'
import { AuthLinkType } from '@dcl/schemas'

const downloadedSnapshotFile = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'

test('fetches a stream from snapshots deleting the downloaded file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  it('prepares the endpoints', () => {
    // serve the snapshot file
    let downloadAttempts = 0
    components.router.get(`/contents/${downloadedSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetches stream', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const servers = [await components.getBaseUrl()]

    const r = []
    const stream = getDeployedEntitiesStreamFromSnapshot(
      {
        metrics: components.metrics,
        logs: components.logs,
        storage: components.storage
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder
      },
      downloadedSnapshotFile,
      new Set(servers)
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must be deleted
    Sinon.assert.calledOnce(storage.delete)

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000004', entityTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000005', entityTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000006', entityTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000007', entityTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000008', entityTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000009', entityTimestamp: 9, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
    ])
  })
})

test('fetches a stream without deleting the downloaded file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  it('prepares the endpoints', () => {
    // serve the snapshot file
    let downloadAttempts = 0
    components.router.get(`/contents/${downloadedSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetch stream', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const servers = [await components.getBaseUrl()]

    const r = []
    const stream = getDeployedEntitiesStreamFromSnapshot(
      {
        metrics: components.metrics,
        logs: components.logs,
        storage: components.storage
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder,
        deleteSnapshotAfterUsage: false
      },
      downloadedSnapshotFile,
      new Set(servers)
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }


    expect(r).toEqual([
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000004', entityTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000005', entityTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000006', entityTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000007', entityTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000008', entityTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000009', entityTimestamp: 9, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
    ])

    // the downloaded file must not be deleted
    Sinon.assert.callCount(storage.delete, 0)
  })
})

test('does not fetch a stream if fromTimestamp is after the snapshot endTimestamp', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')

  it('prepares the endpoints', () => {
    // serve the snapshot file
    let downloadAttempts = 0
    components.router.get(`/contents/${downloadedSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetch stream', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const servers = [await components.getBaseUrl()]

    const r = []
    const stream = getDeployedEntitiesStreamFromSnapshot(
      {
        metrics: components.metrics,
        logs: components.logs,
        storage: components.storage
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder,
        deleteSnapshotAfterUsage: false,
        fromTimestamp: 10
      },
      downloadedSnapshotFile,
      new Set(servers)
    )

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toHaveLength(0)

  })
})

test('does delete snapshot after usage if its not own snapshot', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')

  it('prepares the endpoints', () => {
    // serve the snapshot file
    let downloadAttempts = 0
    components.router.get(`/contents/${downloadedSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetch stream', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const storageDeleteSpy = jest.spyOn(storage, 'delete')

    const servers = [await components.getBaseUrl()]

    const r = []
    const stream = getDeployedEntitiesStreamFromSnapshot(
      {
        metrics: components.metrics,

        logs: components.logs,
        storage: components.storage
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder,
        deleteSnapshotAfterUsage: true
      },
      downloadedSnapshotFile,
      new Set(servers)
    )

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(storageDeleteSpy).toBeCalledWith([downloadedSnapshotFile])
  })
})

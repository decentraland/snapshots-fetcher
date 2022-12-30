import { createProcessedSnapshotsComponent, getDeployedEntitiesStreamFromSnapshot } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import Sinon from 'sinon'
import { AuthLinkType } from '@dcl/schemas'

test('fetches a stream from snapshots deleting the downloaded file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'
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
        body: createReadStream('test/fixtures/bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'),
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
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        storage: components.storage,
        processedSnapshotStorage: components.processedSnapshotStorage,
        processedSnapshots: createProcessedSnapshotsComponent(components)
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder
      },
      {
        snapshotHash: 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y',
        greatestEndTimestamp: 9,
        replacedSnapshotHashes: [],
        servers: new Set(servers)
      }
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must be deleted
    Sinon.assert.calledOnce(storage.delete)

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000001', entityTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000002', entityTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000003', entityTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000004', entityTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000005', entityTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000006', entityTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000007', entityTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000008', entityTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
      { entityType: 'profile', entityId: 'Qm000009', entityTimestamp: 9, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile, servers },
    ])
  })
})

test('fetches a stream without deleting the downloaded file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'

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
        body: createReadStream('test/fixtures/bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'),
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
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        storage: components.storage,
        processedSnapshotStorage: components.processedSnapshotStorage,
        processedSnapshots: createProcessedSnapshotsComponent(components)
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder,
        deleteSnapshotAfterUsage: false
      },
      {
        snapshotHash: 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y',
        greatestEndTimestamp: 9,
        replacedSnapshotHashes: [],
        servers: new Set(servers)
      }
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must not be deleted
    Sinon.assert.callCount(storage.delete, 0)
  })
})

test('when successfully process a snapshot file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'

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
        body: createReadStream('test/fixtures/bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('should call end of stream for all', async () => {
    const { storage } = stubComponents
    storage.storeStream.callThrough()
    storage.retrieve.callThrough()
    const processedSnapshots = createProcessedSnapshotsComponent(components)
    const endStreamOfSpy = jest.spyOn(processedSnapshots, 'endStreamOf')
    const servers = [await components.getBaseUrl()]
    const r = []
    const stream = getDeployedEntitiesStreamFromSnapshot(
      {
        metrics: components.metrics,
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        storage: components.storage,
        processedSnapshotStorage: components.processedSnapshotStorage,
        processedSnapshots
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder
      },
      {
        snapshotHash: 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y',
        greatestEndTimestamp: 9,
        replacedSnapshotHashes: [],
        servers: new Set(servers)
      }
    )

    for await (const deployment of stream) {
      r.push(deployment)
    }
    expect(endStreamOfSpy).toBeCalledTimes(1)
    expect(endStreamOfSpy).toBeCalledWith(downloadedSnapshotFile, 9)
  })
})

test('does not fetch a stream if fromTimestamp is after the snapshot endTimestamp', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'

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
        body: createReadStream('test/fixtures/bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'),
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
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        storage: components.storage,
        processedSnapshotStorage: components.processedSnapshotStorage,
        processedSnapshots: createProcessedSnapshotsComponent(components)
      },
      {
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        tmpDownloadFolder: contentFolder,
        deleteSnapshotAfterUsage: false,
        fromTimestamp: 10
      },
      {
        snapshotHash: 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y',
        greatestEndTimestamp: 9,
        replacedSnapshotHashes: [],
        servers: new Set(servers)
      }
    )

    for await (const deployment of stream) {
      r.push(deployment)
    }

    expect(r).toHaveLength(0)

  })
})
import { getDeployedEntitiesStream } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import Sinon from 'sinon'
import ms from 'ms'
import { AuthLinkType } from '@dcl/schemas'

test('getDeployedEntitiesStream from /snapshots endpoint', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  const greatestEntityTimestamp = 9

  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshots', async () => ({
      body: [{
        hash: downloadedSnapshotFile,
        timeRange: {
          initTimestamp: 0, endTimestamp: 8
        }
      }],
    }))

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

    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == (greatestEntityTimestamp - 20 * 60_000).toString()) {
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

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetches a stream', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
      }
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must be deleted
    Sinon.assert.calledOnce(storage.delete)

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000001', entityTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000002', entityTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000003', entityTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000004', entityTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000005', entityTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000006', entityTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000007', entityTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000008', entityTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000009', entityTimestamp: greatestEntityTimestamp, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain, pointers: ['0x1'] },
    ])
  })
})

test('fetches a stream without deleting the downloaded file', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]
  const greatestEntityTimestamp = 9

  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshots', async () => ({
      body: [{
        hash: downloadedSnapshotFile,
        timeRange: {
          initTimestamp: 0, endTimestamp: 8
        }
      }],
    }))

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

    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == (greatestEntityTimestamp - 20 * 60_000).toString()) {
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

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetches a stream without deleting the downloaded file', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        deleteSnapshotAfterUsage: false,
      }
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must not be deleted
    Sinon.assert.callCount(storage.delete, 0)

    expect(r.length).toBeGreaterThan(0)
  })
})

test('when successfully process all snapshot files', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'
  const anotherSnapshotFile = 'bafkreig6sfhegnp4okzecgx3v6gj6pohh5qzw6zjtrdqtggx64743rkmz4'

  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshots', async () => ({
      body: [{
        hash: downloadedSnapshotFile,
        timeRange: {
          initTimestamp: 0, endTimestamp: 8
        }
      },
      {
        hash: anotherSnapshotFile,
        timeRange: {
          initTimestamp: 8, endTimestamp: 16
        },
        replacedSnapshotHashes: ['otherHash1', 'otherHash2']
      }],
    }))

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

    components.router.get(`/contents/${anotherSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream('test/fixtures/bafkreig6sfhegnp4okzecgx3v6gj6pohh5qzw6zjtrdqtggx64743rkmz4'),
      }
    })

    components.router.get('/pointer-changes', async (ctx) => {
      return {
        body: {
          deltas: [],
          pagination: {},
        },
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('should call end of stream for all', async () => {
    const { storage } = stubComponents
    const endStreamOfSpy = jest.spyOn(components.processedSnapshots, 'endStreamOf')
    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
      }
    )

    for await (const deployment of stream) {
      r.push(deployment)
    }
    expect(endStreamOfSpy).toBeCalledTimes(2)
    expect(endStreamOfSpy).toBeCalledWith(downloadedSnapshotFile, 9)
    expect(endStreamOfSpy).toBeCalledWith(anotherSnapshotFile, 0)
  })
})

test('when successfully process a snapshot file and fails to process other', ({ components, stubComponents }) => {
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
    // serve the snapshots
    components.router.get('/snapshots', async () => ({
      body: [{
        // This is processed first because it has greater end timestamp
        hash: downloadedSnapshotFile,
        timeRange: {
          initTimestamp: 10, endTimestamp: 20
        }
      },
      {
        hash: 'unexistent-hash',
        timeRange: {
          initTimestamp: 0, endTimestamp: 10
        }
      }],
    }))

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

    components.router.get('/pointer-changes', async (ctx) => {
      return {
        body: {
          deltas: [],
          pagination: {},
        },
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('should start and end stream only for the successfully read one', async () => {
    const { storage } = stubComponents
    const startStreamOfSpy = jest.spyOn(components.processedSnapshots, 'startStreamOf')
    const endStreamOfSpy = jest.spyOn(components.processedSnapshots, 'endStreamOf')

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
      }
    )

    const iterate = async () => {
      for await (const deployment of stream) {
        r.push(deployment)
      }
    }
    await expect(iterate()).rejects.toThrow()
    expect(startStreamOfSpy).toBeCalledWith(downloadedSnapshotFile)
    expect(startStreamOfSpy).toBeCalledTimes(1)
    expect(endStreamOfSpy).toBeCalledTimes(1)
    expect(endStreamOfSpy).toBeCalledWith(downloadedSnapshotFile, 9)
  })
})

test('getDeployedEntitiesStream from old /snapshot endpoint', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha'
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  const greatestEntityTimestamp = 9
  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshot', async () => ({
      body: {
        hash: downloadedSnapshotFile,
        lastIncludedDeploymentTimestamp: 8,
      },
    }))

    components.router.get('/snapshots', async () => {
      throw new Error('i am an error!')
    })

    // serve the snapshot file
    let downloadAttempts = 0
    components.router.get(`/contents/${downloadedSnapshotFile}`, async () => {
      if (downloadAttempts == 0) {
        await sleep(100)
        downloadAttempts++
        return { status: 503 }
      }

      return {
        body: createReadStream('test/fixtures/bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha'),
      }
    })

    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == (greatestEntityTimestamp - 20 * 60_000).toString()) {
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

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }
  })

  it('fetches stream from old snapshot if new /snapshots endpoint fails', async () => {
    const { storage } = stubComponents

    storage.storeStream.callThrough()
    storage.retrieve.callThrough()

    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
      }
    )

    Sinon.assert.callCount(storage.delete, 0)

    for await (const deployment of stream) {
      r.push(deployment)
    }

    // the downloaded file must be deleted
    Sinon.assert.calledOnce(storage.delete)

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000004', localTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000005', localTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000006', localTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000007', localTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000008', localTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000009', localTimestamp: greatestEntityTimestamp, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain, pointers: ['0x1'] },
    ])
  })
})

test('does not download snapshot if it does not include relevant deployments. keeps polling after finishing without using pagination', ({
  components,
}) => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'deployments-snapshot'
  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshot', async () => ({
      body: {
        hash: downloadedSnapshotFile,
        lastIncludedDeploymentTimestamp: 100,
      },
    }))

    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      // if (ctx.url.searchParams.get('from') == '150') {
      if (ctx.url.searchParams.get('from') == (150 - 20 * 60_000).toString()) {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000150', localTimestamp: 150, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'Qm000151', localTimestamp: 151, authChain, pointers: ['0x1'] },
            ],
            pagination: {},
          },
        }
      }

      if (ctx.url.searchParams.get('from') == '151') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000152', localTimestamp: 152, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'Qm000153', localTimestamp: 153, authChain, pointers: ['0x1'] },
            ],
            pagination: {},
          },
        }
      }

      return {
        status: 503,
      }
    })
  })

  it('fetches the stream', async () => {
    const r = []
    const stream = getDeployedEntitiesStream(
      components,
      {
        fromTimestamp: 150,
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 1,
        requestRetryWaitTime: 1,
        requestMaxRetries: 10,
      }
    )

    for await (const deployment of stream) {
      r.push(deployment)
      if (r.length == 4) break
    }

    expect(r).toEqual([
      { entityType: 'profile', entityId: 'Qm000150', localTimestamp: 150, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000151', localTimestamp: 151, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000152', localTimestamp: 152, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000153', localTimestamp: 153, authChain, pointers: ['0x1'] },
    ])
  })
})

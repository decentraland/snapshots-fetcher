import { getDeployedEntitiesStream } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import Sinon from 'sinon'

test('getDeployedEntitiesStream', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm'
  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshot', async () => ({
      body: {
        hash: downloadedSnapshotFile,
        lastIncludedDeploymentTimestamp: 8,
      },
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
        body: createReadStream('test/fixtures/bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm'),
      }
    })

    components.router.get('/pointer-changes', async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == '9') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain: [] },
              { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain: [] },
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
            { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain: [] },
            { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain: [] },
          ],
          pagination: {},
        },
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch {}
  })

  it('fetches a stream', async () => {
    const { storage } = stubComponents

    const r = []
    const stream = getDeployedEntitiesStream(
      {
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        metrics: components.metrics,
        storage: components.storage,
      },
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
      { entityType: 'profile', entityId: 'Qm000001', localTimestamp: 1, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000002', localTimestamp: 2, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000003', localTimestamp: 3, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000004', localTimestamp: 4, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000005', localTimestamp: 5, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000006', localTimestamp: 6, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000007', localTimestamp: 7, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000008', localTimestamp: 8, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000009', localTimestamp: 9, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000012', localTimestamp: 12, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000013', localTimestamp: 13, authChain: [] },
    ])
  })

  it('fetches a stream without deleting the downloaded file', async () => {
    const { storage } = stubComponents
    await storage.delete(['*'])
    storage.delete.reset()

    const r = []
    const stream = getDeployedEntitiesStream(
      {
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        metrics: components.metrics,
        storage: components.storage,
      },
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

    // the downloaded file must not be deleted
    Sinon.assert.callCount(storage.delete, 0)

    expect(r.length).toBeGreaterThan(0)
  })
})

test("getDeployedEntitiesStream does not download snapshot if it doesn't include relevant deployments. keeps polling after finishing without using pagination", ({
  components,
}) => {
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

      if (ctx.url.searchParams.get('from') == '150') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000150', localTimestamp: 150, authChain: [] },
              { entityType: 'profile', entityId: 'Qm000151', localTimestamp: 151, authChain: [] },
            ],
            pagination: {},
          },
        }
      }

      if (ctx.url.searchParams.get('from') == '151') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000152', localTimestamp: 152, authChain: [] },
              { entityType: 'profile', entityId: 'Qm000153', localTimestamp: 153, authChain: [] },
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
      {
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        metrics: components.metrics,
        storage: components.storage,
      },
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
      { entityType: 'profile', entityId: 'Qm000150', localTimestamp: 150, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000151', localTimestamp: 151, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000152', localTimestamp: 152, authChain: [] },
      { entityType: 'profile', entityId: 'Qm000153', localTimestamp: 153, authChain: [] },
    ])
  })
})

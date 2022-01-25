import { createCatalystDeploymentStream, getDeployedEntitiesStream } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import future from 'fp-future'
import { IDeployerComponent } from '../src/types'

test('createCatalystDeploymentStream', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm'

  let snapshotHits = 0
  let shouldFailOnNextPointerChanges = false

  it('prepares the endpoints', () => {
    // serve the snapshots
    components.router.get('/snapshot', async () => {
      snapshotHits++
      return {
        body: {
          hash: downloadedSnapshotFile,
          lastIncludedDeploymentTimestamp: 8,
        },
      }
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
        body: createReadStream('test/fixtures/bafkreic2h5lbt3bjljanxmlybase65zmv6lbq3r6ervr6vpmqlb432kgzm'),
      }
    })

    components.router.get('/pointer-changes', async (ctx) => {
      if (ctx.url.searchParams.get('sortingOrder') != 'ASC')
        throw new Error('/pointer-changes MUST be ordered by localTimestamp ASC')
      if (ctx.url.searchParams.get('sortingField') != 'local_timestamp')
        throw new Error('/pointer-changes MUST be ordered by localTimestamp ASC')

      if (shouldFailOnNextPointerChanges) {
        shouldFailOnNextPointerChanges = false
        throw new Error('Failing to simulate recovery')
      }

      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      if (ctx.url.searchParams.get('from') == '9') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000010', localTimestamp: 10, authChain: [] },
              { entityType: 'profile', entityId: 'Qm000011', localTimestamp: 11, authChain: [] },
            ],
            pagination: {
              next: '?from=11&entityId=Qm000011&sortingOrder=ASC&sortingField=local_timestamp',
            },
          },
        }
      }

      if (ctx.url.searchParams.get('from') == '13') {
        return {
          body: { deltas: [] },
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
    const r = []
    const finishedFuture = future<void>()

    const deployer: IDeployerComponent = {
      async deployEntity(deployment, servers) {
        r.push(deployment)

        if (r.length == 13) {
          shouldFailOnNextPointerChanges = true
          stream.stop()
          finishedFuture.resolve()
        }
      },
      onIdle: () => finishedFuture,
    }

    const stream = createCatalystDeploymentStream(
      {
        fetcher: components.fetcher,
        downloadQueue: components.downloadQueue,
        logs: components.logs,
        deployer,
        metrics: components.metrics,
        storage: components.storage,
      },
      {
        contentServer: await components.getBaseUrl(),
        tmpDownloadFolder: contentFolder,
        pointerChangesWaitTime: 0,
        requestRetryWaitTime: 0,
        requestMaxRetries: 10,
        reconnectTime: 50,
        fromTimestamp: 0,
      }
    )

    expect(stream.isStopped()).toEqual(true)

    const startPromise = stream.start()
    while (stream.isStopped()) {
      await sleep(1)
    }

    expect(stream.isStopped()).toEqual(false)
    await deployer.onIdle()
    await startPromise

    expect({ snapshotHits }).toEqual({ snapshotHits: 1 })

    expect(stream.getRetryCount()).toEqual(1)
    expect(stream.isStopped()).toEqual(true)

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

    expect(stream.getGreatesProcessedTimestamp()).toEqual(13)
  })
})

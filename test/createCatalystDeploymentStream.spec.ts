import { createCatalystDeploymentStream } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import future from 'fp-future'
import { IDeployerComponent } from '../src/types'
import { AuthLinkType } from '@dcl/schemas'
import ms from 'ms'

test('createCatalystDeploymentStream', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  let snapshotHits = 0
  let shouldFailOnNextPointerChanges = false
  const greatestEntityTimestampFromSnapshot = 9

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
        body: createReadStream('test/fixtures/bafkreicgygsdjrgyrs6dld3ft2cu2anvwzno3ahjjukohpi3lqkz54ei7y'),
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

      if (ctx.url.searchParams.get('from') == (greatestEntityTimestampFromSnapshot - ms('20m')).toString()) {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'Qm000010', entityTimestamp: 10, localTimestamp: 10, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'Qm000011', entityTimestamp: 11, localTimestamp: 11, authChain, pointers: ['0x1'] },
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
            { entityType: 'profile', entityId: 'Qm000012', entityTimestamp: 12, localTimestamp: 12, authChain, pointers: ['0x1'] },
            { entityType: 'profile', entityId: 'Qm000013', entityTimestamp: 13, localTimestamp: 13, authChain, pointers: ['0x1'] },
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
        processedSnapshotStorage: components.processedSnapshotStorage,
        processedSnapshots: components.processedSnapshots
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
      { entityType: 'profile', entityId: 'Qm000001', entityTimestamp: 1, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000002', entityTimestamp: 2, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000003', entityTimestamp: 3, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000004', entityTimestamp: 4, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000005', entityTimestamp: 5, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000006', entityTimestamp: 6, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000007', entityTimestamp: 7, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000008', entityTimestamp: 8, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000009', entityTimestamp: greatestEntityTimestampFromSnapshot, authChain, pointers: ['0x1'], snapshotHash: downloadedSnapshotFile },
      { entityType: 'profile', entityId: 'Qm000010', entityTimestamp: 10, localTimestamp: 10, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000011', entityTimestamp: 11, localTimestamp: 11, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000012', entityTimestamp: 12, localTimestamp: 12, authChain, pointers: ['0x1'] },
      { entityType: 'profile', entityId: 'Qm000013', entityTimestamp: 13, localTimestamp: 13, authChain, pointers: ['0x1'] },
    ])

    expect(stream.getGreatesProcessedTimestamp()).toEqual(13)
  })
})

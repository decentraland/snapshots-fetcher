import { AuthLinkType } from "@dcl/schemas"
import future from 'fp-future'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { createSynchronizer } from "../src/synchronizer"
import { DeployableEntity } from "../src/types"
import { sleep } from "../src/utils"
import { test } from './components'

test('synchronizer deploys entities from snapshots and pointer-changes at bootstrap', ({ components, stubComponents }) => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: '',
    },
  ]

  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'

  it('prepares the endpoints', () => {
    components.router.get(`/snapshots`, async () => {
      return {
        body: [
          {
            hash: downloadedSnapshotFile,
            timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 1 },
            replacedSnapshotHashes: []
          }
        ]
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
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }

    components.router.get(`/pointer-changes`, async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      // it should be called with 1, because: snapshot.endTimestamp - 20 * 60_000 = 1
      if (ctx.url.searchParams.get('from') == '1') {
        return {
          body: {
            deltas: [
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000010', entityTimestamp: 10, localTimestamp: 10, authChain, pointers: ['0x1'] },
              { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000011', entityTimestamp: 11, localTimestamp: 11, authChain, pointers: ['0x1'] },
            ],
            pagination: {},
          },
        }
      } else {
        throw new Error('called with invalid from')
      }
    })
  })

  it('deploys entities from snapshots and pointer-changes', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    const deployedEntities = new Set()
    const synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        processedSnapshotStorage,
        snapshotStorage,
        metrics,
        deployer: {
          async scheduleEntityDeployment(entity: DeployableEntity, contentServers: string[]) {
            if (entity.markAsDeployed) {
              await entity.markAsDeployed()
            }
            deployedEntities.add(entity.entityId)
          },
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      {
        bootstrapReconnection: {
          reconnectTime: 5000 /* five second */,
          reconnectRetryTimeExponent: 1.5,
          maxReconnectionTime: 3_600_000 /* one hour */
        },
        syncingReconnection: {
          reconnectTime: 1000 /* one second */,
          reconnectRetryTimeExponent: 1.2,
          maxReconnectionTime: 3_600_000 /* one hour */
        },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 10,
        requestRetryWaitTime: 5000,
        pointerChangesWaitTime: 5000
      }
    )

    const bootstrapFinished = future<void>()

    const server1url = await components.getBaseUrl()
    const syncJob = await synchronizer.syncWithServers(new Set([server1url]))
    syncJob.onInitialBootstrapFinished(async () => {
      bootstrapFinished.resolve()
    })
    await bootstrapFinished
    await synchronizer.stop()
    const entityIds = [
      // snapshot entities
      'ba000000000000000000000000000000000000000000000000000000001',
      'ba000000000000000000000000000000000000000000000000000000002',
      'ba000000000000000000000000000000000000000000000000000000003',
      'ba000000000000000000000000000000000000000000000000000000004',
      'ba000000000000000000000000000000000000000000000000000000005',
      'ba000000000000000000000000000000000000000000000000000000006',
      'ba000000000000000000000000000000000000000000000000000000007',
      'ba000000000000000000000000000000000000000000000000000000008',
      'ba000000000000000000000000000000000000000000000000000000009',
      // pointer-changes entities
      'ba000000000000000000000000000000000000000000000000000000010',
      'ba000000000000000000000000000000000000000000000000000000011'
    ]
    for (const id of entityIds) {
      expect(deployedEntities.has(id)).toBeTruthy()
    }

    const processed = await processedSnapshotStorage.filterProcessedSnapshotsFrom([downloadedSnapshotFile])

    expect(processed.has(downloadedSnapshotFile)).toBeTruthy()
  })
})

test('synchronizer should not deploy entities from snapshot if it has itdx as own', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const downloadedSnapshotFile = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'

  it('prepares the endpoints', () => {
    components.router.get(`/snapshots`, async () => {
      return {
        body: [
          {
            hash: downloadedSnapshotFile,
            timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 1 },
            replacedSnapshotHashes: []
          }
        ]
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
        body: createReadStream(`test/fixtures/${downloadedSnapshotFile}`),
      }
    })

    try {
      unlinkSync(resolve(contentFolder, downloadedSnapshotFile))
    } catch { }

    components.router.get(`/pointer-changes`, async (ctx) => {
      if (!ctx.url.searchParams.has('from')) throw new Error('pointer-changes called without ?from')

      // it should be called with 1, because: snapshot.endTimestamp - 20 * 60_000 = 1
      if (ctx.url.searchParams.get('from') == '1') {
        return {
          body: {
            deltas: [],
            pagination: {},
          },
        }
      } else {
        throw new Error('called with invalid from')
      }
    })
  })

  it('do not deploy entities from snapshot', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    const deployerMock = {
      scheduleEntityDeployment: jest.fn(),
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    }
    const storageDeleteSpy = jest.spyOn(storage, 'delete')
    jest.spyOn(snapshotStorage, 'has').mockImplementation(async (hash) => {
      return hash == downloadedSnapshotFile
    })
    const synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        processedSnapshotStorage,
        snapshotStorage,
        metrics,
        deployer: deployerMock
      },
      {
        bootstrapReconnection: {
          reconnectTime: 5000 /* five second */,
          reconnectRetryTimeExponent: 1.5,
          maxReconnectionTime: 3_600_000 /* one hour */
        },
        syncingReconnection: {
          reconnectTime: 1000 /* one second */,
          reconnectRetryTimeExponent: 1.2,
          maxReconnectionTime: 3_600_000 /* one hour */
        },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 10,
        requestRetryWaitTime: 5000,
        pointerChangesWaitTime: 5000
      }
    )

    const bootstrapFinished = future<void>()

    const server1url = await components.getBaseUrl()
    const syncJob = await synchronizer.syncWithServers(new Set([server1url]))
    syncJob.onInitialBootstrapFinished(async () => {
      bootstrapFinished.resolve()
    })
    await bootstrapFinished
    await synchronizer.stop()
    expect(deployerMock.scheduleEntityDeployment).not.toBeCalled()
    // expect snapshot is not deleted
    expect(storageDeleteSpy).not.toBeCalled()
  })
})

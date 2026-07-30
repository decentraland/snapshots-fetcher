import future from 'fp-future'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { SynchronizerComponent } from '../src/types'
import { sleep } from '../src/utils'
import { test } from './components'

const snapshotHashes = [
  'bafkreiwarmupfailureaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  'bafkreiwarmupfailurebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  'bafkreiwarmupfailurecccccccccccccccccccccccccccccccccccccccccc'
]
const snapshotBody = readFileSync('test/fixtures/bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu')

test('synchronizer when deployer warm-up fails during snapshot prefetch', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let downloadRequests: number
  let firstDownloadStarted: ReturnType<typeof future<void>>
  let releaseFirstDownload: ReturnType<typeof future<void>>
  let unhandledRejections: unknown[]
  let onUnhandledRejection: (reason: unknown) => void

  it('prepares snapshots whose first download overlaps the failing warm-up', () => {
    downloadRequests = 0
    firstDownloadStarted = future<void>()
    releaseFirstDownload = future<void>()
    unhandledRejections = []
    onUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason)
    process.on('unhandledRejection', onUnhandledRejection)

    components.router.get('/snapshots', async () => ({
      body: snapshotHashes.map((hash, index) => ({
        hash,
        timeRange: { initTimestamp: index * 1000, endTimestamp: (index + 1) * 1000 }
      }))
    }))
    for (const hash of snapshotHashes) {
      components.router.get(`/contents/${hash}`, async () => {
        downloadRequests++
        firstDownloadStarted.resolve()
        await releaseFirstDownload
        return { body: snapshotBody }
      })
    }
  })

  it('runs the failing bootstrap attempt', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        metrics,
        processedSnapshotStorage,
        snapshotStorage,
        deployer: {
          scheduleEntityDeployment: jest.fn(),
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn(async () => {
            await firstDownloadStarted
            throw new Error('warm-up failed')
          })
        }
      },
      {
        bootstrapReconnection: { reconnectTime: 60_000 },
        syncingReconnection: { reconnectTime: 60_000 },
        tmpDownloadFolder: resolve('downloads'),
        requestMaxRetries: 1,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 0,
        concurrency: { snapshotDeployments: 1 }
      }
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await firstDownloadStarted
    releaseFirstDownload.resolve()
    await sleep(100)
  })

  it('should abandon snapshots that had not started downloading', () => {
    expect(downloadRequests).toBe(1)
  })

  it('should keep the rejected readiness gate handled', () => {
    expect(unhandledRejections).toEqual([])
  })

  afterAll(async () => {
    process.off('unhandledRejection', onUnhandledRejection)
    await synchronizer.stop?.()
  })
})

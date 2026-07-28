import future from 'fp-future'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { DeployableEntity, SynchronizerComponent, SynchronizerOptions } from '../src/types'
import { test } from './components'

// Three snapshot hashes deliberately absent from test/fixtures, so each one is really downloaded
// (rather than short-circuiting on the preloaded in-memory storage) and the download can be held open.
const snapshotHashes = [
  'bafkreiconcurrencysnapshotaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  'bafkreiconcurrencysnapshotbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  'bafkreiconcurrencysnapshotccccccccccccccccccccccccccccccccccccc'
]
const snapshotBody = readFileSync('test/fixtures/bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu')

function baseOptions(): SynchronizerOptions {
  return {
    bootstrapReconnection: { reconnectTime: 60_000 },
    syncingReconnection: { reconnectTime: 60_000 },
    tmpDownloadFolder: resolve('downloads'),
    requestMaxRetries: 1,
    requestRetryWaitTime: 0,
    pointerChangesWaitTime: 0
  }
}

test('createSynchronizer snapshot deployment concurrency', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let inFlight: number
  let maxInFlight: number
  let releaseDownloads: ReturnType<typeof future<void>>

  it('prepares the endpoints', () => {
    components.router.get('/snapshots', async () => ({
      body: snapshotHashes.map((hash, index) => ({
        hash,
        // Distinct, non-overlapping ranges so none is treated as replacing another.
        timeRange: { initTimestamp: index * 1000, endTimestamp: (index + 1) * 1000 }
      }))
    }))

    for (const hash of snapshotHashes) {
      components.router.get(`/contents/${hash}`, async () => {
        inFlight++
        maxInFlight = Math.max(maxInFlight, inFlight)
        // Hold every snapshot download open until they have all had a chance to start, so any
        // overlap the queue permits is observable.
        await releaseDownloads
        inFlight--
        return { body: snapshotBody }
      })
    }

    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))
  })

  // Polls until the condition holds instead of assuming a fixed delay was long enough. The previous
  // fixed 60ms sleep raced the event loop: when no download had started yet the assertion read a peak
  // of 0, which it did on most runs.
  async function waitUntil(condition: () => boolean, description: string, timeoutMs = 10_000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (!condition()) {
      if (Date.now() > deadline) {
        throw new Error(`Timed out after ${timeoutMs}ms waiting until ${description}`)
      }
      await new Promise((ok) => setTimeout(ok, 5))
    }
  }

  async function bootstrapWith(concurrency: SynchronizerOptions['concurrency'], expectedPeak: number) {
    inFlight = 0
    maxInFlight = 0
    releaseDownloads = future<void>()

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
          async scheduleEntityDeployment(entity: DeployableEntity) {
            if (entity.markAsDeployed) await entity.markAsDeployed()
          },
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      { ...baseOptions(), concurrency }
    )

    const bootstrapFinished = future<void>()
    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    void syncJob.onInitialBootstrapFinished(async () => bootstrapFinished.resolve())

    let observedPeak: number
    try {
      // The downloads are held open, so once this many are in flight the count cannot drop again —
      // waiting for it is deterministic, unlike sleeping and hoping they started.
      await waitUntil(
        () => inFlight >= expectedPeak,
        `${expectedPeak} snapshot download(s) are in flight (saw ${maxInFlight})`
      )
      // Brief settle so a queue that permits MORE than the bound gets the chance to reveal it. This
      // only guards the upper bound; the lower bound is already established above.
      await new Promise((ok) => setTimeout(ok, 50))
      observedPeak = maxInFlight
    } finally {
      // Always release, or the held request handlers keep the suite hanging until the jest timeout
      // instead of reporting why the wait above failed.
      releaseDownloads.resolve()
    }
    await bootstrapFinished
    await synchronizer.stop!()

    // Snapshots must be forgotten between cases, or the second run skips them as processed.
    await storage.delete(snapshotHashes)
    return observedPeak
  }

  describe('when snapshotDeployments concurrency is 1', () => {
    let observedPeak: number

    beforeEach(async () => {
      observedPeak = await bootstrapWith({ snapshotDeployments: 1 }, 1)
    })

    it('should download the snapshots one at a time', () => {
      expect(observedPeak).toBe(1)
    })
  })

  describe('and snapshotDeployments concurrency allows all of them at once', () => {
    let observedPeak: number

    beforeEach(async () => {
      observedPeak = await bootstrapWith({ snapshotDeployments: 3 }, snapshotHashes.length)
    })

    it('should download them in parallel up to the configured bound', () => {
      expect(observedPeak).toBe(snapshotHashes.length)
    })
  })
})

test('createSynchronizer concurrency validation', ({ components }) => {
  function create(concurrency: SynchronizerOptions['concurrency']) {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    return createSynchronizer(
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
          prepareForDeploymentsIn: jest.fn()
        }
      },
      { ...baseOptions(), concurrency }
    )
  }

  describe('when snapshotDeployments is zero', () => {
    it('should reject naming the offending option instead of failing inside the queue', async () => {
      await expect(create({ snapshotDeployments: 0 })).rejects.toThrow(
        'options.concurrency.snapshotDeployments must be an integer >= 1, got 0'
      )
    })
  })

  describe('when snapshotChecks is not an integer', () => {
    it('should reject naming the offending option', async () => {
      await expect(create({ snapshotChecks: 2.5 })).rejects.toThrow(
        'options.concurrency.snapshotChecks must be an integer >= 1, got 2.5'
      )
    })
  })

  describe('when concurrency is omitted entirely', () => {
    it('should build the synchronizer with the defaults', async () => {
      const synchronizer = await create(undefined)

      await synchronizer.stop!()

      expect(synchronizer.syncWithServers).toBeInstanceOf(Function)
    })
  })
})

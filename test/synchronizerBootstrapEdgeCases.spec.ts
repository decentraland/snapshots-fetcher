import future from 'fp-future'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from '../src/types'
import { sleep } from '../src/utils'
import { test } from './components'

const snapshotHash = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
const replacedHash = 'bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha'

function buildSynchronizer(
  components: SnapshotsFetcherComponents,
  deployer: IDeployerComponent,
  reconnectTime = 60_000
): Promise<SynchronizerComponent> {
  const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
  return createSynchronizer(
    { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage, deployer },
    {
      bootstrapReconnection: { reconnectTime },
      syncingReconnection: { reconnectTime: 60_000 },
      tmpDownloadFolder: resolve('downloads'),
      requestMaxRetries: 2,
      requestRetryWaitTime: 0,
      pointerChangesWaitTime: 0
    }
  )
}

test('synchronizer when a server serves snapshots but fails pointer-changes during bootstrap', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let snapshotsRequests: number
  let pointerChangesRequests: number

  it('prepares the endpoints', () => {
    snapshotsRequests = 0
    pointerChangesRequests = 0
    components.router.get('/snapshots', async () => {
      snapshotsRequests++
      return { body: [{ hash: snapshotHash, timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 1 } }] }
    })
    components.router.get(`/contents/${snapshotHash}`, async () => ({
      body: createReadStream(`test/fixtures/${snapshotHash}`)
    }))
    components.router.get('/pointer-changes', async () => {
      pointerChangesRequests++
      return { status: 503, body: 'pointer-changes is down' }
    })
  })

  it('attempts the bootstrap repeatedly', async () => {
    synchronizer = await buildSynchronizer(
      components,
      {
        // Reports each entity as deployed, which is what markAsDeployed is for. A deployer that never
        // called it would leave the snapshot incomplete, and the server would stay in snapshot
        // bootstrap instead of reaching the pointer-changes phase this test is about.
        async scheduleEntityDeployment(entity) {
          if (entity.markAsDeployed) await entity.markAsDeployed()
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      },
      // Short enough that the failed bootstrap retries within the test.
      50
    )

    const firstAttemptFinished = future<void>()
    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await syncJob.onInitialBootstrapFinished(async () => firstAttemptFinished.resolve())
    await firstAttemptFinished
    await sleep(300)
  })

  it('should keep retrying the pointer-changes bootstrap for the failing server', () => {
    expect(pointerChangesRequests).toBeGreaterThan(1)
  })

  it('should not re-fetch the snapshots it already processed on each retry', () => {
    // The server moved out of the snapshots-bootstrapping set on the first pass, so retries resume at
    // the pointer-changes phase rather than redoing the whole snapshot download.
    expect(snapshotsRequests).toBe(1)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when a snapshot declares replaced snapshot hashes', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let checkedHashes: string[]

  it('prepares the endpoints', () => {
    checkedHashes = []
    components.router.get('/snapshots', async () => ({
      body: [
        {
          hash: snapshotHash,
          timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 1 },
          replacedSnapshotHashes: [replacedHash]
        }
      ]
    }))
    components.router.get(`/contents/${snapshotHash}`, async () => ({
      body: createReadStream(`test/fixtures/${snapshotHash}`)
    }))
    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))
  })

  it('bootstraps the server', async () => {
    jest
      .spyOn(components.processedSnapshotStorage, 'filterProcessedSnapshotsFrom')
      .mockImplementation(async (hashes: string[]) => {
        checkedHashes.push(...hashes)
        return new Set<string>()
      })

    synchronizer = await buildSynchronizer(components, {
      async scheduleEntityDeployment(entity) {
        if (entity.markAsDeployed) await entity.markAsDeployed()
      },
      onIdle: jest.fn(),
      prepareForDeploymentsIn: jest.fn()
    })

    const bootstrapFinished = future<void>()
    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await syncJob.onInitialBootstrapFinished(async () => bootstrapFinished.resolve())
    await bootstrapFinished
  })

  it('should include the replaced hashes in the single processed-snapshots lookup', () => {
    expect(checkedHashes).toEqual(expect.arrayContaining([snapshotHash, replacedHash]))
  })

  afterAll(async () => {
    jest.restoreAllMocks()
    await synchronizer.stop!()
  })
})

test('synchronizer lifecycle guards', ({ components }) => {
  let synchronizer: SynchronizerComponent

  it('prepares the endpoints', () => {
    components.router.get('/snapshots', async () => ({ body: [] }))
    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))
  })

  describe('when syncWithServers is called after stop', () => {
    beforeEach(async () => {
      synchronizer = await buildSynchronizer(components, {
        scheduleEntityDeployment: jest.fn(),
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      })
      await synchronizer.stop!()
    })

    it('should reject instead of starting new work', async () => {
      await expect(synchronizer.syncWithServers(new Set(['https://peer.example.com']))).rejects.toThrow(
        'synchronizer is stopped.'
      )
    })
  })

  describe('when onInitialBootstrapFinished is registered after the first bootstrap already finished', () => {
    let calledImmediately: boolean

    beforeEach(async () => {
      calledImmediately = false
      synchronizer = await buildSynchronizer(components, {
        scheduleEntityDeployment: jest.fn(),
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      })

      const firstBootstrap = future<void>()
      const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
      await syncJob.onInitialBootstrapFinished(async () => firstBootstrap.resolve())
      await firstBootstrap

      // Registering now must invoke the callback straight away rather than queue it forever.
      await syncJob.onInitialBootstrapFinished(async () => {
        calledImmediately = true
      })
    })

    afterEach(async () => {
      await synchronizer.stop!()
    })

    it('should invoke the callback immediately', () => {
      expect(calledImmediately).toBe(true)
    })
  })
})

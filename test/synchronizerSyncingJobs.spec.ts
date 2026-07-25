import { AuthLinkType } from '@dcl/schemas'
import future from 'fp-future'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { DeployableEntity, SynchronizerComponent } from '../src/types'
import { sleep } from '../src/utils'
import { test } from './components'

const authChain = [{ type: AuthLinkType.SIGNER, payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5', signature: '' }]
const snapshotHash = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
const snapshotEndTimestamp = 20 * 60_000 + 1

function pointerChangesDelta(entityId: string, localTimestamp: number) {
  return {
    entityType: 'profile',
    entityId,
    entityTimestamp: localTimestamp,
    localTimestamp,
    authChain,
    pointers: ['0x1']
  }
}

test('synchronizer when a syncing server is dropped from the desired set', ({ components }) => {
  const contentFolder = resolve('downloads')
  let synchronizer: SynchronizerComponent
  let pointerChangesRequests: string[]
  let requestsWhenDropped: number

  it('prepares the endpoints', () => {
    pointerChangesRequests = []

    components.router.get('/snapshots', async () => ({
      body: [{ hash: snapshotHash, timeRange: { initTimestamp: 0, endTimestamp: snapshotEndTimestamp } }]
    }))
    components.router.get('/pointer-changes', async (ctx) => {
      pointerChangesRequests.push(ctx.url.searchParams.get('from') ?? '')
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('bootstraps the server into the syncing state and then drops it', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        processedSnapshotStorage,
        snapshotStorage,
        metrics,
        deployer: {
          async scheduleEntityDeployment(entity: DeployableEntity) {
            if (entity.markAsDeployed) await entity.markAsDeployed()
          },
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      {
        bootstrapReconnection: { reconnectTime: 60_000 },
        syncingReconnection: { reconnectTime: 60_000 },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 3,
        requestRetryWaitTime: 0,
        // Poll quickly so the running stream is clearly observable.
        pointerChangesWaitTime: 20
      }
    )

    const bootstrapFinished = future<void>()
    const serverUrl = await components.getBaseUrl()
    const syncJob = await synchronizer.syncWithServers(new Set([serverUrl]))
    await syncJob.onInitialBootstrapFinished(async () => bootstrapFinished.resolve())
    await bootstrapFinished

    // Let the syncing (post-bootstrap) pointer-changes job poll a few times.
    await sleep(150)
    expect(pointerChangesRequests.length).toBeGreaterThan(1)

    // Drop the server: syncWithServers removes it from every state set, and the new sync job calls
    // setDesiredJobs with the reduced set, which stops the per-server pointer-changes job.
    await synchronizer.syncWithServers(new Set())
    await sleep(100)
    requestsWhenDropped = pointerChangesRequests.length
  })

  it('should stop polling the dropped server instead of streaming it until shutdown', async () => {
    await sleep(300)

    expect(pointerChangesRequests.length).toBe(requestsWhenDropped)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when a syncing stream fails and reconnects', ({ components }) => {
  const contentFolder = resolve('downloads')
  let synchronizer: SynchronizerComponent
  let pointerChangesRequests: string[]

  it('prepares the endpoints', () => {
    pointerChangesRequests = []
    let call = 0

    components.router.get('/snapshots', async () => ({
      body: [{ hash: snapshotHash, timeRange: { initTimestamp: 0, endTimestamp: snapshotEndTimestamp } }]
    }))
    components.router.get('/pointer-changes', async (ctx) => {
      const from = ctx.url.searchParams.get('from') ?? ''
      pointerChangesRequests.push(from)
      call++
      // 1st call: the bootstrap pass. 2nd: the syncing job's first poll, which advances the
      // last-entity timestamp well past the bootstrap value. 3rd: fail, forcing a reconnect.
      if (call === 2) {
        return { body: { deltas: [pointerChangesDelta('ba00000000000000000000000000000000000000000000000000000dead', 9_000_000)], pagination: {} } }
      }
      if (call === 3) {
        return { status: 503, body: 'synthetic stream failure' }
      }
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('bootstraps, deploys a newer entity, then lets the stream fail', async () => {
    const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
    synchronizer = await createSynchronizer(
      {
        fetcher,
        downloadQueue,
        logs,
        storage,
        processedSnapshotStorage,
        snapshotStorage,
        metrics,
        deployer: {
          async scheduleEntityDeployment(entity: DeployableEntity) {
            if (entity.markAsDeployed) await entity.markAsDeployed()
          },
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      {
        bootstrapReconnection: { reconnectTime: 60_000 },
        // Reconnect quickly after the synthetic failure.
        syncingReconnection: { reconnectTime: 30 },
        tmpDownloadFolder: contentFolder,
        requestMaxRetries: 3,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 20
      }
    )

    const bootstrapFinished = future<void>()
    const syncJob = await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await syncJob.onInitialBootstrapFinished(async () => bootstrapFinished.resolve())
    await bootstrapFinished
    await sleep(400)
  })

  it('should resume from the latest processed timestamp rather than rewinding to bootstrap', () => {
    // The bootstrap pass starts at snapshotEndTimestamp - 20min = 1. After the entity at 9_000_000 is
    // deployed, every later request must start from there; a request back at '1' means the reconnect
    // rewound and is re-streaming everything since bootstrap.
    const requestsAfterTheDeployedEntity = pointerChangesRequests.slice(2)

    expect(requestsAfterTheDeployedEntity.every((from) => Number(from) >= 9_000_000)).toBe(true)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

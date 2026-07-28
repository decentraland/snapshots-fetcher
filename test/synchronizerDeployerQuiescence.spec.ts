import { hashV1 } from '@dcl/hashing'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import {
  DeployableEntity,
  IDeployerComponent,
  SnapshotsFetcherComponents,
  SynchronizerComponent
} from '../src/types'
import { test } from './components'

function deployment(entityId: string, entityTimestamp: number) {
  return JSON.stringify({
    entityId,
    entityType: 'profile',
    pointers: ['0x1'],
    entityTimestamp,
    authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
  })
}

function buildSynchronizer(
  components: SnapshotsFetcherComponents,
  deployer: IDeployerComponent,
  reconnectTime: number
): Promise<SynchronizerComponent> {
  const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
  return createSynchronizer(
    { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage, deployer },
    {
      bootstrapReconnection: { reconnectTime },
      syncingReconnection: { reconnectTime: 60_000 },
      tmpDownloadFolder: resolve('downloads'),
      requestMaxRetries: 1,
      requestRetryWaitTime: 0,
      pointerChangesWaitTime: 0
    }
  )
}

async function waitUntil(condition: () => boolean, description: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting until ${description}`)
    }
    await new Promise((ok) => setTimeout(ok, 10))
  }
}

test('synchronizer when the deployer fails to drain during pointer-changes bootstrap', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let pointerChangesRequested: number
  let drainAlreadyFailed: boolean

  it('prepares the endpoints', () => {
    pointerChangesRequested = 0
    drainAlreadyFailed = false
    // No snapshots, so bootstrap goes straight to the pointer-changes phase.
    components.router.get('/snapshots', async () => ({ body: [] }))
    components.router.get('/pointer-changes', async () => {
      pointerChangesRequested++
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('retries the bootstrap after the failed drain', async () => {
    synchronizer = await buildSynchronizer(
      components,
      {
        scheduleEntityDeployment: jest.fn(),
        // Reject the drain that follows the first pointer-changes bootstrap, then behave normally. This
        // is how an asynchronous deployer reports that a queued deployment failed.
        async onIdle() {
          if (pointerChangesRequested >= 1 && !drainAlreadyFailed) {
            drainAlreadyFailed = true
            throw new Error('deployer failed to drain')
          }
        },
        prepareForDeploymentsIn: jest.fn()
      },
      50
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    // With the server stranded in neither state, the retry finds nothing to bootstrap and never polls
    // again, so a second request is the evidence it was retained in pointer-changes bootstrap.
    await waitUntil(
      () => drainAlreadyFailed && pointerChangesRequested >= 2,
      'the bootstrap has been retried after the failed drain'
    )
  })

  it('should keep the server in pointer-changes bootstrap rather than losing it', () => {
    expect(pointerChangesRequested).toBeGreaterThanOrEqual(2)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when the deployer drains without marking every entity deployed', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let pointerChangesRequested: number
  let markedAsProcessed: string[]
  let snapshotHash: string
  let contentsRequested: number

  // Two valid deployments. The deployer will report only the first as deployed, so the snapshot can
  // never be marked processed even though the deployer's queue drains cleanly.
  const snapshotBody = Buffer.from(
    [
      '### Decentraland json snapshot',
      deployment('bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu', 1),
      deployment('ba' + 'b'.repeat(57), 2)
    ].join('\n')
  )

  it('prepares the endpoints', async () => {
    pointerChangesRequested = 0
    contentsRequested = 0
    markedAsProcessed = []
    snapshotHash = await hashV1(snapshotBody)

    components.router.get('/snapshots', async () => ({
      body: [{ hash: snapshotHash, timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 5000 } }]
    }))
    components.router.get(`/contents/${snapshotHash}`, async () => {
      contentsRequested++
      return { body: snapshotBody.toString() }
    })
    components.router.get('/pointer-changes', async () => {
      pointerChangesRequested++
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('attempts the bootstrap', async () => {
    jest
      .spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      .mockImplementation(async (hash: string) => {
        markedAsProcessed.push(hash)
      })

    let scheduled = 0
    synchronizer = await buildSynchronizer(
      components,
      {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          scheduled++
          // Only the first entity reports back. The second is silently dropped, which is exactly what
          // onIdle() resolving cannot rule out.
          if (scheduled === 1 && entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      },
      60_000
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await waitUntil(() => contentsRequested > 0, 'the snapshot has been fetched and streamed')
    // Let the bootstrap get as far as it is going to get.
    await new Promise((ok) => setTimeout(ok, 300))
  })

  it('should not mark the snapshot as processed', () => {
    expect(markedAsProcessed).not.toContain(snapshotHash)
  })

  it('should not promote the server past entities that never deployed', () => {
    expect(pointerChangesRequested).toBe(0)
  })

  afterAll(async () => {
    jest.restoreAllMocks()
    await synchronizer.stop!()
  })
})

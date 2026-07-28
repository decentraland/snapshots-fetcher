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

test('synchronizer when a pointer-changes deployment marks a later timestamp and the drain then fails', ({
  components
}) => {
  let synchronizer: SynchronizerComponent
  let requestedFrom: string[]
  let drainAlreadyFailed: boolean

  // Well past the 20-minute bootstrap shift, so a prematurely committed mark cannot be masked by it.
  const lateLocalTimestamp = 100 * 60_000

  it('prepares the endpoints', () => {
    requestedFrom = []
    drainAlreadyFailed = false
    components.router.get('/snapshots', async () => ({ body: [] }))
    components.router.get('/pointer-changes', async (ctx: any) => {
      requestedFrom.push(new URL(ctx.url.toString()).searchParams.get('from') ?? '')
      // Only the first poll carries the deployment, so the retry cannot re-advance the mark by itself.
      if (requestedFrom.length > 1) {
        return { body: { deltas: [], pagination: {} } }
      }
      return {
        body: {
          deltas: [
            {
              entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
              entityType: 'profile',
              pointers: ['0x1'],
              entityTimestamp: 1,
              localTimestamp: lateLocalTimestamp,
              authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
            }
          ],
          pagination: {}
        }
      }
    })
  })

  it('retries the bootstrap after the failed drain', async () => {
    synchronizer = await buildSynchronizer(
      components,
      {
        // Reports the entity as deployed, which is what advances the high-water mark.
        async scheduleEntityDeployment(entity: DeployableEntity) {
          if (entity.markAsDeployed) await entity.markAsDeployed()
        },
        // ...and then the drain fails, because some OTHER queued deployment did not make it.
        async onIdle() {
          if (requestedFrom.length >= 1 && !drainAlreadyFailed) {
            drainAlreadyFailed = true
            throw new Error('deployer failed to drain')
          }
        },
        prepareForDeploymentsIn: jest.fn()
      },
      50
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await waitUntil(() => drainAlreadyFailed && requestedFrom.length >= 2, 'the bootstrap has been retried')
  })

  it('should resume the retry from the timestamp it started at, not the uncommitted one', () => {
    expect(requestedFrom[1]).toEqual(requestedFrom[0])
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when a pointer-changes bootstrap deployment is silently dropped', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let requestedFrom: string[]
  let scheduled: number

  // The earlier delta is dropped by the deployer; the later one is acknowledged. Because the resume point
  // is a maximum over acknowledged timestamps, the mark would land past the dropped one — and the drain
  // resolving cleanly cannot tell the difference.
  const droppedLocalTimestamp = 50 * 60_000
  const acknowledgedLocalTimestamp = 100 * 60_000

  it('prepares the endpoints', () => {
    requestedFrom = []
    scheduled = 0
    components.router.get('/snapshots', async () => ({ body: [] }))
    components.router.get('/pointer-changes', async (ctx: any) => {
      requestedFrom.push(new URL(ctx.url.toString()).searchParams.get('from') ?? '')
      if (requestedFrom.length > 1) {
        return { body: { deltas: [], pagination: {} } }
      }
      return {
        body: {
          deltas: [
            {
              entityId: 'ba000000000000000000000000000000000000000000000000000000001',
              entityType: 'profile',
              pointers: ['0x1'],
              entityTimestamp: 1,
              localTimestamp: droppedLocalTimestamp,
              authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
            },
            {
              entityId: 'ba000000000000000000000000000000000000000000000000000000002',
              entityType: 'profile',
              pointers: ['0x2'],
              entityTimestamp: 2,
              localTimestamp: acknowledgedLocalTimestamp,
              authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
            }
          ],
          pagination: {}
        }
      }
    })
  })

  it('attempts the bootstrap', async () => {
    synchronizer = await buildSynchronizer(
      components,
      {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          scheduled++
          // Only the second delta reports back. The first is dropped without an error, which is exactly
          // what a resolving onIdle() cannot rule out.
          if (scheduled === 2 && entity.markAsDeployed) {
            await entity.markAsDeployed()
          }
        },
        // Drains cleanly regardless.
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      },
      50
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await waitUntil(() => scheduled >= 2 && requestedFrom.length >= 2, 'the bootstrap has been retried')
  })

  it('should not adopt a resume point past the dropped deployment', () => {
    // The retry must re-request from where the first attempt started, not from the acknowledged mark.
    // Deliberately the only assertion here: a "was it promoted?" check on the request count would pass
    // either way, because a promoted server's syncing job polls this endpoint too. The `from` value is
    // what actually distinguishes the two outcomes.
    expect(requestedFrom[1]).toEqual(requestedFrom[0])
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when a server is dropped midway through its pointer-changes bootstrap', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let pagesServed: number
  const totalPages = 6

  function pageDelta(index: number) {
    return {
      entityId: `ba00000000000000000000000000000000000000000000000000000000${index}`,
      entityType: 'profile',
      pointers: ['0x1'],
      entityTimestamp: index,
      localTimestamp: index,
      authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
    }
  }

  it('prepares the endpoints', () => {
    pagesServed = 0
    components.router.get('/snapshots', async () => ({ body: [] }))
    // A long paginated backlog, so the bootstrap pass takes several round trips to work through.
    components.router.get('/pointer-changes', async () => {
      pagesServed++
      const isLast = pagesServed >= totalPages
      return {
        body: {
          deltas: [pageDelta(pagesServed)],
          pagination: isLast ? {} : { next: `?from=0&page=${pagesServed + 1}` }
        }
      }
    })
  })

  it('drops the server while the backlog is still streaming', async () => {
    synchronizer = await buildSynchronizer(
      components,
      {
        async scheduleEntityDeployment(entity: DeployableEntity) {
          if (entity.markAsDeployed) await entity.markAsDeployed()
          // Remove the server the moment its bootstrap is underway, the way a DAO refresh would.
          if (pagesServed === 2) {
            await synchronizer.syncWithServers(new Set())
          }
        },
        onIdle: jest.fn(),
        prepareForDeploymentsIn: jest.fn()
      },
      60_000
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    await waitUntil(() => pagesServed >= 2, 'the bootstrap has started streaming the backlog')
    await new Promise((ok) => setTimeout(ok, 300))
  })

  it('should stop streaming rather than working through the rest of the backlog', () => {
    // Without a desired-server check in the stream predicate the pass runs to the last page, deploying
    // entities for a server the caller had already removed.
    expect(pagesServed).toBeLessThan(totalPages)
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

test('synchronizer when a syncing-phase deployment is dropped and the stream then reconnects', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let requestedFrom: string[]
  let scheduled: number

  // Well past the 20-minute bootstrap shift, so nothing masks a mark that moved too far.
  const droppedLocalTimestamp = 50 * 60_000
  const acknowledgedLocalTimestamp = 100 * 60_000

  function delta(entityId: string, localTimestamp: number, entityTimestamp: number) {
    return {
      entityId,
      entityType: 'profile',
      pointers: ['0x1'],
      entityTimestamp,
      localTimestamp,
      authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
    }
  }

  it('prepares the endpoints', () => {
    requestedFrom = []
    scheduled = 0
    components.router.get('/snapshots', async () => ({ body: [] }))
    components.router.get('/pointer-changes', async (ctx: any) => {
      requestedFrom.push(new URL(ctx.url.toString()).searchParams.get('from') ?? '')
      // The first poll is the bootstrap's, and it must come back empty so the bootstrap completes and the
      // server is promoted — otherwise the bootstrap guard, not the syncing path, is what is under test.
      // The second poll is the first syncing one, and it carries the pair.
      if (requestedFrom.length === 2) {
        return {
          body: {
            deltas: [
              delta('ba000000000000000000000000000000000000000000000000000000001', droppedLocalTimestamp, 1),
              delta('ba000000000000000000000000000000000000000000000000000000002', acknowledgedLocalTimestamp, 2)
            ],
            pagination: {}
          }
        }
      }
      return { body: { deltas: [], pagination: {} } }
    })
  })

  it('bootstraps, then drops a syncing deployment and lets the stream reconnect', async () => {
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
            scheduled++
            // The earlier entity is dropped, the later one confirmed. A max over acknowledged timestamps
            // would carry the durable resume point past the dropped one.
            if (scheduled !== 1 && entity.markAsDeployed) {
              await entity.markAsDeployed()
            }
          },
          onIdle: jest.fn(),
          prepareForDeploymentsIn: jest.fn()
        }
      },
      {
        bootstrapReconnection: { reconnectTime: 60_000 },
        // Short, so the syncing stream reconnects inside the test and records its resume point.
        syncingReconnection: { reconnectTime: 50 },
        tmpDownloadFolder: resolve('downloads'),
        requestMaxRetries: 1,
        requestRetryWaitTime: 0,
        pointerChangesWaitTime: 0
      }
    )

    await synchronizer.syncWithServers(new Set([await components.getBaseUrl()]))
    // Poll 1 bootstrap, poll 2 the syncing poll carrying the pair, poll 3+ the reconnects whose `from`
    // is the durable resume point being asserted.
    await waitUntil(() => requestedFrom.length >= 4, 'the syncing stream has reconnected after the drop')
  })

  it('should never resume past the dropped deployment', () => {
    const resumedPastTheDrop = requestedFrom.filter((from) => Number(from) > droppedLocalTimestamp)

    expect(resumedPastTheDrop).toEqual([])
  })

  afterAll(async () => {
    await synchronizer.stop!()
  })
})

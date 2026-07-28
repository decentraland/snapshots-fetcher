import future from 'fp-future'
import { resolve } from 'path'
import { createSynchronizer } from '../src/synchronizer'
import { IDeployerComponent, SnapshotsFetcherComponents, SynchronizerComponent } from '../src/types'
import { test } from './components'

// Deliberately absent from test/fixtures, so nothing is preloaded into storage and a decision to deploy
// really does show up as a request for the snapshot's contents.
const alreadyProcessedHash = 'bafkreichainoldestaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
const middleHash = 'bafkreichainmiddlebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
const newestHash = 'bafkreichainnewestccccccccccccccccccccccccccccccccccccccccccccc'

function buildSynchronizer(
  components: SnapshotsFetcherComponents,
  deployer: IDeployerComponent
): Promise<SynchronizerComponent> {
  const { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage } = components
  return createSynchronizer(
    { fetcher, downloadQueue, logs, storage, metrics, processedSnapshotStorage, snapshotStorage, deployer },
    {
      bootstrapReconnection: { reconnectTime: 60_000 },
      syncingReconnection: { reconnectTime: 60_000 },
      tmpDownloadFolder: resolve('downloads'),
      requestMaxRetries: 1,
      requestRetryWaitTime: 0,
      pointerChangesWaitTime: 0
    }
  )
}

test('synchronizer when snapshots form a replacement chain', ({ components }) => {
  let synchronizer: SynchronizerComponent
  let contentsRequested: string[]
  let markedAsProcessed: string[]

  it('prepares the endpoints', () => {
    contentsRequested = []
    markedAsProcessed = []

    // newest replaces middle, middle replaces the one already processed. Resolving the chain needs the
    // decision for `middle` to be visible to the decision for `newest`, which happens in the same pass.
    components.router.get('/snapshots', async () => ({
      body: [
        {
          hash: middleHash,
          timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 1 },
          replacedSnapshotHashes: [alreadyProcessedHash]
        },
        {
          hash: newestHash,
          timeRange: { initTimestamp: 0, endTimestamp: 20 * 60_000 + 2 },
          replacedSnapshotHashes: [middleHash]
        }
      ]
    }))

    for (const hash of [middleHash, newestHash]) {
      components.router.get(`/contents/${hash}`, async () => {
        contentsRequested.push(hash)
        return { body: '' }
      })
    }

    components.router.get('/pointer-changes', async () => ({ body: { deltas: [], pagination: {} } }))
  })

  it('bootstraps the server', async () => {
    jest
      .spyOn(components.processedSnapshotStorage, 'filterProcessedSnapshotsFrom')
      .mockImplementation(async () => new Set<string>([alreadyProcessedHash]))
    jest
      .spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      .mockImplementation(async (hash: string) => {
        markedAsProcessed.push(hash)
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

  it('should mark the whole chain as processed', () => {
    expect(markedAsProcessed).toEqual(expect.arrayContaining([middleHash, newestHash]))
  })

  it('should not download any snapshot in the chain, since all of them are already covered', () => {
    expect(contentsRequested).toEqual([])
  })

  afterAll(async () => {
    jest.restoreAllMocks()
    await synchronizer.stop!()
  })
})

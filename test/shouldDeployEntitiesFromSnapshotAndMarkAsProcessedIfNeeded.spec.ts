import { shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded } from '../src/deploy-entities'
import { IProcessedSnapshotStorageComponent, ISnapshotStorageComponent } from '../src/types'
import { createProcessedSnapshotStorageComponent } from './test-component'

// This function only reads two components, so the storage fakes are built directly instead of going
// through the `test` runner. Each `test()` block stands up a whole program and HTTP server, which a
// decision function that never makes a request does not need.
describe('shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded', () => {
  const snapshotHash = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'
  const genesisTimestamp = 0

  let processedSnapshotStorage: IProcessedSnapshotStorageComponent
  let snapshotStorage: ISnapshotStorageComponent
  let markSnapshotAsProcessed: jest.SpyInstance
  let components: { processedSnapshotStorage: IProcessedSnapshotStorageComponent; snapshotStorage: ISnapshotStorageComponent }

  beforeEach(() => {
    processedSnapshotStorage = createProcessedSnapshotStorageComponent()
    snapshotStorage = { async has() { return false } }
    components = { processedSnapshotStorage, snapshotStorage }
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  describe('when the snapshot has already been processed', () => {
    beforeEach(async () => {
      // Awaited, unlike before: the fake happens to mutate its set before its first suspension point, so
      // the un-awaited calls worked by accident. A storage that actually awaited anything — which the real
      // DB-backed one does — would have made every one of these tests race its own setup.
      await processedSnapshotStorage.markSnapshotAsProcessed(snapshotHash)
      markSnapshotAsProcessed = jest.spyOn(processedSnapshotStorage, 'markSnapshotAsProcessed')
    })

    it('should not deploy it again', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
          components,
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          []
        )
      ).resolves.toBe(false)
    })

    it('should not mark it as processed a second time', async () => {
      await shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
        components,
        genesisTimestamp,
        snapshotHash,
        genesisTimestamp + 1,
        []
      )

      expect(markSnapshotAsProcessed).not.toHaveBeenCalled()
    })
  })

  describe('when the snapshot ends exactly at the genesis timestamp', () => {
    it('should not deploy it, since genesis is the exclusive lower bound', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
          components,
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp,
          []
        )
      ).resolves.toBe(false)
    })
  })

  describe('when the snapshot ends after the genesis timestamp', () => {
    it('should deploy it', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(
          components,
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          []
        )
      ).resolves.toBe(true)
    })
  })

  describe('when the snapshot is one this server produced itself', () => {
    beforeEach(() => {
      snapshotStorage = {
        async has(hash: string) {
          return hash === snapshotHash
        }
      }
      components = { processedSnapshotStorage, snapshotStorage }
    })

    it('should not deploy its own entities back to itself', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [])
      ).resolves.toBe(false)
    })
  })

  describe('when every hash in a replaced group has been processed', () => {
    beforeEach(async () => {
      await processedSnapshotStorage.markSnapshotAsProcessed(h1)
      await processedSnapshotStorage.markSnapshotAsProcessed(h2)
      await processedSnapshotStorage.markSnapshotAsProcessed(h3)
      markSnapshotAsProcessed = jest.spyOn(processedSnapshotStorage, 'markSnapshotAsProcessed')
    })

    it('should not deploy the snapshot, its entities having arrived through the replaced ones', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
          [h1, h2, h3]
        ])
      ).resolves.toBe(false)
    })

    it('should mark the snapshot itself as processed, so the conclusion survives a restart', async () => {
      await shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
        [h1, h2, h3]
      ])

      expect(markSnapshotAsProcessed).toHaveBeenCalledWith(snapshotHash)
    })

    describe('and a second group is only partly processed', () => {
      it('should still treat one fully-processed group as sufficient', async () => {
        await expect(
          shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
            [h1, h2, h3],
            ['non-processed']
          ])
        ).resolves.toBe(false)
      })
    })

    // Every existing case put the fully-processed group first, so a wrapper that looked up only
    // `replacedSnapshotHashes[0]` instead of the flattened list passed this spec. Ordering is not
    // meaningful to the caller, so it must not be meaningful here.
    describe('and that group is not the first one', () => {
      it('should find it regardless of its position', async () => {
        await expect(
          shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
            ['non-processed'],
            [h1, h2, h3]
          ])
        ).resolves.toBe(false)
      })

      it('should mark the snapshot as processed just the same', async () => {
        await shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
          ['non-processed'],
          [h1, h2, h3]
        ])

        expect(markSnapshotAsProcessed).toHaveBeenCalledWith(snapshotHash)
      })
    })
  })

  describe('when no replaced group has been fully processed', () => {
    beforeEach(async () => {
      await processedSnapshotStorage.markSnapshotAsProcessed(h1)
      await processedSnapshotStorage.markSnapshotAsProcessed(h2)
      await processedSnapshotStorage.markSnapshotAsProcessed(h3)
      markSnapshotAsProcessed = jest.spyOn(processedSnapshotStorage, 'markSnapshotAsProcessed')
    })

    it('should deploy the snapshot, since no group covers its entities', async () => {
      await expect(
        shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
          [h1, h2, 'non-processed'],
          [h3, 'non-processed']
        ])
      ).resolves.toBe(true)
    })

    it('should not mark it as processed before its entities have been deployed', async () => {
      await shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
        [h1, h2, 'non-processed'],
        [h3, 'non-processed']
      ])

      expect(markSnapshotAsProcessed).not.toHaveBeenCalledWith(snapshotHash)
    })

    it('should leave the storage with no record of it', async () => {
      await shouldDeployEntitiesFromSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, snapshotHash, 1, [
        [h1, h2, 'non-processed'],
        [h3, 'non-processed']
      ])

      await expect(
        processedSnapshotStorage.filterProcessedSnapshotsFrom([snapshotHash, 'non-processed'])
      ).resolves.toEqual(new Set())
    })
  })
})

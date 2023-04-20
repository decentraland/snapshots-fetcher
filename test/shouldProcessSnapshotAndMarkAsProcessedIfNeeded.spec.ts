import { shouldProcessSnapshotAndMarkAsProcessedIfNeeded } from '../src/synchronizer'
import { test } from './components'


describe('shouldProcessSnapshotAndMarkAsProcessedIfNeeded', () => {

  const processedSnapshotHash = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'
  const genesisTimestamp = 0

  beforeEach(() => {
    jest.restoreAllMocks()
  })

  test('should not process an already processed snapshot', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(processedSnapshotHash)
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: genesisTimestamp + 1,
        replacedSnapshotHashes: [],
        servers: new Set([])
      })
      expect(shouldProcess).toBeFalsy()
    })
  })

  test('should not mark as process an already processed snapshot', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(processedSnapshotHash)
      const markSnapshotAsProcessed = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: genesisTimestamp + 1,
        replacedSnapshotHashes: [],
        servers: new Set([])
      })
      expect(markSnapshotAsProcessed).not.toBeCalled()
    })
  })

  test('should not process snapshot with greatest timestamp smaller or equal than genesis timestamp', ({ components }) => {
    it('run test', async () => {
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 0,
        replacedSnapshotHashes: [],
        servers: new Set([])
      })
      expect(shouldProcess).toBeFalsy()
    })
  })

  test('should process snapshot with greatest timestamp bigger than genesis timestamp', ({ components }) => {
    it('run test', async () => {
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [],
        servers: new Set([])
      })
      expect(shouldProcess).toBeTruthy()
    })
  })

  test('should not process own snapshot', ({ components }) => {
    it('run test', async () => {
      jest.spyOn(components.snapshotStorage, 'has').mockImplementation(async (hash) => {
        return hash === processedSnapshotHash
      })
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [],
        servers: new Set([])
      })
      expect(shouldProcess).toBeFalsy()
    })
  })

  test('should not process snapshot when the replaced hashes of some group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [[h1, h2, h3]],
        servers: new Set([])
      })
      expect(shouldProcess).toBeFalsy()
    })
  })

  test('should mark snapshot as processed when the replaced hashes of some group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const markAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [[h1, h2, h3]],
        servers: new Set([])
      })
      expect(markAsProcessedSpy).toBeCalledWith(processedSnapshotHash)
    })
  })

  test('should mark snapshot as processed when all the hashes of some replaced-hashes group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const markAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [[h1, h2, h3], ['non-processed']],
        servers: new Set([])
      })
      expect(shouldProcess).toBeFalsy()
      expect(markAsProcessedSpy).toBeCalledWith(processedSnapshotHash)
    })
  })

  test('should process snapshot when not all the replaced hashes of any group were processed', ({ components }) => {
    it('run test', async () => {
      components.processedSnapshotStorage.markSnapshotAsProcessed(h1)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h2)
      components.processedSnapshotStorage.markSnapshotAsProcessed(h3)
      const markAsProcessedSpy = jest.spyOn(components.processedSnapshotStorage, 'markSnapshotAsProcessed')
      const shouldProcess = await shouldProcessSnapshotAndMarkAsProcessedIfNeeded(components, genesisTimestamp, {
        snapshotHash: processedSnapshotHash,
        greatestEndTimestamp: 1,
        replacedSnapshotHashes: [[h1, h2, 'non-processed'], [h3, 'non-processed']],
        servers: new Set([])
      })
      expect(shouldProcess).toBeTruthy()
      expect(markAsProcessedSpy).not.toBeCalledWith(processedSnapshotHash)
      const processed = await components.processedSnapshotStorage.filterProcessedSnapshotsFrom([processedSnapshotHash, 'non-processed'])
      expect(processed.size).toEqual(0)
    })
  })
})

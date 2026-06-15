import { decideSnapshotDeploymentFromProcessedSet } from '../src/deploy-entities'

describe('decideSnapshotDeploymentFromProcessedSet', () => {
  const snapshotHash = 'snapshotHash'
  const genesisTimestamp = 100
  let markSnapshotAsProcessed: jest.Mock
  let snapshotStorageHas: jest.Mock
  let components: any

  beforeEach(() => {
    markSnapshotAsProcessed = jest.fn().mockResolvedValue(undefined)
    snapshotStorageHas = jest.fn().mockResolvedValue(false)
    components = {
      processedSnapshotStorage: { markSnapshotAsProcessed, filterProcessedSnapshotsFrom: jest.fn() },
      snapshotStorage: { has: snapshotStorageHas }
    }
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when the snapshot is already in the processed set', () => {
    it('should return false and not mark it as processed again', async () => {
      const result = await decideSnapshotDeploymentFromProcessedSet(
        components,
        new Set([snapshotHash]),
        genesisTimestamp,
        snapshotHash,
        genesisTimestamp + 1,
        []
      )
      expect(result).toBe(false)
      expect(markSnapshotAsProcessed).not.toHaveBeenCalled()
    })
  })

  describe('when the snapshot is not processed', () => {
    describe('and its greatest end timestamp is not newer than the genesis timestamp', () => {
      it('should return false', async () => {
        const result = await decideSnapshotDeploymentFromProcessedSet(
          components,
          new Set(),
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp,
          []
        )
        expect(result).toBe(false)
      })
    })

    describe('and it is newer than genesis and not already present in the snapshot storage', () => {
      beforeEach(() => {
        snapshotStorageHas.mockResolvedValue(false)
      })

      it('should return true', async () => {
        const result = await decideSnapshotDeploymentFromProcessedSet(
          components,
          new Set(),
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          []
        )
        expect(result).toBe(true)
      })
    })

    describe('and it is newer than genesis but already present in the snapshot storage', () => {
      beforeEach(() => {
        snapshotStorageHas.mockResolvedValue(true)
      })

      it('should return false', async () => {
        const result = await decideSnapshotDeploymentFromProcessedSet(
          components,
          new Set(),
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          []
        )
        expect(result).toBe(false)
      })
    })

    describe('and a full replaced-hashes group is already in the processed set', () => {
      it('should mark the snapshot as processed and return false', async () => {
        const result = await decideSnapshotDeploymentFromProcessedSet(
          components,
          new Set(['h1', 'h2']),
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          [['h1', 'h2']]
        )
        expect(result).toBe(false)
        expect(markSnapshotAsProcessed).toHaveBeenCalledWith(snapshotHash)
      })
    })

    describe('and no replaced-hashes group is fully in the processed set', () => {
      it('should not mark the snapshot and decide based on the timestamp', async () => {
        const result = await decideSnapshotDeploymentFromProcessedSet(
          components,
          new Set(['h1']),
          genesisTimestamp,
          snapshotHash,
          genesisTimestamp + 1,
          [['h1', 'h2']]
        )
        expect(result).toBe(true)
        expect(markSnapshotAsProcessed).not.toHaveBeenCalled()
      })
    })
  })
})


import { createConfigComponent } from '@well-known-components/env-config-provider'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { createLogComponent } from '@well-known-components/logger'
import { createProcessedSnapshotsComponent } from '../src'
import { IProcessedSnapshotsComponent, IProcessedSnapshotStorageComponent } from '../src/types'
import { createProcessedSnapshotStorageComponent } from './test-component'


describe('processed snapshots', () => {

  const processedSnapshot = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'

  let processedSnapshotStorage: IProcessedSnapshotStorageComponent
  let processedSnapshots: IProcessedSnapshotsComponent

  beforeEach(() => {
    jest.restoreAllMocks()
    processedSnapshotStorage = createProcessedSnapshotStorageComponent()
    processedSnapshots = createProcessedSnapshotsComponent({ processedSnapshotStorage })
  })

  describe('shouldStream', () => {
    it('should return false when the snapshot was processed', async () => {
      processedSnapshotStorage.saveProcessed(processedSnapshot)
      expect(await processedSnapshots.shouldStream(processedSnapshot)).toBeFalsy()
    })

    it('should return false when the replaced hashes where processed', async () => {
      processedSnapshotStorage.saveProcessed(h1)
      processedSnapshotStorage.saveProcessed(h2)
      processedSnapshotStorage.saveProcessed(h3)

      expect(await processedSnapshots.shouldStream(processedSnapshot, [h1, h2, h3])).toBeFalsy()
    })

    it('should return false and save the snapshot as processed when the replaced hashes where processed', async () => {
      processedSnapshotStorage.saveProcessed(h1)
      processedSnapshotStorage.saveProcessed(h2)
      processedSnapshotStorage.saveProcessed(h3)

      await processedSnapshots.shouldStream(processedSnapshot, [h1, h2, h3])

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.has(processedSnapshot)).toBeTruthy()
    })

    it('should return false when the snapshot is being streamed', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      expect(await processedSnapshots.shouldStream(processedSnapshot)).toBeFalsy()
    })

    it('should return false when the snapshot was completely streamed', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.endStreamOf(processedSnapshot, 1)
      expect(await processedSnapshots.shouldStream(processedSnapshot)).toBeFalsy()
    })

    it('should return true when the snapshot and the replaced ones were not processed', async () => {
      expect(await processedSnapshots.shouldStream(processedSnapshot, [h1, h2, h3])).toBeTruthy()
    })

  })

  describe('endStreamOf', () => {
    it('should not save the snapshot as processed when there are still entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.endStreamOf(processedSnapshot, 1)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should save the snapshot as processed when there are not entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)
      processedSnapshots.endStreamOf(processedSnapshot, 1)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.size).toEqual(1)
    })
  })

  describe('entityProcessedFrom', () => {
    it('should not save the snapshot as processed when the stream is not finished yet', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should not save the snapshot as processed when the stream is finished but there still are entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.endStreamOf(processedSnapshot, 3)
      processedSnapshots.entityProcessedFrom(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)


      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should save the snapshot as processed when there are not entities to process and the stream is finished', async () => {
      processedSnapshots.startStreamOf(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)
      processedSnapshots.entityProcessedFrom(processedSnapshot)
      processedSnapshots.endStreamOf(processedSnapshot, 3)
      processedSnapshots.entityProcessedFrom(processedSnapshot)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshot])
      expect(processedSnapshotsInDB.has(processedSnapshot)).toBeTruthy()
    })
  })
})

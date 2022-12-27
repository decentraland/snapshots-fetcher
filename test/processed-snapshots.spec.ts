import { createConfigComponent } from '@well-known-components/env-config-provider'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { createLogComponent } from '@well-known-components/logger'
import { createTestMetricsComponent } from '@well-known-components/metrics'
import { createProcessedSnapshotsComponent } from '../src'
import { IProcessedSnapshotsComponent, IProcessedSnapshotStorageComponent } from '../src/types'
import { createProcessedSnapshotStorageComponent } from './test-component'
import { metricsDefinitions } from '../src'

describe('processed snapshots', () => {

  const processedSnapshotHash = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'

  let processedSnapshotStorage: IProcessedSnapshotStorageComponent
  let processedSnapshots: IProcessedSnapshotsComponent
  let logs: ILoggerComponent
  let metrics = createTestMetricsComponent(metricsDefinitions)

  beforeAll(async () => {
    logs = await createLogComponent({
      config: createConfigComponent({
        LOG_LEVEL: 'INFO'
      })
    })
  })

  beforeEach(() => {
    jest.restoreAllMocks()
    processedSnapshotStorage = createProcessedSnapshotStorageComponent()
    processedSnapshots = createProcessedSnapshotsComponent({ processedSnapshotStorage, logs, metrics })
  })

  describe('shouldStream', () => {
    it('should return false when the snapshot was processed', async () => {
      processedSnapshotStorage.saveProcessed(processedSnapshotHash)
      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [])).toBeFalsy()
    })

    it('should return false when the replaced hashes where processed (in the same group)', async () => {
      processedSnapshotStorage.saveProcessed(h1)
      processedSnapshotStorage.saveProcessed(h2)
      processedSnapshotStorage.saveProcessed(h3)

      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [[h1, h2, h3]])).toBeFalsy()
    })

    it('should save the snapshot as processed when all the hashes of any replaced-hashes group were processed', async () => {
      processedSnapshotStorage.saveProcessed(h1)
      processedSnapshotStorage.saveProcessed(h2)
      processedSnapshotStorage.saveProcessed(h3)

      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [[h1, h2, h3], ['non-processed']])).toBeFalsy()
      const processed = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processed.has(processedSnapshotHash)).toBeTruthy()
    })

    it('should return true when not all the replaced hashes of any group were processed', async () => {
      processedSnapshotStorage.saveProcessed(h1)
      processedSnapshotStorage.saveProcessed(h2)
      processedSnapshotStorage.saveProcessed(h3)

      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [[h1, h2, 'non-processed'], [h3, 'non-processed']])).toBeTruthy()
      const processed = await processedSnapshotStorage.processedFrom([processedSnapshotHash, 'non-processed'])
      expect(processed.size).toEqual(0)
    })

    it('should return false when the snapshot is being streamed', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [])).toBeFalsy()
    })

    it('should return false when the snapshot was completely streamed', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.endStreamOf(processedSnapshotHash, 0)
      expect(await processedSnapshots.shouldProcessSnapshot(processedSnapshotHash, [])).toBeFalsy()
    })
  })

  describe('endStreamOf', () => {
    it('should not save the snapshot as processed when there are still entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.endStreamOf(processedSnapshotHash, 1)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should save the snapshot as processed when there are not entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)
      processedSnapshots.endStreamOf(processedSnapshotHash, 1)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processedSnapshotsInDB.size).toEqual(1)
    })
  })

  describe('entityProcessedFrom', () => {
    it('should not save the snapshot as processed when the stream is not finished yet', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should not save the snapshot as processed when the stream is finished but there still are entities to process', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.endStreamOf(processedSnapshotHash, 3)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)


      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processedSnapshotsInDB.size).toEqual(0)
    })

    it('should save the snapshot as processed when there are not entities to process and the stream is finished', async () => {
      processedSnapshots.startStreamOf(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)
      processedSnapshots.endStreamOf(processedSnapshotHash, 3)
      processedSnapshots.entityProcessedFrom(processedSnapshotHash)

      const processedSnapshotsInDB = await processedSnapshotStorage.processedFrom([processedSnapshotHash])
      expect(processedSnapshotsInDB.has(processedSnapshotHash)).toBeTruthy()
    })
  })
})

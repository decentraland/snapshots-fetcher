import { createConfigComponent } from '@well-known-components/env-config-provider'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { createLogComponent } from '@well-known-components/logger'
import { IProcessedSnapshotStorageComponent } from '../src/types'
import { createProcessedSnapshotStorageComponent } from './test-component'
import { shouldProcessSnapshotAndMarkAsProcessedIfNeeded } from '../src/synchronizer'

describe('shouldProcessSnapshotAndMarkAsProcessedIfNeeded', () => {

  const processedSnapshotHash = 'someHash'
  const h1 = 'h1'
  const h2 = 'h2'
  const h3 = 'h3'

  let processedSnapshotStorage: IProcessedSnapshotStorageComponent
  let logs: ILoggerComponent

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
  })

  it('should return false when the snapshot was processed', async () => {
    processedSnapshotStorage.markSnapshotAsProcessed(processedSnapshotHash)
    expect(await shouldProcessSnapshotAndMarkAsProcessedIfNeeded({ processedSnapshotStorage }, processedSnapshotHash, [])).toBeFalsy()
  })

  it('should return false when the replaced hashes where processed (in the same group)', async () => {
    processedSnapshotStorage.markSnapshotAsProcessed(h1)
    processedSnapshotStorage.markSnapshotAsProcessed(h2)
    processedSnapshotStorage.markSnapshotAsProcessed(h3)

    expect(await shouldProcessSnapshotAndMarkAsProcessedIfNeeded({ processedSnapshotStorage }, processedSnapshotHash, [[h1, h2, h3]])).toBeFalsy()
  })

  it('should save the snapshot as processed when all the hashes of any replaced-hashes group were processed', async () => {
    processedSnapshotStorage.markSnapshotAsProcessed(h1)
    processedSnapshotStorage.markSnapshotAsProcessed(h2)
    processedSnapshotStorage.markSnapshotAsProcessed(h3)

    expect(await shouldProcessSnapshotAndMarkAsProcessedIfNeeded({ processedSnapshotStorage }, processedSnapshotHash, [[h1, h2, h3], ['non-processed']])).toBeFalsy()
    const processed = await processedSnapshotStorage.filterProcessedSnapshotsFrom([processedSnapshotHash])
    expect(processed.has(processedSnapshotHash)).toBeTruthy()
  })

  it('should return true when not all the replaced hashes of any group were processed', async () => {
    processedSnapshotStorage.markSnapshotAsProcessed(h1)
    processedSnapshotStorage.markSnapshotAsProcessed(h2)
    processedSnapshotStorage.markSnapshotAsProcessed(h3)

    expect(await shouldProcessSnapshotAndMarkAsProcessedIfNeeded({ processedSnapshotStorage }, processedSnapshotHash, [[h1, h2, 'non-processed'], [h3, 'non-processed']])).toBeTruthy()
    const processed = await processedSnapshotStorage.filterProcessedSnapshotsFrom([processedSnapshotHash, 'non-processed'])
    expect(processed.size).toEqual(0)
  })
})

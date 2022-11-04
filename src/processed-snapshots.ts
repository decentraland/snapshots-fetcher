import { IProcessedSnapshotsComponent, SnapshotsFetcherComponents } from "./types"

/**
 * Creates the component that signals the streaming and processing of a snapshot.
 *
 * @public
 */
export function createProcessedSnapshotsComponent(components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage' | 'logs'>): IProcessedSnapshotsComponent {
  const logger = components.logs.getLogger('processed-snapshots-logic')
  const snapshotsBeingStreamed = new Set()
  const snapshotsCompletelyStreamed = new Set()
  const numberOfProcessedEntitiesBySnapshot: Map<string, number> = new Map()

  return {
    async shouldStream(snapshotHash: string, replacedSnapshotHashes?: string[]) {
      const isBeingStreamed = snapshotsBeingStreamed.has(snapshotHash)
      const wasStreamed = snapshotsCompletelyStreamed.has(snapshotHash)
      const replacedHashes = replacedSnapshotHashes ?? []
      const processedSnapshotHashes = await components.processedSnapshotStorage.processedFrom([snapshotHash, ...replacedHashes])
      const snapshotWasAlreadyProcessed = processedSnapshotHashes.has(snapshotHash)
      const replacedHashesWereAlreadyProcessed = replacedHashes.length > 0 && replacedHashes.every((h) => processedSnapshotHashes.has(h))

      if (!snapshotWasAlreadyProcessed && replacedHashesWereAlreadyProcessed) {
        await components.processedSnapshotStorage.saveProcessed(snapshotHash)
      }

      return !isBeingStreamed && !wasStreamed && !(snapshotWasAlreadyProcessed || replacedHashesWereAlreadyProcessed)
    },
    startStreamOf(snapshotHash: string) {
      snapshotsBeingStreamed.add(snapshotHash)
      logger.info('Starting stream...', { snapshotHash })
    },
    async endStreamOf(snapshotHash: string, numberOfStreamedEntities: number) {
      snapshotsCompletelyStreamed.add(snapshotHash)
      let numberOfEntities = numberOfProcessedEntitiesBySnapshot.get(snapshotHash) ?? 0
      numberOfEntities = numberOfEntities - numberOfStreamedEntities
      numberOfProcessedEntitiesBySnapshot.set(snapshotHash, numberOfEntities)
      logger.info('Stream ended.', { snapshotHash })
      if (numberOfEntities == 0) {
        await components.processedSnapshotStorage.saveProcessed(snapshotHash)
      }
    },
    async entityProcessedFrom(snapshotHash: string) {
      let numberOfEntities = numberOfProcessedEntitiesBySnapshot.get(snapshotHash) ?? 0
      numberOfEntities = numberOfEntities + 1
      numberOfProcessedEntitiesBySnapshot.set(snapshotHash, numberOfEntities)
      if (snapshotsCompletelyStreamed.has(snapshotHash) && numberOfEntities == 0) {
        await components.processedSnapshotStorage.saveProcessed(snapshotHash)
      }
    }
  }
}

import { IProcessedSnapshotsComponent, SnapshotsFetcherComponents } from "./types"

/**
 * Creates the component that signals the streaming and processing of a snapshot.
 * It decides wheter or not a snapshot should be streamed when syncing. In _index.ts_ the snapshots are fetched from
 * /snapshots or /snapshot. For each one of those, this component will be used to decide to stream the entities inside
 * it or not. It will return false if the snapshot was already processed or if it's currently being streamed in other
 * execution. Once the streaming starts, it will be signalized @startStreamOf so this component knows that a snapshot is
 * being currently streamed.
 *
 * Each entity streamed within a snapshot will be processed by the catalyst/batchDeployer. It adds each one to a queue
 * and they are processed in background. Once a deploy in background finishes, it signals this component that one
 * entity from the snapshot was processed using @entityProcessedFrom.
 *
 * When the stream of all deployments it's done within a snapshot, then the signal @endStreamOf is called together with
 * the quantity of entities streamed. If the quantity matches the number of @entityProcessedFrom signals, it saves
 * the snapshot as processed. If not, every time the signal @entityProcessedFrom is called, it will check if this
 * condition is met in order to save or not the snapshot as procesed.
 *
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

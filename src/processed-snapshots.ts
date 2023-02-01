import { IProcessedSnapshotsComponent, SnapshotsFetcherComponents } from './types'

/**
 * Creates the component that signals the streaming and processing of a snapshot.
 * It decides wheter or not a snapshot should be streamed when syncing. In _index.ts_ the snapshots are fetched from
 * /snapshots or /snapshot. For each one of those, this component will be used to decide to stream the entities inside
 * it or not. It will return false if the snapshot was already processed (or the ones that it replaces) or if it's
 * currently being streamed. Once the streaming starts, it will be signalized #startStreamOf so this component knows
 * that a snapshot is being currently streamed.
 *
 * Each entity streamed within a snapshot will be processed by the {@link catalyst#batchDeployer}. It adds each one to a queue
 * and they are processed in background. Once a deploy in background finishes, it signals this component that one
 * entity from the snapshot was processed using {@link IProcessedSnapshotsComponent#entityProcessedFrom}.
 *
 * When the stream of all deployments it's done within a snapshot, then the signal {@link IProcessedSnapshotsComponent#endStreamOf} is called together with
 * the quantity of entities streamed. If the quantity matches the number of {@link IProcessedSnapshotsComponent#entityProcessedFrom} signals, it saves
 * the snapshot as processed. If not, every time the signal {@link IProcessedSnapshotsComponent#entityProcessedFrom} is called, it will check if this
 * condition is met in order to save or not the snapshot as procesed.
 *
 *
 * @internal
 */
export function createProcessedSnapshotsComponent(
  components: Pick<SnapshotsFetcherComponents, 'processedSnapshotStorage' | 'logs' | 'metrics'>
): IProcessedSnapshotsComponent {
  const logger = components.logs.getLogger('processed-snapshots-logic')
  const snapshotsBeingStreamed = new Set()
  const snapshotsCompletelyStreamed = new Set()
  const numberOfProcessedEntitiesBySnapshot: Map<string, number> = new Map()

  async function saveIfStreamEndedAndAllEntitiesWereProcessed(snapshotHash: string) {
    const numberOfEntities = numberOfProcessedEntitiesBySnapshot.get(snapshotHash)
    if (snapshotsCompletelyStreamed.has(snapshotHash) && numberOfEntities == 0) {
      await components.processedSnapshotStorage.saveProcessed(snapshotHash)
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'saved' })
    }
  }

  return {
    async shouldProcessSnapshotAndMarkAsProcessedIfNeeded(snapshotHash: string, snapshotReplacedGroups: string[][]) {
      if (snapshotsBeingStreamed.has(snapshotHash) || snapshotsCompletelyStreamed.has(snapshotHash)) {
        return false
      }
      const processedSnapshots = await components.processedSnapshotStorage.processedFrom([
        snapshotHash,
        ...snapshotReplacedGroups.flat()
      ])

      if (processedSnapshots.has(snapshotHash)) {
        return false
      }
      for (const replacedGroup of snapshotReplacedGroups) {
        if (replacedGroup.length > 0 && replacedGroup.every((s) => processedSnapshots.has(s))) {
          await components.processedSnapshotStorage.saveProcessed(snapshotHash)
          return false
        }
      }
      return true
    },
    async startStreamOf(snapshotHash: string) {
      snapshotsBeingStreamed.add(snapshotHash)
      logger.info('Starting stream...', { snapshotHash })
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'stream_start' })
    },
    async endStreamOf(snapshotHash: string, numberOfStreamedEntities: number) {
      snapshotsCompletelyStreamed.add(snapshotHash)
      let numberOfEntities = numberOfProcessedEntitiesBySnapshot.get(snapshotHash) ?? 0
      numberOfEntities = numberOfEntities - numberOfStreamedEntities
      numberOfProcessedEntitiesBySnapshot.set(snapshotHash, numberOfEntities)
      components.metrics.increment('dcl_processed_snapshots_total', { state: 'stream_end' })
      logger.info('Stream ended.', { snapshotHash })
      await saveIfStreamEndedAndAllEntitiesWereProcessed(snapshotHash)
    },
    async entityProcessedFrom(snapshotHash: string) {
      let numberOfEntities = numberOfProcessedEntitiesBySnapshot.get(snapshotHash) ?? 0
      numberOfEntities = numberOfEntities + 1
      numberOfProcessedEntitiesBySnapshot.set(snapshotHash, numberOfEntities)
      await saveIfStreamEndedAndAllEntitiesWereProcessed(snapshotHash)
    }
  }
}

import { filterProcessedSnapshotsInChunks } from '../src/deploy-entities'
import { IProcessedSnapshotStorageComponent } from '../src/types'

describe('filterProcessedSnapshotsInChunks', () => {
  const CHUNK_SIZE = 1000
  let processed: Set<string>
  let requestedChunkSizes: number[]
  let components: { processedSnapshotStorage: IProcessedSnapshotStorageComponent }

  beforeEach(() => {
    processed = new Set()
    requestedChunkSizes = []
    components = {
      processedSnapshotStorage: {
        async filterProcessedSnapshotsFrom(hashes: string[]) {
          requestedChunkSizes.push(hashes.length)
          return new Set(hashes.filter((hash) => processed.has(hash)))
        },
        async markSnapshotAsProcessed(hash: string) {
          processed.add(hash)
        }
      }
    }
  })

  describe('when the list fits in a single chunk', () => {
    beforeEach(() => {
      processed.add('h5')
    })

    it('should issue exactly one lookup', async () => {
      await filterProcessedSnapshotsInChunks(components, ['h1', 'h5'])

      expect(requestedChunkSizes).toEqual([2])
    })

    it('should return the hashes the storage reported as processed', async () => {
      await expect(filterProcessedSnapshotsInChunks(components, ['h1', 'h5'])).resolves.toEqual(new Set(['h5']))
    })
  })

  describe('when the list is empty', () => {
    it('should not touch the storage at all', async () => {
      await filterProcessedSnapshotsInChunks(components, [])

      expect(requestedChunkSizes).toEqual([])
    })
  })

  describe('when the list is exactly one chunk long', () => {
    it('should not issue a second, empty lookup', async () => {
      const hashes = Array.from({ length: CHUNK_SIZE }, (_unused, index) => `h${index}`)

      await filterProcessedSnapshotsInChunks(components, hashes)

      expect(requestedChunkSizes).toEqual([CHUNK_SIZE])
    })
  })

  describe('when the list is far larger than one chunk', () => {
    let hashes: string[]

    beforeEach(() => {
      // Well past the 65,535 bind parameters Postgres accepts in one statement, which is what an
      // unchunked lookup would hand a consumer's repository as a single `IN` clause.
      hashes = Array.from({ length: 70_000 }, (_unused, index) => `h${index}`)
      processed.add('h0')
      processed.add('h35000')
      processed.add('h69999')
    })

    it('should never ask for more than one chunk at a time', async () => {
      await filterProcessedSnapshotsInChunks(components, hashes)

      expect(Math.max(...requestedChunkSizes)).toEqual(CHUNK_SIZE)
    })

    it('should cover every hash exactly once across the chunks', async () => {
      await filterProcessedSnapshotsInChunks(components, hashes)

      expect(requestedChunkSizes.reduce((total, size) => total + size, 0)).toEqual(hashes.length)
    })

    it('should merge the results from every chunk, including the first and last', async () => {
      await expect(filterProcessedSnapshotsInChunks(components, hashes)).resolves.toEqual(
        new Set(['h0', 'h35000', 'h69999'])
      )
    })
  })
})

import future from 'fp-future'
import type { ReadStream } from 'fs'
import { getDeployedEntitiesStreamFromSnapshot } from '../src/stream-entities'

const fs = jest.requireActual<typeof import('fs')>('fs')
const downloader = jest.requireActual<typeof import('../src/downloader')>('../src/downloader')
const snapshotHash = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
const snapshotFixture = `test/fixtures/${snapshotHash}`

function createComponents() {
  return {
    logs: {
      getLogger: jest.fn().mockReturnValue({
        info: jest.fn(),
        debug: jest.fn(),
        error: jest.fn()
      })
    },
    storage: {
      exist: jest.fn().mockResolvedValue(false),
      delete: jest.fn()
    }
  } as any
}

const options = {
  requestRetryWaitTime: 0,
  requestMaxRetries: 1,
  tmpDownloadFolder: 'downloads'
}

describe('getDeployedEntitiesStreamFromSnapshot temporary stream lifecycle', () => {
  afterEach(() => {
    jest.restoreAllMocks()
  })

  describe('when the readiness gate rejects after the snapshot was prefetched', () => {
    let cleanup: jest.Mock
    let createReadStream: jest.SpyInstance
    let iterationError: unknown

    beforeEach(async () => {
      cleanup = jest.fn().mockResolvedValue(undefined)
      createReadStream = jest.spyOn(fs, 'createReadStream')
      jest.spyOn(downloader, 'downloadFileToTemporaryFileWithRetries').mockResolvedValue({
        filename: snapshotFixture,
        cleanup
      })
      const ready = future<void>()
      void ready.catch(() => undefined)
      const stream = getDeployedEntitiesStreamFromSnapshot(
        createComponents(),
        options,
        snapshotHash,
        new Set(['https://peer.example.com']),
        () => false,
        undefined,
        ready
      )
      const iteration = stream.next()
      ready.reject(new Error('warm-up failed'))
      iterationError = await iteration.catch((error) => error)
    })

    it('should not open the temporary file before readiness succeeds', () => {
      expect(createReadStream).not.toHaveBeenCalled()
    })

    it('should remove the prefetched temporary file', () => {
      expect(cleanup).toHaveBeenCalledTimes(1)
    })

    it('should propagate the readiness failure', () => {
      expect(iterationError).toEqual(new Error('warm-up failed'))
    })
  })

  describe('when the consumer stops after the first deployment', () => {
    let cleanup: jest.Mock
    let openedReadStream: ReadStream

    beforeEach(async () => {
      cleanup = jest.fn().mockResolvedValue(undefined)
      jest.spyOn(downloader, 'downloadFileToTemporaryFileWithRetries').mockResolvedValue({
        filename: snapshotFixture,
        cleanup
      })
      const originalCreateReadStream = fs.createReadStream
      jest.spyOn(fs, 'createReadStream').mockImplementation(((...args: Parameters<typeof fs.createReadStream>) => {
        openedReadStream = originalCreateReadStream(...args)
        return openedReadStream
      }) as typeof fs.createReadStream)

      const stream = getDeployedEntitiesStreamFromSnapshot(
        createComponents(),
        options,
        snapshotHash,
        new Set(['https://peer.example.com'])
      )
      await stream.next()
      await stream.return(undefined)
    })

    it('should close its owned file stream', () => {
      expect(openedReadStream.closed).toBe(true)
    })

    it('should remove the temporary file after closing the stream', () => {
      expect(cleanup).toHaveBeenCalledTimes(1)
    })
  })
})

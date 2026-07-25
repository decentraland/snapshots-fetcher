import { ILoggerComponent } from '@well-known-components/interfaces'
import { Readable } from 'stream'
import { SyncDeployment } from '@dcl/schemas'
import { processDeploymentsInStream } from '../src/file-processor'

const validSnapshotLine = {
  entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
  entityType: 'profile',
  pointers: ['0x1'],
  entityTimestamp: 5,
  authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
}

async function collect(stream: Readable, logger: ILoggerComponent.ILogger): Promise<SyncDeployment[]> {
  const collected: SyncDeployment[] = []
  for await (const deployment of processDeploymentsInStream(stream, logger)) {
    collected.push(deployment)
  }
  return collected
}

describe('processDeploymentsInStream', () => {
  let logger: ILoggerComponent.ILogger

  beforeEach(() => {
    logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when a line is well-formed JSON but not a valid deployment', () => {
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      const lines = [JSON.stringify(validSnapshotLine), JSON.stringify({ not: 'a deployment' })].join('\n')
      deployments = await collect(Readable.from([Buffer.from(lines)]), logger)
    })

    it('should yield only the valid deployment', () => {
      expect(deployments).toHaveLength(1)
    })

    it('should log the invalid deployment with the validation errors', () => {
      expect(logger.error).toHaveBeenCalledWith(
        'ERROR: Invalid entity deployment in snapshot file',
        expect.objectContaining({ deployment: expect.any(String), errors: expect.any(String) })
      )
    })
  })

  describe('when a line carries the extra localTimestamp of a pointer-changes deployment', () => {
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      // A pointer-changes deployment is a snapshot deployment plus localTimestamp, so the single
      // snapshot check accepts it. This is what makes a separate pointer-changes branch unreachable.
      const pointerChangesLine = { ...validSnapshotLine, localTimestamp: 7 }
      deployments = await collect(Readable.from([Buffer.from(JSON.stringify(pointerChangesLine))]), logger)
    })

    it('should yield it', () => {
      expect(deployments).toHaveLength(1)
    })

    it('should not log any error', () => {
      expect(logger.error).not.toHaveBeenCalled()
    })
  })

  describe('when a line has localTimestamp but is missing entityTimestamp', () => {
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      const { entityTimestamp: _dropped, ...withoutEntityTimestamp } = validSnapshotLine
      const line = { ...withoutEntityTimestamp, localTimestamp: 7 }
      deployments = await collect(Readable.from([Buffer.from(JSON.stringify(line))]), logger)
    })

    it('should reject it, since both deployment shapes require entityTimestamp', () => {
      expect(deployments).toEqual([])
    })
  })

  describe('when a line is not parseable JSON', () => {
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      const lines = ['{not json at all}', JSON.stringify(validSnapshotLine)].join('\n')
      deployments = await collect(Readable.from([Buffer.from(lines)]), logger)
    })

    it('should skip it and still yield the following valid deployment', () => {
      expect(deployments).toHaveLength(1)
    })

    it('should log a parse error for the malformed line', () => {
      expect(logger.error).toHaveBeenCalledWith(
        'ERROR: Could not parse line in snapshot file',
        expect.objectContaining({ line: '{not json at all}' })
      )
    })
  })

  describe('when lines do not look like JSON objects at all', () => {
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      // The snapshot format prefixes a header line; anything not wrapped in braces is ignored silently.
      const lines = ['### Decentraland json snapshot', '', '   ', JSON.stringify(validSnapshotLine)].join('\n')
      deployments = await collect(Readable.from([Buffer.from(lines)]), logger)
    })

    it('should ignore them without logging an error', () => {
      expect(logger.error).not.toHaveBeenCalled()
    })

    it('should still yield the valid deployment', () => {
      expect(deployments).toHaveLength(1)
    })
  })

  describe('when the file contains more invalid lines than the log cap allows', () => {
    beforeEach(async () => {
      const lines = Array.from({ length: 150 }, (_unused, index) => JSON.stringify({ invalid: index })).join('\n')
      await collect(Readable.from([Buffer.from(lines)]), logger)
    })

    it('should cap the per-line errors at 100 plus one suppression notice', () => {
      expect(logger.error).toHaveBeenCalledTimes(101)
    })

    it('should log that further line errors are suppressed', () => {
      expect(logger.error).toHaveBeenCalledWith(
        'Too many invalid lines in snapshot file, suppressing further line errors',
        { suppressedAfter: '100' }
      )
    })
  })
})

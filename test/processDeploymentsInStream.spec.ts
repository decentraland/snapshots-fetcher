import { ILoggerComponent } from '@well-known-components/interfaces'
import { Readable } from 'stream'
import { SyncDeployment } from '@dcl/schemas'
import { processDeploymentsInStream, SnapshotStreamReport } from '../src/file-processor'

const validSnapshotLine = {
  entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
  entityType: 'profile',
  pointers: ['0x1'],
  entityTimestamp: 5,
  authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
}

async function collect(
  stream: Readable,
  logger: ILoggerComponent.ILogger,
  report?: SnapshotStreamReport
): Promise<SyncDeployment[]> {
  const collected: SyncDeployment[] = []
  for await (const deployment of processDeploymentsInStream(stream, logger, report)) {
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
      // The header line and blank padding are part of the snapshot format, so they are framing rather
      // than lines we failed to read.
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

  describe('when a report is supplied and every line is readable', () => {
    let report: SnapshotStreamReport

    beforeEach(async () => {
      report = { unusableLines: 0 }
      const lines = ['### Decentraland json snapshot', '', JSON.stringify(validSnapshotLine)].join('\n')
      await collect(Readable.from([Buffer.from(lines)]), logger, report)
    })

    it('should leave the unusable line count at zero so the snapshot can be marked processed', () => {
      expect(report.unusableLines).toEqual(0)
    })
  })

  describe('when a report is supplied and the file ends with a truncated line', () => {
    let report: SnapshotStreamReport
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      report = { unusableLines: 0 }
      const lines = [JSON.stringify(validSnapshotLine), '{"entityType":"pro'].join('\n')
      deployments = await collect(Readable.from([Buffer.from(lines)]), logger, report)
    })

    it('should count the truncated line as unusable', () => {
      expect(report.unusableLines).toEqual(1)
    })

    it('should log the unrecognized line instead of dropping it in silence', () => {
      expect(logger.error).toHaveBeenCalledWith('ERROR: Unrecognized line in snapshot file', {
        line: '{"entityType":"pro'
      })
    })

    it('should still yield the deployments it could read', () => {
      expect(deployments).toHaveLength(1)
    })
  })

  describe('when a report is supplied and a line fails schema validation', () => {
    let report: SnapshotStreamReport

    beforeEach(async () => {
      report = { unusableLines: 0 }
      const lines = [JSON.stringify(validSnapshotLine), JSON.stringify({ not: 'a deployment' })].join('\n')
      await collect(Readable.from([Buffer.from(lines)]), logger, report)
    })

    it('should count the invalid deployment as unusable', () => {
      expect(report.unusableLines).toEqual(1)
    })
  })

  describe('when a report is supplied and more lines are invalid than the log cap allows', () => {
    let report: SnapshotStreamReport

    beforeEach(async () => {
      report = { unusableLines: 0 }
      const lines = Array.from({ length: 150 }, (_unused, index) => JSON.stringify({ invalid: index })).join('\n')
      await collect(Readable.from([Buffer.from(lines)]), logger, report)
    })

    it('should keep counting past the log cap so suppression cannot hide missing entities', () => {
      expect(report.unusableLines).toEqual(150)
    })
  })

  describe('when a single line exceeds the maximum allowed length', () => {
    let thrownError: Error | undefined

    beforeEach(async () => {
      thrownError = undefined
      // 12 MiB with no newline, against a 10 MiB cap.
      const chunk = Buffer.alloc(1024 * 1024, 0x61)
      async function* newlineless() {
        for (let index = 0; index < 12; index++) {
          yield chunk
        }
      }
      try {
        await collect(Readable.from(newlineless()), logger)
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should fail the snapshot rather than buffering the whole file into one string', () => {
      expect(thrownError?.message).toEqual('Snapshot line exceeds the maximum allowed length of 10485760 bytes')
    })
  })
  describe.each([
    ['a far-future instant', Date.now() + 400 * 24 * 60 * 60 * 1000],
    ['1e308', 1e308],
    ['an unsafe integer', 9007199254740993],
    ['a fractional value', 100.5]
  ])('when a line carries %s as its entityTimestamp', (_label: string, entityTimestamp: number) => {
    let report: SnapshotStreamReport
    let deployments: SyncDeployment[]

    beforeEach(async () => {
      report = { unusableLines: 0 }
      // All four pass SnapshotSyncDeployment.validate: the schema rejects Infinity and negatives but
      // bounds nothing above. The entity would otherwise be handed to the deployer carrying this value.
      const lines = [JSON.stringify({ ...validSnapshotLine, entityTimestamp }), JSON.stringify(validSnapshotLine)].join(
        '\n'
      )
      deployments = await collect(Readable.from([Buffer.from(lines)]), logger, report)
    })

    it('should not yield it', () => {
      expect(deployments).toHaveLength(1)
    })

    it('should count it as unusable, so the snapshot stays unprocessed and is retried', () => {
      expect(report.unusableLines).toEqual(1)
    })
  })

  describe('when an invalid line is very large', () => {
    let loggedLine: string

    beforeEach(async () => {
      const hugeLine = `{"entityType":"profile","junk":"${'a'.repeat(200_000)}"}`
      await collect(Readable.from([Buffer.from(hugeLine)]), logger)
      const call = (logger.error as jest.Mock).mock.calls[0]
      loggedLine = String(call[1].deployment ?? call[1].line ?? '')
    })

    it('should log a truncated preview rather than the whole attacker-controlled payload', () => {
      expect(loggedLine.length).toBeLessThan(1000)
    })

    it('should record the original length so the truncation is visible', () => {
      expect(loggedLine).toContain('original length')
    })
  })
  describe('when the underlying stream fails midway through the snapshot', () => {
    let thrownError: Error | undefined
    let yielded: number

    beforeEach(async () => {
      thrownError = undefined
      yielded = 0
      // A storage read that dies after one good line. pipe() does not forward a source error to its
      // destination, so before this was bridged explicitly the error surfaced as an unhandled 'error'
      // event — a process crash — rather than rejecting the snapshot.
      const source = new Readable({ read() {} })
      source.push(Buffer.from(JSON.stringify(validSnapshotLine) + '\n'))
      setTimeout(() => source.destroy(new Error('storage read failed mid-snapshot')), 20)

      try {
        for await (const _deployment of processDeploymentsInStream(source, logger)) {
          yielded++
        }
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should reject with the source error rather than crashing or hanging', () => {
      expect(thrownError?.message).toEqual('storage read failed mid-snapshot')
    })

    it('should still have yielded the deployments it read before the failure', () => {
      expect(yielded).toEqual(1)
    })
  })
  describe.each([
    [
      'an oversized line and its newline arrive in the same chunk',
      [Buffer.concat([Buffer.alloc(12 * 1024 * 1024, 0x61), Buffer.from('\ntail\n')])]
    ],
    [
      'an oversized line is followed by a shorter one in the same chunk',
      [
        Buffer.concat([
          Buffer.alloc(12 * 1024 * 1024, 0x61),
          Buffer.from('\n'),
          Buffer.alloc(100, 0x62),
          Buffer.from('\n')
        ])
      ]
    ],
    [
      'a line spans chunks and its newline arrives with more data',
      [Buffer.alloc(6 * 1024 * 1024, 0x61), Buffer.concat([Buffer.alloc(6 * 1024 * 1024, 0x61), Buffer.from('\nshort\n')])]
    ]
  ])('when %s', (_label: string, chunks: Buffer[]) => {
    let thrownError: Error | undefined

    beforeEach(async () => {
      thrownError = undefined
      // Each of these hides the oversized line *behind* a newline, which is what an earlier limiter that
      // only measured bytes after the last newline in a chunk failed to catch.
      try {
        await collect(Readable.from(chunks), logger)
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should still fail the snapshot rather than forwarding the oversized line', () => {
      expect(thrownError?.message).toEqual('Snapshot line exceeds the maximum allowed length of 10485760 bytes')
    })
  })

  describe('when a line is exactly at the maximum allowed length', () => {
    let thrownError: Error | undefined

    beforeEach(async () => {
      thrownError = undefined
      try {
        await collect(Readable.from([Buffer.concat([Buffer.alloc(10 * 1024 * 1024, 0x61), Buffer.from('\n')])]), logger)
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should accept it, so the cap is a maximum rather than an exclusive bound', () => {
      expect(thrownError).toBeUndefined()
    })
  })
})

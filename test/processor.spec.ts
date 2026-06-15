import { AuthLinkType } from '@dcl/schemas'
import { Readable } from 'stream'
import { processDeploymentsInFile, processDeploymentsInStream } from '../src/file-processor'
import { createStorageComponent } from './test-component'
import { test } from './components'
import { ILoggerComponent } from '@well-known-components/interfaces'

function streamOfLines(lines: string[]): Readable {
  return Readable.from(
    (function* () {
      for (const line of lines) {
        yield line + '\n'
      }
    })()
  )
}

test('processor', ({ components, stubComponents }) => {

  describe('processor', () => {
    const authChain = [
      {
        type: AuthLinkType.SIGNER,
        payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
        signature: '',
      },
    ]
    let logger: ILoggerComponent.ILogger

    beforeAll(() => {
      logger = components.logs.getLogger('processor-test-logger')
    })

    it('emits every deployment ignoring empty lines', async () => {
      const r = []
      const stream = processDeploymentsInFile(
        'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
        { storage: await createStorageComponent() },
        logger
      )

      for await (const deployment of stream) {
        r.push(deployment)
      }

      expect(r).toEqual([
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000001', entityTimestamp: 1, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000002', entityTimestamp: 2, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000003', entityTimestamp: 3, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000004', entityTimestamp: 4, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000005', entityTimestamp: 5, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000006', entityTimestamp: 6, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000007', entityTimestamp: 7, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000008', entityTimestamp: 8, authChain, pointers: ['0x1'] },
        { entityType: 'profile', entityId: 'ba000000000000000000000000000000000000000000000000000000009', entityTimestamp: 9, authChain, pointers: ['0x1'] },
      ])
    })

    it('fails on unexistent file', async () => {
      await expect(async () => {
        const stream = processDeploymentsInFile(
          'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu' + Math.random(),
          { storage: await createStorageComponent() },
          logger
        )
        for await (const c of stream) {
          // noop
        }
      }).rejects.toThrow('does not exist')
    })
  })
})

describe('processDeploymentsInStream', () => {
  const authChain = [
    {
      type: AuthLinkType.SIGNER,
      payload: '0x3b21028719a4aca7ebee35b0157a6f1b0cf0d0c5',
      signature: ''
    }
  ]
  let errorMock: jest.Mock
  let logger: ILoggerComponent.ILogger

  beforeEach(() => {
    errorMock = jest.fn()
    logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: errorMock }
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when the stream contains a malformed JSON line between valid deployments', () => {
    let deployment1: any
    let deployment2: any
    let stream: Readable

    beforeEach(() => {
      deployment1 = {
        entityType: 'profile',
        entityId: 'ba000000000000000000000000000000000000000000000000000000001',
        entityTimestamp: 1,
        authChain,
        pointers: ['0x1']
      }
      deployment2 = {
        entityType: 'profile',
        entityId: 'ba000000000000000000000000000000000000000000000000000000002',
        entityTimestamp: 2,
        authChain,
        pointers: ['0x1']
      }
      stream = streamOfLines([JSON.stringify(deployment1), '{not valid json}', JSON.stringify(deployment2)])
    })

    it('should skip the malformed line and still yield the valid deployments', async () => {
      const yielded: any[] = []
      for await (const deployment of processDeploymentsInStream(stream, logger)) {
        yielded.push(deployment)
      }
      expect(yielded).toEqual([deployment1, deployment2])
    })

    it('should log a single error for the malformed line', async () => {
      for await (const _deployment of processDeploymentsInStream(stream, logger)) {
        // drain
      }
      expect(errorMock).toHaveBeenCalledTimes(1)
    })
  })

  describe('when the stream contains more invalid lines than the logging cap', () => {
    let stream: Readable

    beforeEach(() => {
      stream = streamOfLines(new Array(150).fill('{not valid json}'))
    })

    it('should cap the logged errors at 100 plus one suppression message', async () => {
      for await (const _deployment of processDeploymentsInStream(stream, logger)) {
        // drain the (empty) stream of valid deployments
      }
      expect(errorMock).toHaveBeenCalledTimes(101)
    })
  })
})

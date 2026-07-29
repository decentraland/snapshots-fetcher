import { ILoggerComponent } from '@well-known-components/interfaces'
import { IFetchComponent, RequestOptions } from '@dcl/core-commons'
import { PointerChangesSyncDeployment } from '@dcl/schemas'
import { fetchPointerChanges } from '../src/client'

function createStubLogger(): ILoggerComponent.ILogger {
  return { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
}

function createJsonFetcher(body: unknown): IFetchComponent {
  return {
    async fetch(): Promise<Response> {
      return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } })
    }
  }
}

/**
 * Stands in for a content server that accepts the connection and then stops sending anything.
 *
 * It only settles when the caller bounded the request: an unbounded request gets a promise that
 * never resolves, exactly like the real stall. That makes "the request carries a timeout" observable
 * as "the stream rejects" instead of "the test hangs".
 */
function createStalledFetcher(): IFetchComponent & { requestedTimeouts: (number | undefined)[] } {
  const requestedTimeouts: (number | undefined)[] = []
  return {
    requestedTimeouts,
    async fetch(_url: string | URL | Request, init?: RequestOptions): Promise<Response> {
      requestedTimeouts.push(init?.timeout)
      if (!init?.timeout) {
        return new Promise<Response>(() => undefined)
      }
      throw new Error(`Request timed out after ${init.timeout}ms`)
    }
  }
}

describe('fetchPointerChanges', () => {
  describe('when the content server accepts the connection but never responds', () => {
    let fetcher: IFetchComponent & { requestedTimeouts: (number | undefined)[] }
    let logger: ILoggerComponent.ILogger

    beforeEach(() => {
      fetcher = createStalledFetcher()
      logger = createStubLogger()
    })

    afterEach(() => {
      jest.resetAllMocks()
    })

    it('should reject with a timeout instead of stalling the stream forever', async () => {
      const deployments = fetchPointerChanges({ fetcher }, 'https://peer.example.com', 0, logger)

      await expect(deployments[Symbol.asyncIterator]().next()).rejects.toThrow('Request timed out')
    })

    it('should bound the request with a positive timeout', async () => {
      const deployments = fetchPointerChanges({ fetcher }, 'https://peer.example.com', 0, logger)

      await expect(deployments[Symbol.asyncIterator]().next()).rejects.toThrow()
      expect(fetcher.requestedTimeouts).toEqual([expect.any(Number)])
    })
  })

  describe('when the feed mixes valid and schema-invalid deployments', () => {
    let logger: ILoggerComponent.ILogger
    let validDeployment: PointerChangesSyncDeployment
    let yielded: PointerChangesSyncDeployment[]

    beforeEach(async () => {
      logger = createStubLogger()
      validDeployment = {
        entityId: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
        entityType: 'profile',
        pointers: ['0x1'],
        entityTimestamp: 5,
        localTimestamp: 6,
        authChain: [{ type: 'SIGNER', payload: '0x1', signature: '' }]
      } as PointerChangesSyncDeployment

      const fetcher = createJsonFetcher({
        deltas: [validDeployment, { entityId: 'missing-everything-else' }],
        pagination: {}
      })

      yielded = []
      for await (const deployment of fetchPointerChanges({ fetcher }, 'https://peer.example.com', 0, logger)) {
        yielded.push(deployment)
      }
    })

    afterEach(() => {
      jest.resetAllMocks()
    })

    it('should yield only the deployment that satisfies the schema', () => {
      expect(yielded).toEqual([validDeployment])
    })

    it('should log the rejected deployment with the validation errors', () => {
      expect(logger.error).toHaveBeenCalledWith(
        'ERROR: Invalid entity deployment from /pointer-changes',
        expect.objectContaining({ deployment: expect.any(String), error: expect.any(String) })
      )
    })
  })
})

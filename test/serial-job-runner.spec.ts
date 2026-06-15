import { ILoggerComponent } from '@well-known-components/interfaces'
import future from 'fp-future'
import { createSerialJobRunner } from '../src/serial-job-runner'
import { IJobWithLifecycle } from '../src/job-lifecycle-manager'
import { sleep } from '../src/utils'

describe('createSerialJobRunner', () => {
  let logger: ILoggerComponent.ILogger
  let runner: ReturnType<typeof createSerialJobRunner>

  beforeEach(() => {
    logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    runner = createSerialJobRunner(logger)
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when more jobs are enqueued than can be running at once', () => {
    let startOrder: string[]
    let allStarted: ReturnType<typeof future<void>>

    beforeEach(() => {
      startOrder = []
      allStarted = future<void>()
    })

    it('should run every enqueued job in FIFO order', async () => {
      const makeJob = (id: string, isLast = false): IJobWithLifecycle => ({
        async start() {
          startOrder.push(id)
          if (isLast) {
            allStarted.resolve()
          }
        },
        async stop() {}
      })

      runner.enqueue(makeJob('a'))
      runner.enqueue(makeJob('b'))
      runner.enqueue(makeJob('c', true))

      await allStarted

      expect(startOrder).toEqual(['a', 'b', 'c'])
    })
  })

  describe('when a job is still running', () => {
    let startOrder: string[]
    let releaseFirstJob: ReturnType<typeof future<void>>

    beforeEach(() => {
      startOrder = []
      releaseFirstJob = future<void>()
    })

    it('should not start the next job until the running one finishes', async () => {
      runner.enqueue({
        async start() {
          startOrder.push('a')
          await releaseFirstJob
        },
        async stop() {}
      })
      runner.enqueue({
        async start() {
          startOrder.push('b')
        },
        async stop() {}
      })

      await sleep(20)
      expect(startOrder).toEqual(['a'])

      releaseFirstJob.resolve()
      await sleep(20)
      expect(startOrder).toEqual(['a', 'b'])
    })
  })

  describe('when stop() is called while a job is running and others are queued', () => {
    let startOrder: string[]
    let firstJobStopped: boolean
    let releaseFirstJob: ReturnType<typeof future<void>>

    beforeEach(() => {
      startOrder = []
      firstJobStopped = false
      releaseFirstJob = future<void>()
    })

    it('should stop the running job and never start the queued ones', async () => {
      runner.enqueue({
        async start() {
          startOrder.push('a')
          await releaseFirstJob
        },
        async stop() {
          firstJobStopped = true
          releaseFirstJob.resolve()
        }
      })
      runner.enqueue({
        async start() {
          startOrder.push('b')
        },
        async stop() {}
      })

      await sleep(20)
      await runner.stop()
      await sleep(20)

      expect(firstJobStopped).toBe(true)
      expect(startOrder).toEqual(['a'])
    })
  })
})

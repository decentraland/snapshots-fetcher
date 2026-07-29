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

  describe('and jobs are queued behind the running one', () => {
    let stoppedJobs: string[]
    let releaseRunningJob: ReturnType<typeof future<void>>

    beforeEach(async () => {
      stoppedJobs = []
      releaseRunningJob = future<void>()

      const makeJob = (id: string): IJobWithLifecycle => ({
        async start() {
          if (id === 'running') {
            await releaseRunningJob
          }
        },
        async stop() {
          stoppedJobs.push(id)
          releaseRunningJob.resolve()
        }
      })

      runner.enqueue(makeJob('running'))
      runner.enqueue(makeJob('queued-1'))
      runner.enqueue(makeJob('queued-2'))

      await sleep(20)
      await runner.stop()
    })

    it('should stop the queued jobs too, so nothing they handed a caller stays pending', () => {
      expect(stoppedJobs).toEqual(['running', 'queued-1', 'queued-2'])
    })
  })

  describe('when a queued job throws while being stopped', () => {
    let stoppedJobs: string[]

    beforeEach(async () => {
      stoppedJobs = []

      runner.enqueue({
        async start() {},
        async stop() {
          throw new Error('failed to stop')
        }
      })
      runner.enqueue({
        async start() {},
        async stop() {
          stoppedJobs.push('second')
        }
      })

      await runner.stop()
    })

    it('should log the failure and still stop the remaining jobs', () => {
      expect(logger.error).toHaveBeenCalled()
      expect(stoppedJobs).toEqual(['second'])
    })
  })
})

import future from 'fp-future'
import { createJobLifecycleManagerComponent, IJobWithLifecycle } from '../src/job-lifecycle-manager'
import { createSerialJobRunner } from '../src/serial-job-runner'
import { sleep } from '../src/utils'
import { test } from './components'

test('createJobLifecycleManagerComponent', ({ components }) => {
  describe('when a replacement job is dropped again before its predecessor finished winding down', () => {
    let started: string[]
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>
    let releaseFirstJob: ReturnType<typeof future<void>>

    beforeEach(async () => {
      started = []
      releaseFirstJob = future<void>()
      let instances = 0

      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'replacement-manager',
          createJob(name: string) {
            const instance = `${name}-${++instances}`
            return {
              async start() {
                started.push(instance)
                if (instance === 'a-1') {
                  await releaseFirstJob
                }
              },
              async stop() {
                // Only signals; the run keeps going until releaseFirstJob settles.
              }
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a'])) // a-1 starts and blocks
      manager.setDesiredJobs(new Set()) // a-1 asked to stop, its run still pending
      manager.setDesiredJobs(new Set(['a'])) // a-2 created, deferred behind a-1's run
      manager.setDesiredJobs(new Set()) // a-2 dropped before it ever started

      releaseFirstJob.resolve()
      await sleep(30)
    })

    it('should never start the replacement that was dropped while it waited', () => {
      expect(started).toEqual(['a-1'])
    })

    it('should report no running jobs', () => {
      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })
  })

  describe('when a job ends on its own', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>

    beforeEach(async () => {
      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'short-lived-manager',
          createJob() {
            return {
              async start() {
                // returns immediately
              },
              async stop() {}
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a']))
      await sleep(20)
    })

    it('should drop it from the running set without needing another setDesiredJobs call', () => {
      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })
  })

  describe('when a job rejects as soon as it starts', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>

    beforeEach(async () => {
      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'failing-start-manager',
          createJob() {
            return {
              async start() {
                throw new Error('the job could not start')
              },
              async stop() {}
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a']))
      await sleep(20)
    })

    it('should swallow the rejection and drop the job from the running set', () => {
      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })

    it('should allow the same name to be created again afterwards', async () => {
      manager.setDesiredJobs(new Set(['a']))
      await sleep(20)

      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })
  })

  describe('when a job throws while the whole manager is stopping', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>

    beforeEach(() => {
      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'failing-stop-manager',
          createJob(name: string) {
            return {
              async start() {
                await sleep(10_000)
              },
              async stop() {
                if (name === 'a') {
                  throw new Error('could not stop a')
                }
              }
            }
          }
        }
      )
    })

    it('should keep stopping the remaining jobs and end with an empty running set', async () => {
      manager.setDesiredJobs(new Set(['a', 'b']))

      await manager.stop!()

      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })
  })
})

describe('createSerialJobRunner', () => {
  let logger: any
  let runner: ReturnType<typeof createSerialJobRunner>

  beforeEach(() => {
    logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    runner = createSerialJobRunner(logger)
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when a job rejects while running', () => {
    let secondJobStarted: boolean

    beforeEach(async () => {
      secondJobStarted = false

      runner.enqueue({
        async start() {
          throw new Error('the job blew up')
        },
        async stop() {}
      })
      runner.enqueue({
        async start() {
          secondJobStarted = true
        },
        async stop() {}
      })

      await sleep(20)
    })

    it('should log the failure', () => {
      expect(logger.error).toHaveBeenCalled()
    })

    it('should still run the next queued job', () => {
      expect(secondJobStarted).toBe(true)
    })
  })

  describe('when a job is enqueued after the runner was stopped', () => {
    let lateJobStarted: boolean

    beforeEach(async () => {
      lateJobStarted = false
      await runner.stop()

      runner.enqueue({
        async start() {
          lateJobStarted = true
        },
        async stop() {}
      })

      await sleep(20)
    })

    it('should ignore it', () => {
      expect(lateJobStarted).toBe(false)
    })

    it('should not count it as queued', () => {
      expect(runner.size()).toBe(0)
    })
  })

  describe('when jobs are queued behind a running one', () => {
    let releaseRunningJob: ReturnType<typeof future<void>>

    beforeEach(async () => {
      releaseRunningJob = future<void>()
      const blockingJob: IJobWithLifecycle = {
        async start() {
          await releaseRunningJob
        },
        async stop() {
          releaseRunningJob.resolve()
        }
      }

      runner.enqueue(blockingJob)
      runner.enqueue({ async start() {}, async stop() {} })
      await sleep(10)
    })

    afterEach(async () => {
      await runner.stop()
    })

    it('should report the running job plus the queued ones', () => {
      expect(runner.size()).toBe(2)
    })
  })
})

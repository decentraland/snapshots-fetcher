import future from 'fp-future'
import { createJobLifecycleManagerComponent } from '../src/job-lifecycle-manager'
import { createSerialJobRunner } from '../src/serial-job-runner'
import { sleep } from '../src/utils'
import { test } from './components'

// Both helpers only *signal* a job to stop; per IJobWithLifecycle the job ends when start() returns.
// If stop() resolves before then, a caller that treats it as "everything has wound down" can still
// have a job mutating state — deploying entities, advancing timestamps — behind its back.

describe('createSerialJobRunner', () => {
  describe('when stop() is called while a job is still inside its action', () => {
    let logger: any
    let runner: ReturnType<typeof createSerialJobRunner>
    let actionFinished: boolean

    beforeEach(async () => {
      logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
      runner = createSerialJobRunner(logger)
      actionFinished = false
      const stopSignalled = future<void>()

      runner.enqueue({
        async start() {
          await stopSignalled
          // The action keeps working for a while after being told to stop, as a real one would while
          // it finishes the request or deployment it is in the middle of.
          await sleep(40)
          actionFinished = true
        },
        async stop() {
          stopSignalled.resolve()
        }
      })

      await sleep(10)
      await runner.stop()
    })

    it('should not resolve until the running action has actually finished', () => {
      expect(actionFinished).toBe(true)
    })
  })
})

test('createJobLifecycleManagerComponent', ({ components }) => {
  describe('when stop() is called while a job is still inside its action', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>
    let finishedActions: string[]

    beforeEach(async () => {
      finishedActions = []

      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'shutdown-manager',
          createJob(name: string) {
            const stopSignalled = future<void>()
            return {
              async start() {
                await stopSignalled
                await sleep(40)
                finishedActions.push(name)
              },
              async stop() {
                stopSignalled.resolve()
              }
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a', 'b']))
      await sleep(10)
      await manager.stop!()
    })

    it('should not resolve until every running action has actually finished', () => {
      expect(finishedActions.sort()).toEqual(['a', 'b'])
    })

    it('should report no running jobs', () => {
      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })
  })

  describe('and a replacement job is waiting behind a predecessor when stop() is called', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>
    let startedInstances: string[]
    let releaseFirst: ReturnType<typeof future<void>>

    beforeEach(async () => {
      startedInstances = []
      releaseFirst = future<void>()
      let instances = 0

      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'stop-during-deferred-start',
          createJob(name: string) {
            const instance = `${name}-${++instances}`
            let jobStopped = false
            return {
              async start() {
                startedInstances.push(instance)
                if (instance === 'a-1') {
                  await releaseFirst
                }
                // Mirrors the real jobs: the action returns promptly once stopped.
                while (!jobStopped) {
                  await sleep(5)
                }
              },
              async stop() {
                jobStopped = true
              }
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a'])) // a-1 starts and blocks
      manager.setDesiredJobs(new Set()) // a-1 signalled, its run still pending
      manager.setDesiredJobs(new Set(['a'])) // a-2 created, deferred behind a-1's run
      releaseFirst.resolve()
      await manager.stop!()
    })

    it('should not leave a job running, and must not hang', () => {
      expect(Array.from(manager.getRunningJobs())).toEqual([])
    })

    it('should never start a replacement it has already stopped', () => {
      // If the name is still in createdJobs while `await job.stop()` yields, the deferred start fires
      // and launches a job that was just stopped — whose run then never settles, hanging stop().
      expect(startedInstances).toEqual(['a-1'])
    })
  })

  describe('and a job was dropped from the desired set before the component was stopped', () => {
    let manager: ReturnType<typeof createJobLifecycleManagerComponent>
    let finishedActions: string[]

    beforeEach(async () => {
      finishedActions = []

      manager = createJobLifecycleManagerComponent(
        { logs: components.logs },
        {
          jobManagerName: 'shutdown-manager',
          createJob(name: string) {
            const stopSignalled = future<void>()
            return {
              async start() {
                await stopSignalled
                await sleep(40)
                finishedActions.push(name)
              },
              async stop() {
                stopSignalled.resolve()
              }
            }
          }
        }
      )

      manager.setDesiredJobs(new Set(['a', 'b']))
      await sleep(10)
      // 'a' is signalled to stop here but its action keeps running.
      manager.setDesiredJobs(new Set(['b']))
      await manager.stop!()
    })

    it('should wait for the dropped job too, not just the ones still desired', () => {
      expect(finishedActions.sort()).toEqual(['a', 'b'])
    })
  })
})

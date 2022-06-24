import { test } from './components'
import { createJobLifecycleManagerComponent } from '../src/job-lifecycle-manager'
import { sleep } from '../src/fetcher'

test('job-manager-1', ({ components }) => {
  let jobManager: ReturnType<typeof createJobLifecycleManagerComponent>
  let emittedEvents: string[] = []

  it('creates the manager', () => {
    jobManager = createJobLifecycleManagerComponent(
      { logs: components.logs },
      {
        jobManagerName: 'test-manager',
        createJob(name: string) {
          let i = 0
          let shouldRun = true
          return {
            async start() {
              while (shouldRun) {
                emittedEvents.push(`${name},${i++}`)
                await sleep(1000)
              }
            },
            async stop() {
              shouldRun = false
            },
          }
        },
      }
    )
  })

  it('creates three jobs', async () => {
    jobManager.setDesiredJobs(new Set(['a', 'b', 'c']))
    expect(Array.from(jobManager.getRunningJobs())).toEqual(['a', 'b', 'c'])
    await sleep(10)
    expect(emittedEvents).toEqual([`a,0`, `b,0`, `c,0`])
    await sleep(1011)
    expect(emittedEvents).toEqual([`a,0`, `b,0`, `c,0`, `a,1`, `b,1`, `c,1`])
  })

  it('creates a new job and deletes two more', async () => {
    jobManager.setDesiredJobs(new Set(['a', 'XXX']))
    expect(Array.from(jobManager.getRunningJobs())).toEqual(['a', 'XXX'])
    expect(emittedEvents).toEqual([`a,0`, `b,0`, `c,0`, `a,1`, `b,1`, `c,1`, 'XXX,0'])
    await sleep(1011)
    expect(emittedEvents).toEqual([`a,0`, `b,0`, `c,0`, `a,1`, `b,1`, `c,1`, 'XXX,0', `a,2`, 'XXX,1'])
  })

  it('kills all the jobs', async () => {
    jobManager.setDesiredJobs(new Set())
    expect(Array.from(jobManager.getRunningJobs())).toEqual([])
    await sleep(1011)
    expect(emittedEvents).toEqual([`a,0`, `b,0`, `c,0`, `a,1`, `b,1`, `c,1`, 'XXX,0', `a,2`, 'XXX,1'])
  })

  afterAll(async () => {
    await jobManager.stop()
  })
})

test('job-manager-stops-all', ({ components }) => {
  let jobManager: ReturnType<typeof createJobLifecycleManagerComponent>
  let emittedEvents: string[] = []

  it('creates the manager', () => {
    jobManager = createJobLifecycleManagerComponent(
      { logs: components.logs },
      {
        jobManagerName: 'test-manager',
        createJob(name: string) {
          let i = 0
          let shouldRun = true
          return {
            async start() {
              while (shouldRun) {
                emittedEvents.push(`${name},${i++}`)
                await sleep(1000)
              }
            },
            async stop() {
              shouldRun = false
            },
          }
        },
      }
    )
  })

  it('stopping the component kills all the jobs', async () => {
    jobManager.setDesiredJobs(new Set(['a', 'b', 'c']))
    expect(Array.from(jobManager.getRunningJobs())).toEqual(['a', 'b', 'c'])
    await jobManager.stop()
    expect(Array.from(jobManager.getRunningJobs())).toEqual([])
  })
})

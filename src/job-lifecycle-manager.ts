import { IBaseComponent } from '@well-known-components/interfaces'
import { SnapshotsFetcherComponents } from './types'

export type JobLifecycleManagerComponent = {
  setDesiredJobs(desiredJobNames: Set<string>): void
  getRunningJobs(): Set<string>
}
export type IJobWithLifecycle = {
  // once start() finishes, the job ends
  start(): Promise<void>
  // should trigger the signal to end the job
  stop(): Promise<void>
}
export type JobLifecycleManagerOptions = {
  jobManagerName: string
  createJob(jobName: string): IJobWithLifecycle
}

/**
 * Creates a component that handles a list of running jobs.
 *
 * Every time setDesiredJobs is called, the component will try to
 * create an asynchronous job for each of the given names.
 *
 * Once a job ends, it can be manually recreated by calling setDesiredJobs again.
 *
 * It is recommended that if a job needs to be persistent, that the job
 * itself should control its own core-loop and handle its exceptions.
 */
export function createJobLifecycleManagerComponent(
  components: Pick<SnapshotsFetcherComponents, 'logs'>,
  options: JobLifecycleManagerOptions
): IBaseComponent & JobLifecycleManagerComponent {
  const logs = components.logs.getLogger(options.jobManagerName)

  const createdJobs = new Map<string, IJobWithLifecycle>()
  // Tracks each name's in-flight run, so a replacement for that name waits for its predecessor to
  // wind down instead of running alongside it. It keys off start(), which per IJobWithLifecycle is
  // where a job actually ends — stop() is only the signal to end and can resolve while the job is
  // still finishing.
  const runningJobs = new Map<string, Promise<void>>()

  return {
    setDesiredJobs(desiredJobNames: Set<string>): void {
      // first stop all the jobs that are not part of the desiredJobNames
      // and remove them from the map of running jobs
      for (const [name, job] of createdJobs) {
        if (!desiredJobNames.has(name)) {
          logs.info('Stopping job', { name })
          job.stop().catch((err) => logs.error(err))
          createdJobs.delete(name)
          // Its entry in runningJobs stays until start() returns; a replacement chains onto it.
        }
      }

      // then create the jobs for the new desired set
      for (const name of desiredJobNames) {
        if (!createdJobs.has(name)) {
          logs.info('Creating job', { name })
          const job = options.createJob(name)
          createdJobs.set(name, job)

          const startJob = () => {
            // A newer setDesiredJobs may already have replaced or removed this job while it waited.
            if (createdJobs.get(name) !== job) {
              return
            }
            return job
              .start()
              .catch((err) => logs.error(err))
              .finally(() => {
                // then remove it from the list of running jobs after it ends
                if (createdJobs.get(name) === job) {
                  logs.info('Job finished', { name })
                  createdJobs.delete(name)
                }
              })
          }

          // Start synchronously when no previous run of this name is still winding down, so callers
          // observing the manager right after setDesiredJobs see the job already running.
          const previousRun = runningJobs.get(name)
          const run = previousRun ? previousRun.then(startJob) : startJob()

          const settledRun = Promise.resolve(run).then(
            () => undefined,
            () => undefined
          )
          runningJobs.set(name, settledRun)
          void settledRun.then(() => {
            if (runningJobs.get(name) === settledRun) {
              runningJobs.delete(name)
            }
          })
        }
      }
    },
    getRunningJobs() {
      return new Set(createdJobs.keys())
    },
    async stop() {
      for (const [name, job] of createdJobs) {
        logs.info('Stopping job', { name })
        try {
          await job.stop()
        } catch (e: any) {
          logs.error(e)
        }
        createdJobs.delete(name)
      }
    }
  }
}

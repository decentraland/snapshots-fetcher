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

  return {
    setDesiredJobs(desiredJobNames: Set<string>): void {
      // first stop all the jobs that are not part of the desiredJobNames
      // and remove them from the map of running jobs
      for (const [name, job] of createdJobs) {
        if (!desiredJobNames.has(name)) {
          logs.info('Stopping job', { name })
          job.stop().catch(logs.error)
          createdJobs.delete(name)
        }
      }

      // then create the jobs for the new desired set
      for (const name of desiredJobNames) {
        if (!createdJobs.has(name)) {
          logs.info('Creating job', { name })
          const job = options.createJob(name)
          createdJobs.set(name, job)
          job
            .start()
            .catch(logs.error)
            .finally(() => {
              // then remove it from the list of running jobs after it ends
              if (createdJobs.get(name) === job) {
                logs.info('Job finished', { name })
                createdJobs.delete(name)
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

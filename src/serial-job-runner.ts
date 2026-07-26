import { ILoggerComponent } from '@well-known-components/interfaces'
import { IJobWithLifecycle } from './job-lifecycle-manager'

/**
 * Runs jobs one at a time in FIFO order. Enqueuing while a job is running just appends; the running
 * chain re-arms itself in each job's `finally`, so the queue always drains regardless of how many
 * jobs were enqueued mid-flight.
 */
export type SerialJobRunner = {
  /** Append a job. Starts it immediately if nothing else is queued/running. */
  enqueue(job: IJobWithLifecycle): void
  /** Number of jobs queued, including the one currently running. */
  size(): number
  /** Stop every queued job (the running one included); no further jobs will start. */
  stop(): Promise<void>
}

export function createSerialJobRunner(logger: ILoggerComponent.ILogger): SerialJobRunner {
  const jobs: IJobWithLifecycle[] = []
  let stopped = false
  // The run of the job currently executing, so stop() can wait for it to actually finish rather than
  // merely signalling it. Undefined whenever nothing is running.
  let currentRun: Promise<void> | undefined

  function startNext() {
    if (stopped || jobs.length === 0) {
      return
    }
    currentRun = jobs[0]
      .start()
      .catch((err) => logger.error(err))
      .finally(() => {
        jobs.shift()
        currentRun = undefined
        startNext()
      })
  }

  return {
    enqueue(job: IJobWithLifecycle) {
      if (stopped) {
        return
      }
      jobs.push(job)
      // Only kick off the chain when this is the sole queued job; otherwise the currently-running
      // chain picks it up when it finishes.
      if (jobs.length === 1) {
        startNext()
      }
    },
    size() {
      return jobs.length
    },
    async stop() {
      stopped = true
      // Stop every queued job, not just the running one: a job that is dropped without being
      // stopped never gets to settle whatever it handed its caller (e.g. a completion future), and
      // the caller would wait on it forever.
      const queuedJobs = jobs.slice()
      jobs.length = 0
      for (const job of queuedJobs) {
        try {
          await job.stop()
        } catch (err: any) {
          logger.error(err)
        }
      }

      // stop() above only raises the signal; the running job ends when its start() returns. Waiting
      // for it here is what lets a caller treat a resolved stop() as "nothing is running any more".
      // `stopped` already prevents startNext from picking up anything else.
      await currentRun
    }
  }
}

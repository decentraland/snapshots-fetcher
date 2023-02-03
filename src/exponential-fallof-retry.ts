import { ILoggerComponent } from '@well-known-components/interfaces'
import { IJobWithLifecycle } from './job-lifecycle-manager'
import { sleep } from './utils'

/**
 * @public
 */
export type ExponentialFallofRetryComponent = IJobWithLifecycle & {
  getRetryCount(): number
  isStopped(): boolean
}

/**
 * @public
 */
export type ExponentialFallofRetryOptions = {
  retryTime: number
  /**
   * @default 1.1
   */
  retryTimeExponent?: number
  action: () => Promise<void>
  /**
   * Maximum falloff interval in milliseconds.
   * @default 86_400_000 one day
   */
  maxInterval?: number
  exitOnSuccess?: boolean
}

/**
 * Creates a component that executes long living tasks over and over until the component is stopped.
 *
 * Retries are exponential and configurable.
 * @public
 */
export function createExponentialFallofRetry(
  logs: ILoggerComponent.ILogger,
  options: ExponentialFallofRetryOptions
): ExponentialFallofRetryComponent {
  let started: boolean = false

  if (options.maxInterval && options.maxInterval < 0) throw new Error('options.maxInterval must be >= 0')

  const exitOnSuccess = options.exitOnSuccess || false

  let reconnectionCount = 0

  async function start() {
    // reset reconnection time
    let reconnectionTime = options.retryTime

    while (true) {
      logs.info('Starting...')
      reconnectionCount++

      try {
        await options.action()
        if (exitOnSuccess) {
          logs.info('Breaking iteration. Action ended successfully')
          return
        }
      } catch (e: any) {
        logs.error(e)
        // increment reconnection time
        reconnectionTime = reconnectionTime * (options.retryTimeExponent ?? 1.1)
        if (options.maxInterval) {
          reconnectionTime = Math.min(reconnectionTime, options.maxInterval)
        }
      }

      if (!started) {
        // break iterator if closed
        logs.info('Breaking iteration, started == false')
        return
      }

      if (!options.retryTime) {
        // break iterator if no retryTime was set
        logs.info('Not iterating due to missing or zero options.retryTime')
        return
      }

      if (options.maxInterval) {
        reconnectionTime = Math.min(reconnectionTime, options.maxInterval)
      } else {
        reconnectionTime = Math.min(reconnectionTime, 86_400_000 /* one day */)
      }

      logs.info('Retrying in ' + reconnectionTime.toFixed(1) + 'ms')
      await sleep(reconnectionTime)
    }
  }

  return {
    getRetryCount() {
      return reconnectionCount
    },
    isStopped() {
      return !started
    },
    async start() {
      if (started === true) return
      started = true
      await start()
    },
    async stop() {
      started = false
    }
  }
}

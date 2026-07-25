import { ILoggerComponent } from '@well-known-components/interfaces'
import { IJobWithLifecycle } from './job-lifecycle-manager'

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
  /**
   * When the action fails *after* running for at least this many milliseconds, the run is treated as
   * healthy and the retry interval goes back to `retryTime` instead of growing.
   *
   * Set this for long-lived actions that only ever return by failing (e.g. a polling stream). Without
   * it, such an action inherits the interval grown by failures from hours or days earlier and
   * reconnects at the maxed-out delay even though it had been perfectly healthy in between.
   *
   * When unset, only a clean completion resets the interval — which is the right default for
   * short actions whose normal duration may exceed `retryTime`.
   */
  healthyRunTime?: number
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
  // A negative retryTime passes the `!options.retryTime` guard below and then makes every retry sleep
  // resolve immediately, turning the loop into a busy spin. Zero is allowed: it means "do not retry".
  if (options.retryTime < 0) throw new Error('options.retryTime must be >= 0')
  if (options.healthyRunTime !== undefined && options.healthyRunTime < 0) {
    throw new Error('options.healthyRunTime must be >= 0')
  }

  const exitOnSuccess = options.exitOnSuccess || false

  let reconnectionCount = 0

  // Allows stop() to interrupt an in-flight retry sleep instead of waiting out the full (possibly
  // multi-day) interval before the loop notices it was stopped.
  let cancelCurrentSleep: (() => void) | undefined

  function interruptibleSleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      if (ms <= 0) {
        resolve()
        return
      }
      const timeout = setTimeout(() => {
        cancelCurrentSleep = undefined
        resolve()
      }, ms)
      cancelCurrentSleep = () => {
        clearTimeout(timeout)
        cancelCurrentSleep = undefined
        resolve()
      }
    })
  }

  async function start() {
    // reset reconnection time
    let reconnectionTime = options.retryTime

    while (true) {
      logs.info('Starting...')
      reconnectionCount++

      const actionStartedAt = Date.now()
      let actionFailed = false

      try {
        await options.action()
        if (exitOnSuccess) {
          logs.info('Breaking iteration. Action ended successfully')
          return
        }
      } catch (e: any) {
        logs.error(e)
        actionFailed = true
      }

      // A run counts as healthy when the action completed without throwing, or when it stayed up for
      // at least healthyRunTime before failing. Only an unhealthy run grows the interval; otherwise
      // the backoff would never come back down after a recovery.
      const runWasHealthy =
        !actionFailed ||
        (options.healthyRunTime !== undefined && Date.now() - actionStartedAt >= options.healthyRunTime)

      if (runWasHealthy) {
        reconnectionTime = options.retryTime
      } else {
        reconnectionTime = reconnectionTime * (options.retryTimeExponent ?? 1.1)
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
      await interruptibleSleep(reconnectionTime)
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
      try {
        await start()
      } finally {
        // Reset so isStopped() is accurate once the loop exits (e.g. exitOnSuccess) and the
        // component can be started again.
        started = false
      }
    },
    async stop() {
      started = false
      if (cancelCurrentSleep) {
        cancelCurrentSleep()
      }
    }
  }
}

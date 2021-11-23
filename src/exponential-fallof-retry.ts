import { ILoggerComponent } from '@well-known-components/interfaces'
import { sleep } from './utils'

/**
 * @public
 */
export type ExponentialFallofRetryComponent = {
  getRetryCount(): number
  isStopped(): boolean
  start(): Promise<void>
  stop(): Promise<void>
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
}

/**
 * Creates a component that executes an action over and over until it is stopped.
 * Retries are exponential.
 * @public
 */
export function createExponentialFallofRetry(
  logs: ILoggerComponent.ILogger,
  options: ExponentialFallofRetryOptions
): ExponentialFallofRetryComponent {
  let started: boolean = false

  let reconnectionCount = 0

  async function start() {
    // reset reconnection time
    let reconnectionTime = options.retryTime

    while (true) {
      logs.info('Starting...')
      reconnectionCount++

      try {
        await options.action()
      } catch (e: any) {
        logs.error(e)
        // increment reconnection time
        reconnectionTime = reconnectionTime * (options.retryTimeExponent ?? 1.1)
      }

      if (!started) {
        // break iterator if closed
        return
      }

      if (!options.retryTime) {
        // break iterator if no retryTime was set
        return
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
      return started != true
    },
    async start() {
      if (started === true) return
      started = true
      start().catch(logs.error /* this should never be executed */)
    },
    async stop() {
      started = false
    },
  }
}

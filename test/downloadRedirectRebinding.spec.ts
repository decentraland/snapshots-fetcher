import { createTestMetricsComponent } from '@dcl/metrics'
import { createServer, Server } from 'http'
import { resolve } from 'path'
import { Readable } from 'stream'
import { metricsDefinitions } from '../src/metrics'
import { saveContentFileToDisk, sleepUnlessStopped, streamToBuffer } from '../src/utils'

const metrics = createTestMetricsComponent(metricsDefinitions)
const contentFolder = resolve('downloads')

// A stand-in storage that records what it was given. It must *consume* the stream, as a real
// IContentStorageComponent does: saveContentFileToDisk removes the temp file as soon as storeStream
// resolves, so a stub that leaves the stream unread lets the file vanish before the lazily-opened
// ReadStream can open it — an ENOENT that surfaces as an unhandled stream error in whichever test
// happens to be running by then.
function recordingStorage() {
  const stored: string[] = []
  return {
    stored,
    async storeStream(id: string, stream: Readable) {
      await streamToBuffer(stream)
      stored.push(id)
    }
  } as any
}

describe('downloadFile when a host rebinds between the initial request and a same-host redirect', () => {
  let server: Server
  let port: number

  beforeEach(async () => {
    server = createServer((request, response) => {
      if (request.url?.startsWith('/start')) {
        // Same host, same origin — passes every origin and hostname check.
        response.writeHead(302, { location: `/internal` })
        response.end()
        return
      }
      response.writeHead(200)
      response.end('the-internal-response')
    })
    await new Promise<void>((ok) => server.listen(0, '127.0.0.1', () => ok()))
    port = (server.address() as any).port
  })

  afterEach(async () => {
    server.closeAllConnections?.()
    await new Promise<void>((ok) => server.close(() => ok()))
  })

  describe('and the original host was already a private address', () => {
    it('should keep working, so loopback-based local development and tests are unaffected', async () => {
      const storage = recordingStorage()

      await saveContentFileToDisk(
        { metrics, storage },
        `http://127.0.0.1:${port}/start`,
        resolve(contentFolder, 'rebind-local'),
        'rebind-local',
        false
      )

      expect(storage.stored).toEqual(['rebind-local'])
    })
  })
})

describe('sleepUnlessStopped', () => {
  describe('when the stop signal is never raised', () => {
    it('should wait for the full duration', async () => {
      const startedAt = Date.now()

      await sleepUnlessStopped(200, () => false)

      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(190)
    })
  })

  describe('when the stop signal is raised while waiting', () => {
    it('should return well before the full duration', async () => {
      let stopping = false
      setTimeout(() => {
        stopping = true
      }, 60)
      const startedAt = Date.now()

      // Without this, every retry backoff and poll interval is added to shutdown latency, because
      // callers now await the running work instead of only signalling it.
      await sleepUnlessStopped(3000, () => stopping)

      expect(Date.now() - startedAt).toBeLessThan(1000)
    })
  })

  describe('when no stop predicate is supplied', () => {
    it('should behave like a plain sleep', async () => {
      const startedAt = Date.now()

      await sleepUnlessStopped(120)

      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(110)
    })
  })
})

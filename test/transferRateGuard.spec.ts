import { Readable } from 'stream'
import { pipeline as streamPipeline } from 'stream/promises'
import { createTransferLimiter, tooSlowToContinue } from '../src/utils'

// The guard measures against Date.now(), and the grace period is a minute — far longer than a test
// should take. Advancing a stubbed clock is what lets the real code paths be exercised in milliseconds.
function withClock(): { advanceBy(ms: number): void; restore(): void } {
  const realNow = Date.now()
  let offset = 0
  const spy = jest.spyOn(Date, 'now').mockImplementation(() => realNow + offset)
  return {
    advanceBy(ms: number) {
      offset += ms
    },
    restore() {
      spy.mockRestore()
    }
  }
}

describe('tooSlowToContinue', () => {
  let clock: ReturnType<typeof withClock>
  let startedAt: number

  beforeEach(() => {
    clock = withClock()
    startedAt = Date.now()
  })

  afterEach(() => {
    clock.restore()
  })

  describe('when the transfer is still inside the grace period', () => {
    beforeEach(() => {
      clock.advanceBy(59_000)
    })

    it('should allow a transfer that has sent almost nothing, since setup dominates early samples', () => {
      expect(tooSlowToContinue(1, startedAt)).toBeUndefined()
    })
  })

  describe('when a peer has trickled one byte per inactivity window past the grace period', () => {
    let error: Error | undefined

    beforeEach(() => {
      // Three windows of 30s: every one refreshes the inactivity deadline, so this peer looks alive to
      // every check that existed before the rate floor.
      clock.advanceBy(90_000)
      error = tooSlowToContinue(3, startedAt)
    })

    it('should refuse to continue', () => {
      expect(error).toBeDefined()
    })

    it('should report the bytes, the measured rate and the floor it fell below', () => {
      expect(error!.message).toEqual(
        'Transfer of 3 bytes averaged 0.03 bytes/s over 90s, below the minimum of 4096 bytes/s'
      )
    })
  })

  describe('when a slow but genuine transfer is making steady progress past the grace period', () => {
    beforeEach(() => {
      clock.advanceBy(120_000)
    })

    it('should allow a rate above the floor', () => {
      // 120s at ~8 KiB/s: half the speed of a dial-up modem is still allowed through.
      expect(tooSlowToContinue(8 * 1024 * 120, startedAt)).toBeUndefined()
    })

    it('should allow a rate sitting exactly on the floor', () => {
      expect(tooSlowToContinue(4 * 1024 * 120, startedAt)).toBeUndefined()
    })
  })
})

describe('createTransferLimiter', () => {
  let clock: ReturnType<typeof withClock>

  beforeEach(() => {
    clock = withClock()
  })

  afterEach(() => {
    clock.restore()
  })

  describe('when the source trickles bytes slowly enough to stay under the floor', () => {
    it('should destroy the pipeline rather than let it hold its slot indefinitely', async () => {
      const limiter = createTransferLimiter(1024 * 1024)
      // Each chunk arrives well within the 30s inactivity deadline, so only the rate floor can stop it.
      const trickle = Readable.from(
        (async function* () {
          for (let chunk = 0; chunk < 5; chunk++) {
            clock.advanceBy(25_000)
            yield Buffer.from('x')
          }
        })()
      )

      await expect(streamPipeline(trickle, limiter, async function* () {})).rejects.toThrow(
        'below the minimum of 4096 bytes/s'
      )
    })
  })

  describe('when the source delivers a normal payload promptly', () => {
    let received: number

    beforeEach(() => {
      received = 0
    })

    it('should pass every byte through untouched', async () => {
      const limiter = createTransferLimiter(1024 * 1024)
      const payload = Buffer.alloc(64 * 1024, 7)

      await streamPipeline(Readable.from([payload]), limiter, async function (source) {
        for await (const chunk of source) {
          received += chunk.length
        }
      })

      expect(received).toEqual(payload.length)
    })
  })

  describe('when the payload exceeds the size ceiling', () => {
    it('should still report the size failure rather than the rate one', async () => {
      const limiter = createTransferLimiter(10)

      await expect(
        streamPipeline(Readable.from([Buffer.alloc(64)]), limiter, async function* () {})
      ).rejects.toThrow('exceeds the maximum allowed size of 10 bytes')
    })
  })
})

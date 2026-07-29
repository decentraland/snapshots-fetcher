import { createRedirectSafeLookup } from '../src/utils'

// require, not `import * as`: ts-jest gives an ES namespace object whose properties are getter-only, so
// the stub below cannot be assigned onto it. The module object from require is the one src/utils reads.
const dns: any = require('dns')

// The guard's decisions depend on what DNS answers, which is impractical to arrange through a real
// download — the existing rebinding test uses an IP literal and so never reaches the lookup at all.
// Driving the returned LookupFunction directly with fabricated answer sets covers it without a socket.
// Assigned rather than jest.spyOn'd: dns.lookup is writable but not configurable, so defineProperty —
// which is what spyOn uses — fails with "Cannot redefine property".
const realLookup = dns.lookup

function withAnswers(answers: Array<Array<{ address: string; family: number }>>) {
  let call = 0
  ;(dns as any).lookup = (_hostname: string, _options: any, callback: any) => {
    const answer = answers[Math.min(call, answers.length - 1)]
    call++
    callback(null, answer, answer[0].family)
  }
}

function resolveOnce(lookup: ReturnType<typeof createRedirectSafeLookup>, hostname: string): Promise<Error | null> {
  return new Promise((ok) => {
    ;(lookup as any)(hostname, { all: true }, (error: Error | null) => ok(error))
  })
}

describe('createRedirectSafeLookup', () => {
  afterEach(() => {
    ;(dns as any).lookup = realLookup
  })

  describe('when the allowed host resolves to both a public and a non-public address', () => {
    let firstLookupError: Error | null

    beforeEach(async () => {
      withAnswers([
        [
          { address: '203.0.113.10', family: 4 },
          { address: '127.0.0.1', family: 4 }
        ]
      ])
      firstLookupError = await resolveOnce(createRedirectSafeLookup('mixed.example'), 'mixed.example')
    })

    it('should refuse the ambiguity rather than pick a classification to hold', () => {
      // Recording "contains a non-public address" was exploitable: the connection may use the public
      // address while the guard remembers "non-public", after which a same-host redirect resolving to
      // ONLY loopback matches the recorded value and is allowed through.
      expect(firstLookupError?.message).toContain('mix of public and non-public addresses')
    })
  })

  describe('and the mixed answer set is returned again', () => {
    let errors: Array<Error | null>

    beforeEach(async () => {
      withAnswers([
        [
          { address: '203.0.113.10', family: 4 },
          { address: '127.0.0.1', family: 4 }
        ]
      ])
      const lookup = createRedirectSafeLookup('mixed.example')
      errors = [await resolveOnce(lookup, 'mixed.example'), await resolveOnce(lookup, 'mixed.example')]
    })

    it('should keep refusing, having recorded no classification from an ambiguous answer', () => {
      // This is what makes the reported pivot unreachable rather than merely blocked once. The download
      // aborts on the first refusal, so there is no second request to redirect — and because nothing was
      // recorded, no later same-host answer has a poisoned value to match against.
      expect(errors.every((error) => error?.message.includes('mix of public and non-public'))).toBe(true)
    })
  })

  describe('when the allowed host resolves only to public addresses throughout', () => {
    let errors: Array<Error | null>

    beforeEach(async () => {
      withAnswers([[{ address: '203.0.113.10', family: 4 }], [{ address: '203.0.113.11', family: 4 }]])
      const lookup = createRedirectSafeLookup('public.example')
      errors = [await resolveOnce(lookup, 'public.example'), await resolveOnce(lookup, 'public.example')]
    })

    it('should allow both, so a round-robin CDN handing out a different public IP still works', () => {
      expect(errors).toEqual([null, null])
    })
  })

  describe('when the allowed host resolves only to non-public addresses throughout', () => {
    let errors: Array<Error | null>

    beforeEach(async () => {
      withAnswers([[{ address: '127.0.0.1', family: 4 }], [{ address: '127.0.0.1', family: 4 }]])
      const lookup = createRedirectSafeLookup('local.example')
      errors = [await resolveOnce(lookup, 'local.example'), await resolveOnce(lookup, 'local.example')]
    })

    it('should allow both, so a private catalyst and local development keep working', () => {
      expect(errors).toEqual([null, null])
    })
  })

  describe('when the allowed host was public and a same-host redirect resolves to loopback', () => {
    let redirectError: Error | null

    beforeEach(async () => {
      withAnswers([[{ address: '203.0.113.10', family: 4 }], [{ address: '127.0.0.1', family: 4 }]])
      const lookup = createRedirectSafeLookup('rebinding.example')
      await resolveOnce(lookup, 'rebinding.example')
      redirectError = await resolveOnce(lookup, 'rebinding.example')
    })

    it('should refuse it as a classification change', () => {
      expect(redirectError?.message).toContain('unlike the original request')
    })
  })

  describe('when a redirect points at a different host that resolves to a non-public address', () => {
    let redirectError: Error | null

    beforeEach(async () => {
      withAnswers([[{ address: '203.0.113.10', family: 4 }], [{ address: '169.254.169.254', family: 4 }]])
      const lookup = createRedirectSafeLookup('allowed.example')
      await resolveOnce(lookup, 'allowed.example')
      redirectError = await resolveOnce(lookup, 'metadata.example')
    })

    it('should refuse it regardless of the allowed host classification', () => {
      expect(redirectError?.message).toContain('not a public address')
    })
  })
})

import { fetchJsonPaginated } from '../src/client'
import { test } from './components'

async function drain<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const collected: T[] = []
  for await (const element of iterable) {
    collected.push(element)
  }
  return collected
}

test('fetchJsonPaginated when the server returns a malformed or endless feed', ({ components }) => {
  let pagesServed: number

  it('prepares the endpoints', () => {
    pagesServed = 0

    // Points `next` back at itself, so a caller that trusts the link paginates forever.
    components.router.get('/endless', async (): Promise<any> => {
      pagesServed++
      return { body: { deltas: [], pagination: { next: '/endless' } } }
    })
    // Two pages that point at each other. They differ only by query string, because a `next` that
    // changes the path is refused before the cycle detection could ever see it.
    components.router.get('/cycle', async (): Promise<any> => {
      pagesServed++
      return { body: { deltas: [], pagination: { next: pagesServed % 2 === 1 ? '?p=b' : '?p=a' } } }
    })
    components.router.get('/null-deltas', async (): Promise<any> => ({ body: { deltas: null, pagination: {} } }))
    components.router.get('/null-body', async (): Promise<any> => ({
      body: 'null',
      headers: { 'content-type': 'application/json' }
    }))
    components.router.get('/two-pages', async (): Promise<any> => {
      pagesServed++
      return pagesServed === 1
        ? { body: { deltas: ['first'], pagination: { next: '/two-pages?page=2' } } }
        : { body: { deltas: ['second'], pagination: {} } }
    })
  })

  describe('when a page links back to itself', () => {
    let baseUrl: string

    beforeEach(async () => {
      pagesServed = 0
      baseUrl = await components.getBaseUrl()
    })

    it('should reject with a pagination loop error instead of fetching forever', async () => {
      await expect(
        drain(fetchJsonPaginated(components, `${baseUrl}/endless`, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds'))
      ).rejects.toThrow('Pagination loop')
    })

    it('should stop after serving only the first page', async () => {
      await expect(
        drain(fetchJsonPaginated(components, `${baseUrl}/endless`, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds'))
      ).rejects.toThrow()

      expect(pagesServed).toBe(1)
    })
  })

  describe('and two pages point at each other', () => {
    let baseUrl: string

    beforeEach(async () => {
      pagesServed = 0
      baseUrl = await components.getBaseUrl()
    })

    it('should detect the cycle after revisiting the first page', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/cycle?p=a`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('Pagination loop')

      expect(pagesServed).toBe(2)
    })
  })

  describe('and the element list is not an array', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject naming the expected shape instead of throwing a TypeError', async () => {
      await expect(
        drain(
          fetchJsonPaginated(components, `${baseUrl}/null-deltas`, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds')
        )
      ).rejects.toThrow('expected an array of elements')
    })
  })

  describe('and the body is a bare null document', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
    })

    it('should reject naming the expected shape instead of throwing a TypeError', async () => {
      await expect(
        drain(
          fetchJsonPaginated(components, `${baseUrl}/null-body`, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds')
        )
      ).rejects.toThrow('expected a JSON object')
    })
  })

  describe('and the pagination link points at another origin', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      // A remote server steering us at an internal address, using this process as the client.
      components.router.get('/cross-origin', async (): Promise<any> => ({
        body: {
          deltas: [],
          pagination: { next: 'http://169.254.169.254/latest/meta-data/' }
        }
      }))
    })

    it('should refuse to follow it rather than issue a request to that host', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/cross-origin`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('Refusing to follow a cross-origin pagination link')
    })
  })

  describe('and the pagination link keeps the origin but points at another path', () => {
    let baseUrl: string
    let pivotWasRequested: boolean

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      pivotWasRequested = false
      // Same host and port, so the origin check passes. This is what a hostile server needs in order to
      // aim a rebound address at an endpoint of its choosing rather than the one we meant to poll:
      // rebinding itself cannot be defended against on this path (IFetchComponent exposes no DNS
      // lookup), but the path is ours to hold fixed.
      components.router.get('/path-pivot', async (): Promise<any> => ({
        body: { deltas: [], pagination: { next: '/internal-admin?x=1' } }
      }))
      components.router.get('/internal-admin', async (): Promise<any> => {
        pivotWasRequested = true
        return { body: { deltas: [], pagination: {} } }
      })
    })

    it('should refuse to follow it', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/path-pivot`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('Refusing to follow a pagination link that changes the path')
    })

    it('should never issue a request to the path the server chose', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/path-pivot`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow()

      expect(pivotWasRequested).toBe(false)
    })
  })

  describe('and the pagination link varies only the query string', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      pagesServed = 0
      // The shape real catalysts return: `?from=…&entityId=…` against the same endpoint.
      components.router.get('/query-only', async (): Promise<any> => {
        pagesServed++
        return pagesServed === 1
          ? { body: { deltas: ['page1'], pagination: { next: '?from=11&entityId=ba00' } } }
          : { body: { deltas: ['page2'], pagination: {} } }
      })
    })

    it('should follow it, since pagination legitimately only moves the query', async () => {
      const elements = await drain(
        fetchJsonPaginated(
          components,
          `${baseUrl}/query-only?from=0`,
          ($) => $.deltas,
          'dcl_catalysts_pointer_changes_response_time_seconds'
        )
      )

      expect(elements).toEqual(['page1', 'page2'])
    })
  })

  describe('and the response carries no pagination key at all', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      components.router.get('/no-pagination', async (): Promise<any> => ({ body: { deltas: ['only'] } }))
    })

    it('should yield its elements and stop after the single page', async () => {
      const elements = await drain(
        fetchJsonPaginated(
          components,
          `${baseUrl}/no-pagination`,
          ($) => $.deltas,
          'dcl_catalysts_pointer_changes_response_time_seconds'
        )
      )

      expect(elements).toEqual(['only'])
    })
  })

  describe('and the feed paginates normally', () => {
    let baseUrl: string

    beforeEach(async () => {
      pagesServed = 0
      baseUrl = await components.getBaseUrl()
    })

    it('should still follow every distinct page and yield all elements', async () => {
      const elements = await drain(
        fetchJsonPaginated(components, `${baseUrl}/two-pages`, ($) => $.deltas, 'dcl_catalysts_pointer_changes_response_time_seconds')
      )

      expect(elements).toEqual(['first', 'second'])
    })
  })
  describe.each([
    ['a number', 123],
    ['an object', { href: '/next' }],
    ['a boolean', true]
  ])('and the pagination link is %s instead of a string', (_label: string, next: unknown) => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      components.router.get('/wrong-typed-next', async (): Promise<any> => ({
        body: { deltas: ['only'], pagination: { next } }
      }))
    })

    it('should reject rather than read it as the end of the feed', async () => {
      // Every other malformed link here already fails — too long, unparseable, cross-origin,
      // path-changing. Treating a wrong type as an ending made a truncated feed indistinguishable from a
      // complete one.
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/wrong-typed-next`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('expected a string')
    })
  })

  describe.each([
    ['absent', undefined],
    ['null', null],
    ['an empty string', '']
  ])('and the pagination link is %s', (_label: string, next: unknown) => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      components.router.get('/no-next', async (): Promise<any> => ({
        body: { deltas: ['only'], pagination: { next } }
      }))
    })

    it('should treat it as the end of the feed', async () => {
      const elements = await drain(
        fetchJsonPaginated(
          components,
          `${baseUrl}/no-next`,
          ($) => $.deltas,
          'dcl_catalysts_pointer_changes_response_time_seconds'
        )
      )

      expect(elements).toEqual(['only'])
    })
  })
  describe.each([
    ['true', true],
    ['a string', 'more'],
    ['a number', 42],
    ['an array', ['?from=2']]
  ])('and the pagination container is %s instead of an object', (_label: string, pagination: unknown) => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      components.router.get('/wrong-typed-pagination', async (): Promise<any> => ({
        body: { deltas: ['only'], pagination }
      }))
    })

    it('should reject rather than reading a container it cannot parse as the end of the feed', async () => {
      // Reading `.next` off `true` or `"more"` yields undefined, which the truthiness check then ended the
      // feed on — the same "a shape we cannot read means there is no more" hazard as a wrong-typed link.
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/wrong-typed-pagination`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('expected an object')
    })
  })

  describe('and the pagination container is an empty object', () => {
    let baseUrl: string

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      components.router.get('/empty-pagination', async (): Promise<any> => ({
        body: { deltas: ['only'], pagination: {} }
      }))
    })

    it('should treat it as the end of the feed, which is how a real last page looks', async () => {
      const elements = await drain(
        fetchJsonPaginated(
          components,
          `${baseUrl}/empty-pagination`,
          ($) => $.deltas,
          'dcl_catalysts_pointer_changes_response_time_seconds'
        )
      )

      expect(elements).toEqual(['only'])
    })
  })
  describe('and the pagination link differs from the current page only by fragment', () => {
    let baseUrl: string
    let pagesServed: number

    beforeEach(async () => {
      baseUrl = await components.getBaseUrl()
      pagesServed = 0
      // Fragments are never sent to a server, so every one of these fetches the same network resource —
      // but as distinct strings they used to slip past the visited-URL check.
      components.router.get('/fragment-loop', async (): Promise<any> => {
        pagesServed++
        return { body: { deltas: [], pagination: { next: `?page=1#frag${pagesServed}` } } }
      })
    })

    it('should detect it as a loop instead of re-fetching the same page', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/fragment-loop?page=1`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow('Pagination loop')
    })

    it('should stop after re-fetching it at most once, not up to the page cap', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/fragment-loop?page=1`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds'
          )
        )
      ).rejects.toThrow()

      expect(pagesServed).toBeLessThanOrEqual(2)
    })
  })
})

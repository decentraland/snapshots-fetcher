import { fetchJsonPaginated } from '../src/client'
import { test } from './components'

async function drain<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const collected: T[] = []
  for await (const element of iterable) {
    collected.push(element)
  }
  return collected
}

test('fetchJsonPaginated when a server keeps advertising a next link', ({ components }) => {
  let pagesServed: number
  let baseUrl: string

  beforeEach(async () => {
    pagesServed = 0
    baseUrl = await components.getBaseUrl()
    components.router.get('/endless', async (): Promise<any> => {
      pagesServed++
      // Each page names a URL nothing has fetched yet, so the visited-URL loop check never fires and only
      // the page cap can stop the walk.
      return { body: { deltas: [], pagination: { next: `?page=${pagesServed}` } } }
    })
  })

  describe('and the caller lowers the page cap', () => {
    it('should stop at the configured cap rather than the 10,000 default', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/endless?page=0`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds',
            { maxPagesPerPaginatedCall: 5 }
          )
        )
      ).rejects.toThrow('stopped after 5')
    })

    it('should have made only that many requests', async () => {
      await expect(
        drain(
          fetchJsonPaginated(
            components,
            `${baseUrl}/endless?page=0`,
            ($) => $.deltas,
            'dcl_catalysts_pointer_changes_response_time_seconds',
            { maxPagesPerPaginatedCall: 5 }
          )
        )
      ).rejects.toThrow()

      // The cap bounds the amplification a single poll can produce, which is the whole point of making it
      // configurable: an operator who cannot tolerate 10,000 same-path requests per poll can say so.
      expect(pagesServed).toEqual(5)
    })
  })
})

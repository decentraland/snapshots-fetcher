import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { assertHash } from '../src/utils'
import { downloadEntityAndContentFiles } from '../src'

test('entities', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')

  it('cleanup files', () => {
    try {
      unlinkSync(resolve(contentFolder, 'Qma14gteWwHrn61kv4zkRABKzopWd5ZXBppHRhbqikfaev'))
      unlinkSync(resolve(contentFolder, 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'))
      unlinkSync(resolve(contentFolder, 'QmUiEzCQPxz5eHq7KXMrGq7PiM1fnZNvZg2sWELQnuYank'))
      unlinkSync(resolve(contentFolder, 'QmWxyDrJWABXjGonFpUxJD8YYzz2xiFNxupGwYKTbySaZD'))
      unlinkSync(resolve(contentFolder, 'QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn'))
    } catch {}
  })

  it('prepares the endpoints', () => {
    // serve the snapshot file
    components.router.get(`/deployments`, async () => {
      return {
        body: createReadStream('test/fixtures/entity-deployment.json'),
      }
    })

    let i = 0
    components.router.get(`/contents/:file`, async (ctx) => {
      if (i++ % 3 == 0)
        return {
          status: 500,
          body: 'Synthetic failure while downloading file',
        }

      return {
        body: createReadStream('test/fixtures/' + ctx.params.file),
      }
    })
  })

  it('downloads an entity', async () => {
    await assertHash(
      'test/fixtures/QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn',
      'QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn'
    )

    const usedServers = new Map()
    const entity = await downloadEntityAndContentFiles(
      { fetcher: components.fetcher, logs: components.logs, metrics: components.metrics, storage: components.storage },
      'QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn',
      [await components.getBaseUrl()],
      usedServers,
      contentFolder,
      10,
      1
    )

    expect(entity).toHaveProperty('pointers')
    expect(entity).toHaveProperty('metadata')
  })
})

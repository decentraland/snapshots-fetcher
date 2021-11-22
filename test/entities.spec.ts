import { getDeployedEntitiesStream } from '../src'
import { test } from './components'
import { createReadStream, unlinkSync } from 'fs'
import { resolve } from 'path'
import { sleep } from '../src/utils'
import { downloadEntityAndContentFiles } from '../src/entities'

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
    components.router.get(`/content/deployments`, async () => {
      return {
        body: createReadStream('test/fixtures/entity-deployment.json'),
      }
    })

    let i = 0
    components.router.get(`/content/contents/:file`, async (ctx) => {
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
    const usedServers = new Map()
    const entity = await downloadEntityAndContentFiles(
      { fetcher: components.fetcher },
      'QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn',
      [await components.getBaseUrl()],
      usedServers,
      contentFolder
    )

    expect(entity).toHaveProperty('entityId')
    expect(entity).toHaveProperty('entityType')
    expect(entity).toHaveProperty('content')
    expect(entity).toHaveProperty('auditInfo')
  })
})

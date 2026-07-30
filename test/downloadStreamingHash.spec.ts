import { resolve } from 'path'
import { Readable } from 'stream'
import { downloadContentFileToTemporaryFile } from '../src/utils'
import { test } from './components'

const fs = jest.requireActual<typeof import('fs')>('fs')
const contentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'
const contentBody = fs.readFileSync(`test/fixtures/${contentHash}`)

test('downloadContentFileToTemporaryFile streaming hash verification', ({ components }) => {
  let createReadStream: jest.SpyInstance

  it('prepares a hash-addressed content response', () => {
    components.router.get(`/contents/${contentHash}`, async () => ({ body: Readable.from([contentBody]) }))
  })

  it('downloads and verifies the content', async () => {
    createReadStream = jest.spyOn(fs, 'createReadStream')
    const downloaded = await downloadContentFileToTemporaryFile(
      { metrics: components.metrics },
      `${await components.getBaseUrl()}/contents/${contentHash}`,
      resolve('downloads', contentHash),
      contentHash
    )
    await downloaded.cleanup()
  })

  it('should not reopen the completed file for hash verification', () => {
    expect(createReadStream).not.toHaveBeenCalled()
  })

  afterAll(() => {
    jest.restoreAllMocks()
  })
})

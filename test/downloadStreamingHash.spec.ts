import { resolve } from 'path'
import { Readable } from 'stream'
import {
  createPrivateDownloadWriteStream,
  downloadContentFileToTemporaryFile,
  VerifiedTemporaryFile
} from '../src/utils'
import { test } from './components'

const fs = jest.requireActual<typeof import('fs')>('fs')
const contentHash = 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6'
const contentBody = fs.readFileSync(`test/fixtures/${contentHash}`)

describe('createPrivateDownloadWriteStream', () => {
  describe('when the target path already exists', () => {
    let targetFilename: string
    let writeError: NodeJS.ErrnoException
    let existingContents: string

    beforeEach(async () => {
      targetFilename = resolve('downloads', 'exclusive-download-target')
      await fs.promises.mkdir(resolve('downloads'), { recursive: true })
      await fs.promises.writeFile(targetFilename, 'preserve me', { mode: 0o600 })
      const stream = createPrivateDownloadWriteStream(targetFilename)
      writeError = await new Promise((resolveError) => stream.once('error', resolveError))
      existingContents = await fs.promises.readFile(targetFilename, 'utf8')
    })

    it('should reject rather than following or overwriting the existing entry', () => {
      expect(writeError.code).toBe('EEXIST')
    })

    it('should preserve the existing file contents', () => {
      expect(existingContents).toBe('preserve me')
    })

    afterEach(async () => {
      await fs.promises.unlink(targetFilename).catch(() => undefined)
    })
  })
})

test('when a streamed download matches its content hash', ({ components }) => {
  let downloaded: VerifiedTemporaryFile
  let downloadedMode: number

  it('prepares a hash-addressed content response', () => {
    components.router.get(`/contents/${contentHash}`, async () => ({ body: Readable.from([contentBody]) }))
  })

  it('downloads and verifies the content', async () => {
    downloaded = await downloadContentFileToTemporaryFile(
      { metrics: components.metrics },
      `${await components.getBaseUrl()}/contents/${contentHash}`,
      resolve('downloads', contentHash),
      contentHash
    )
    downloadedMode = (await fs.promises.stat(downloaded.filename)).mode & 0o777
  })

  it('should keep the temporary file inaccessible to group and other users', () => {
    expect(downloadedMode).toBe(0o600)
  })

  afterAll(async () => {
    await downloaded?.cleanup()
  })
})

test('when a streamed download does not match its claimed content hash', ({ components }) => {
  let remainingTemporaryFiles: string[]
  let downloadError: unknown

  it('prepares a response with bytes belonging to a different hash', () => {
    components.router.get(`/contents/${contentHash}`, async () => ({ body: Readable.from(['tampered bytes']) }))
  })

  it('attempts the verified temporary download', async () => {
    downloadError = await downloadContentFileToTemporaryFile(
      { metrics: components.metrics },
      `${await components.getBaseUrl()}/contents/${contentHash}`,
      resolve('downloads', contentHash),
      contentHash
    ).catch((error) => error)
    remainingTemporaryFiles = (await fs.promises.readdir(resolve('downloads'))).filter((filename) =>
      filename.startsWith(contentHash)
    )
  })

  it('should reject the bytes instead of exposing an unverified temporary file', () => {
    expect(downloadError).toEqual(
      expect.objectContaining({
        message: expect.stringContaining('hashes do not match')
      })
    )
  })

  it('should remove the mismatched temporary file', () => {
    expect(remainingTemporaryFiles).toEqual([])
  })
})

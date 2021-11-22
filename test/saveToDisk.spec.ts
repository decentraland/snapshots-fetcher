import { test } from './components'
import { readFileSync, unlinkSync, promises as fsPromises, constants } from 'fs'
import { resolve } from 'path'
import { Readable } from 'stream'
import { checkFileExists, saveToDisk, sleep } from '../src/utils'
import { downloadFileWithRetries } from '../src/downloader'

test('saveToDisk', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const content = Buffer.from(Math.random().toString(), 'utf-8')

  it('prepares the endpoints', () => {
    components.router.get(`/working`, async () => {
      return {
        body: content.toString(),
      }
    })

    components.router.get(`/content/contents/alwaysFails`, async () => {
      return {
        status: 503,
      }
    })

    let wasCalled = false
    components.router.get(`/content/contents/failsSecondTime`, async () => {
      if (wasCalled) {
        return {
          status: 503,
        }
      }

      wasCalled = true
      return {
        status: 200,
        body: 'hi there',
      }
    })

    components.router.get(`/fails`, async () => {
      let chunk = 0

      function* streamContent() {
        // sleep to fool the nagle algorithm
        chunk++
        yield 'a'
        if (chunk == 100) {
          console.log('Closing stream')
          throw new Error('Closing stream')
        }
      }

      return {
        headers: {
          'content-length': '100000',
        },
        body: Readable.from(streamContent(), { encoding: 'utf-8' }),
      }
    })
  })

  it('downloads a file to the content folder', async () => {
    const filename = resolve(contentFolder, 'working')
    try {
      unlinkSync(filename)
    } catch {}

    await saveToDisk((await components.getBaseUrl()) + '/working', filename)

    // check file exists and has correct content
    expect(readFileSync(filename)).toEqual(content)
    // check permissions
    await fsPromises.access(filename, constants.R_OK)
    await fsPromises.access(filename, constants.W_OK)
    await expect(() => fsPromises.access(filename, constants.X_OK)).rejects.toThrow('EACCES')
  })

  it('fails to download an aborted stream', async () => {
    const filename = resolve(contentFolder, 'fails')
    try {
      unlinkSync(filename)
    } catch {}

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(async () => await saveToDisk((await components.getBaseUrl()) + '/fails', filename)).rejects.toThrow(
      'aborted'
    )
    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a 404 response', async () => {
    const filename = resolve(contentFolder, 'fails404')
    try {
      unlinkSync(filename)
    } catch {}

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(async () => await saveToDisk((await components.getBaseUrl()) + '/fails404', filename)).rejects.toThrow(
      'status: 404'
    )
    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a ECONNREFUSED error', async () => {
    const filename = resolve(contentFolder, 'failsECONNREFUSED')
    try {
      unlinkSync(filename)
    } catch {}

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () => await saveToDisk('http://0.0.0.0:65433/please-dont-listen-on-this-port', filename)
    ).rejects.toThrow('ECONNREFUSED')

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a TLS error', async () => {
    const filename = resolve(contentFolder, 'failsTLS')
    try {
      unlinkSync(filename)
    } catch {}

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () => await saveToDisk((await components.getBaseUrl()).replace('http:', 'https:') + '/working', filename)
    ).rejects.toThrow()

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('downloads file using TLS', async () => {
    const filename = resolve(contentFolder, 'decentraland.org')
    try {
      unlinkSync(filename)
    } catch {}

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await saveToDisk('https://decentraland.org', filename)

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(true)
  })

  it('always failing endpoint converges and fails', async () => {
    await expect(async () => {
      await downloadFileWithRetries('alwaysFails', contentFolder, [await components.getBaseUrl()], new Map())
    }).rejects.toThrow('Invalid response')
  })

  it('concurrent download reuses job', async () => {
    const a = downloadFileWithRetries('failsSecondTime', contentFolder, [await components.getBaseUrl()], new Map())
    const b = downloadFileWithRetries('failsSecondTime', contentFolder, [await components.getBaseUrl()], new Map())

    expect(await a).toEqual(await b)
  })

  it('already downloaded files must return without actually downloading the file', async () => {
    const a = downloadFileWithRetries('failsSecondTime', contentFolder, [await components.getBaseUrl()], new Map())

    await a
  })
})

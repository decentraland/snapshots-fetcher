import { test } from './components'
import { readFileSync, unlinkSync, promises as fsPromises, constants } from 'fs'
import { resolve } from 'path'
import { Readable } from 'stream'
import { gzipSync } from 'zlib'
import { checkFileExists, saveContentFileToDisk, streamToBuffer } from '../src/utils'
import { downloadFileWithRetries } from '../src/downloader'
import { metricsDefinitions } from '../src/metrics'
import { createTestMetricsComponent } from '@well-known-components/metrics'

const maxRetries = 10
const waitTimeBetweenRetries = 100

test('saveToDisk', ({ components, stubComponents }) => {
  const contentFolder = resolve('downloads')
  const content = Buffer.from(Math.random().toString(), 'utf-8')
  const metrics = createTestMetricsComponent(metricsDefinitions)

  beforeEach(() => {
    metrics.resetAll()
  })

  it('prepares the endpoints', () => {
    components.router.get(`/working`, async () => {
      return {
        body: content.toString(),
      }
    })
    components.router.get(`/working-redirected-302`, async () => {
      return {
        status: 302,
        headers: {
          location: '/working',
        },
      }
    })
    components.router.get(`/working-redirected-301`, async () => {
      return {
        status: 301,
        headers: {
          location: '/working',
        },
      }
    })
    components.router.get(`/forever-redirecting-301`, async () => {
      return {
        status: 301,
        headers: {
          location: '/forever-redirecting-301',
        },
      }
    })
    components.router.get(`/QmInValidHash`, async () => {
      return {
        body: content.toString(),
      }
    })

    components.router.get(`/contents/alwaysFails`, async () => {
      return {
        status: 503,
      }
    })

    let wasCalled = false
    // this endpoint works the first time and fails the 1+Nth time
    components.router.get(`/contents/bafkreigwey5vc6q25ilofdu2vjvcag72eqj46lzipi6mredsfpe42ls2ri`, async () => {
      if (wasCalled) {
        return {
          status: 503,
        }
      }

      wasCalled = true
      return {
        status: 200,
        body: gzipSync('some file'),
        headers: {
          'content-encoding': 'gzip',
        },
      }
    })

    components.router.get(`/fails`, async () => {
      let chunk = 0

      function* streamContent() {
        // sleep to fool the nagle algorithm
        chunk++
        yield 'a'
        if (chunk == 100) {
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
    } catch { }

    await saveContentFileToDisk(
      { metrics, storage: components.storage },
      (await components.getBaseUrl()) + '/working',
      filename,
      'working',
      false
    )

    // check file exists and has correct content
    const fileContent = await streamToBuffer(await (await components.storage.retrieve('working')).asStream())

    expect(fileContent).toEqual(content)
  })

  it('downloads a file to the content folder, follows 302 redirects', async () => {
    const filename = resolve(contentFolder, 'working')
    try {
      unlinkSync(filename)
    } catch { }

    await saveContentFileToDisk(
      { metrics, storage: components.storage },
      (await components.getBaseUrl()) + '/working-redirected-302',
      filename,
      'working',
      false
    )

    const fileContent = await streamToBuffer(await (await components.storage.retrieve('working')).asStream())

    // check file exists and has correct content
    expect(fileContent).toEqual(content)
  })

  it('downloads a file to the content folder, follows 301 redirects', async () => {
    const filename = resolve(contentFolder, 'working')
    try {
      unlinkSync(filename)
    } catch { }

    await saveContentFileToDisk(
      { metrics, storage: components.storage },
      (await components.getBaseUrl()) + '/working-redirected-301',
      filename,
      'working',
      false
    )

    const fileContent = await streamToBuffer(await (await components.storage.retrieve('working')).asStream())

    // check file exists and has correct content
    expect(fileContent).toEqual(content)
  })

  it('fails on eternal redirection loop', async () => {
    const filename = resolve(contentFolder, 'working')
    await expect(
      saveContentFileToDisk(
        { metrics, storage: components.storage },
        (await components.getBaseUrl()) + '/forever-redirecting-301',
        filename,
        'working',
        false
      )
    ).rejects.toThrow('Too much redirects')
  })

  it('fails to download an aborted stream', async () => {
    const filename = resolve(contentFolder, 'fails')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () =>
        await saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/fails',
          filename,
          'fails',
          false
        )
    ).rejects.toThrow('aborted')
    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a 404 response', async () => {
    const filename = resolve(contentFolder, 'fails404')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () =>
        await saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/fails404',
          filename,
          'fails404',
          false
        )
    ).rejects.toThrow('status: 404')
    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a ECONNREFUSED error', async () => {
    const filename = resolve(contentFolder, 'failsECONNREFUSED')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () =>
        await saveContentFileToDisk(
          { metrics, storage: components.storage },
          'http://0.0.0.0:65433/please-dont-listen-on-this-port',
          filename,
          'failsECONNREFUSED',
          false
        )
    ).rejects.toThrow('ECONNREFUSED')

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('fails to download a TLS error', async () => {
    const filename = resolve(contentFolder, 'failsTLS')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () =>
        await saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()).replace('http:', 'https:') + '/working',
          filename,
          'failsTLS',
          false
        )
    ).rejects.toThrow()

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })

  it('downloads file using TLS', async () => {
    const filename = resolve(contentFolder, 'decentraland.org')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await saveContentFileToDisk(
      { metrics, storage: components.storage },
      'https://decentraland.org',
      filename,
      'decentraland.org',
      false
    )

    // check file exists and has correct content
    expect(await components.storage.exist('decentraland.org')).toEqual(true)
  })

  it('always failing endpoint converges and fails', async () => {
    await expect(async () => {
      await downloadFileWithRetries(
        { metrics, storage: components.storage },
        'alwaysFails',
        contentFolder,
        [await components.getBaseUrl()],
        new Map(),
        maxRetries,
        waitTimeBetweenRetries
      )
    }).rejects.toThrow('Invalid response')
  })

  it('concurrent download reuses job', async () => {
    const a = downloadFileWithRetries(
      { metrics, storage: components.storage },
      'bafkreigwey5vc6q25ilofdu2vjvcag72eqj46lzipi6mredsfpe42ls2ri',
      contentFolder,
      [await components.getBaseUrl()],
      new Map(),
      maxRetries,
      waitTimeBetweenRetries
    )
    const b = downloadFileWithRetries(
      { metrics, storage: components.storage },
      'bafkreigwey5vc6q25ilofdu2vjvcag72eqj46lzipi6mredsfpe42ls2ri',
      contentFolder,
      [await components.getBaseUrl()],
      new Map(),
      maxRetries,
      waitTimeBetweenRetries
    )

    expect(await a).toEqual(await b)
  })

  it('already downloaded files must return without actually downloading the file', async () => {
    const a = downloadFileWithRetries(
      { metrics, storage: components.storage },
      'bafkreigwey5vc6q25ilofdu2vjvcag72eqj46lzipi6mredsfpe42ls2ri',
      contentFolder,
      [await components.getBaseUrl()],
      new Map(),
      maxRetries,
      waitTimeBetweenRetries
    )

    await a
  })

  it('fails to download a file with an invalid hash', async () => {
    const filename = resolve(contentFolder, 'QmInValidHash')
    try {
      unlinkSync(filename)
    } catch { }

    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)

    await expect(
      async () =>
        await saveContentFileToDisk(
          { metrics, storage: components.storage },
          (await components.getBaseUrl()) + '/QmInValidHash',
          filename,
          'QmInValidHash'
        )
    ).rejects.toThrow('hashes do not match')
    // check file exists and has correct content
    expect(await checkFileExists(filename)).toEqual(false)
  })
})

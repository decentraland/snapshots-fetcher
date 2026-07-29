import { Readable } from 'stream'
import { downloadEntityAndContentFiles } from '../src'
import { hashV1 } from '@dcl/hashing'
import { test } from './components'

// Builds a stored entity file whose content hash matches its bytes, so downloadEntityAndContentFiles
// passes the content-hash verification step and reaches the profile-avatar handling under test.
async function storeEntity(
  storage: { storeStream(id: string, stream: Readable): Promise<void> },
  entity: Record<string, unknown>
): Promise<string> {
  const bytes = Buffer.from(JSON.stringify(entity))
  const entityId = await hashV1(bytes)
  await storage.storeStream(entityId, Readable.from(bytes))
  return entityId
}

test('downloadEntityAndContentFiles when the profile metadata has a malformed shape', ({ components }) => {
  const targetFolder = 'downloads'

  describe('when an avatars entry is missing its `avatar` object', () => {
    let entityId: string

    beforeEach(async () => {
      entityId = await storeEntity(components.storage, {
        type: 'profile',
        metadata: { avatars: [{ name: 'a-profile-without-an-avatar-object' }] }
      })
    })

    it('should return the parsed entity without attempting any avatar download', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).resolves.toMatchObject({ type: 'profile' })
    })
  })

  describe('and the avatar snapshots hold a non-string value', () => {
    let entityId: string

    beforeEach(async () => {
      entityId = await storeEntity(components.storage, {
        type: 'profile',
        metadata: { avatars: [{ avatar: { snapshots: { body: 12345, face256: null } } }] }
      })
    })

    it('should return the parsed entity without attempting any avatar download', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).resolves.toMatchObject({ type: 'profile' })
    })
  })

  describe('and the avatar snapshots value is a long string instead of an object', () => {
    let entityId: string
    let requestedFiles: number

    beforeEach(async () => {
      requestedFiles = 0
      components.router.get('/contents/:file', async () => {
        requestedFiles++
        return { body: 'unused' }
      })
      entityId = await storeEntity(components.storage, {
        type: 'profile',
        // Object.values on a string yields one entry per character, and each single character passes
        // content-hash validation — so this used to become one queued download per character.
        metadata: { avatars: [{ avatar: { snapshots: 'a'.repeat(5000) } }] }
      })
    })

    it('should return the parsed entity without expanding the string into downloads', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).resolves.toMatchObject({ type: 'profile' })
    })

    it('should not request a single content file for it', async () => {
      await downloadEntityAndContentFiles(
        components,
        entityId,
        [await components.getBaseUrl()],
        new Map(),
        targetFolder,
        1,
        0
      )

      expect(requestedFiles).toBe(0)
    })
  })

  describe('and the avatars property is not an array', () => {
    let entityId: string

    beforeEach(async () => {
      entityId = await storeEntity(components.storage, {
        type: 'profile',
        metadata: { avatars: { body: 'not-an-array' } }
      })
    })

    it('should return the parsed entity without attempting any avatar download', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).resolves.toMatchObject({ type: 'profile' })
    })
  })

  describe.each([
    ['an object', { file: 'body.png' }],
    ['a string', 'body.png'],
    ['a number', 42],
    // Rejected too: @dcl/schemas declares content as a required array, so a present-but-null field is
    // not something a conforming server sends, and reading it as "no content" is a guess.
    ['null', null]
  ])('and the content property is %s rather than an array', (_label: string, content: unknown) => {
    let entityId: string

    beforeEach(async () => {
      entityId = await storeEntity(components.storage, {
        type: 'profile',
        metadata: { avatars: [] },
        content
      })
    })

    // This used to resolve: every reader of content[] guards with Array.isArray and falls back to "no
    // content", so a malformed manifest was reported as a fully downloaded entity whose dependencies had
    // never been fetched — and it would then be deployed as complete. Not iterating it is not the same as
    // there being nothing to iterate.
    it('should reject rather than reporting an entity with no content files', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).rejects.toThrow('declares a content field that is not an array')
    })
  })

  describe('and the content property is absent altogether', () => {
    let entityId: string

    beforeEach(async () => {
      // Absence is the one case that can be read as "no content files" without guessing.
      entityId = await storeEntity(components.storage, { type: 'profile', metadata: { avatars: [] } })
    })

    it('should treat it as an entity with no content files', async () => {
      await expect(
        downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
      ).resolves.toMatchObject({ type: 'profile' })
    })
  })
})

test('downloadEntityAndContentFiles when the entity vanishes from storage after the download', ({ components }) => {
  const targetFolder = 'downloads'
  const entityId = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'

  beforeEach(() => {
    // The download reports success but the file is not there to read back: a storage-level
    // inconsistency, not a corrupt file, so it must not be mistaken for a hash failure.
    jest.spyOn(components.storage, 'retrieve').mockResolvedValue(undefined)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('should reject reporting that the entity could not be retrieved', async () => {
    await expect(
      downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
    ).rejects.toThrow(`Entity file ${entityId} could not be retrieved from storage after download`)
  })
})

test('downloadEntityAndContentFiles when a profile references an avatar snapshot missing from content', ({
  components
}) => {
  const targetFolder = 'downloads'
  const snapshotHash = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
  let entityId: string
  let requestedFiles: string[]

  beforeEach(async () => {
    requestedFiles = []
    entityId = await storeEntity(components.storage, {
      type: 'profile',
      metadata: {
        avatars: [
          {
            avatar: {
              snapshots: {
                body: `https://peer.decentraland.org/content/contents/${snapshotHash}`,
                face256: null
              }
            }
          }
        ]
      },
      content: []
    })
  })

  it('prepares the endpoints', () => {
    components.router.get('/contents/:file', async (ctx) => {
      requestedFiles.push(ctx.params.file)
      return { body: 'the-file-contents' }
    })
  })

  it('should extract the hash from the snapshot URL and download it', async () => {
    // The snapshot fixture is already in the in-memory storage, so it short-circuits before any
    // request; delete it first to force the download path that exercises the URL extraction.
    await components.storage.delete([snapshotHash])

    await downloadEntityAndContentFiles(
      components,
      entityId,
      [await components.getBaseUrl()],
      new Map(),
      targetFolder,
      1,
      0
    )

    expect(requestedFiles).toEqual([snapshotHash])
  })
})

test('downloadEntityAndContentFiles when a profile declares avatar snapshots and a bad content hash', ({
  components
}) => {
  const targetFolder = 'downloads'
  let requestedFiles: number

  beforeEach(() => {
    requestedFiles = 0
    components.router.get('/contents/:file', async () => {
      requestedFiles++
      return { body: 'unused' }
    })
  })

  it('should reject without spending the avatar budget first', async () => {
    const entityId = await storeEntity(components.storage, {
      type: 'profile',
      metadata: {
        avatars: [
          {
            avatar: {
              // Deliberately NOT fixture hashes: test-component preloads every fixture into storage, so a
              // fixture-named snapshot short-circuits on storage.exist and never reaches the network —
              // which would make this assertion pass whatever the ordering is.
              snapshots: {
                body: `ba${'a'.repeat(57)}`,
                face: `ba${'b'.repeat(57)}`
              }
            }
          }
        ]
      },
      // One unusable required hash means the entity can never be accepted, so none of the avatar
      // fallback downloads above were ever worth starting.
      content: [{ file: 'good.png', hash: 'QmazJLZfUmZgNMTdwWSmJRvw4dBfcjS9GuqkwkKGRWb4K6' }, { file: 'evil', hash: '../../etc/passwd' }]
    })

    await expect(
      downloadEntityAndContentFiles(components, entityId, [await components.getBaseUrl()], new Map(), targetFolder, 1, 0)
    ).rejects.toThrow('declares an invalid content file hash')

    expect(requestedFiles).toEqual(0)
  })
})

test('downloadEntityAndContentFiles when a profile declares more avatar snapshots than the cap', ({ components }) => {
  const targetFolder = 'downloads'
  let warnings: Array<{ message: string; extra?: any }>

  beforeEach(() => {
    warnings = []
    components.router.get('/contents/:file', async () => ({ body: 'unused' }))
    jest.spyOn(components.logs, 'getLogger').mockReturnValue({
      log: jest.fn(),
      debug: jest.fn(),
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn((message: string, extra?: any) => warnings.push({ message, extra }))
    })
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('should log that the list was truncated, not silently drop the rest', async () => {
    // 1001 distinct snapshot hashes against a cap of 1000. Without a signal an operator cannot tell this
    // apart from a profile that simply had nothing missing.
    const snapshots: Record<string, string> = {}
    for (let index = 0; index < 1001; index++) {
      snapshots[`slot${index}`] = `ba${String(index).padStart(57, '0')}`
    }
    const entityId = await storeEntity(components.storage, {
      type: 'profile',
      metadata: { avatars: [{ avatar: { snapshots } }] },
      content: []
    })

    await downloadEntityAndContentFiles(
      components,
      entityId,
      [await components.getBaseUrl()],
      new Map(),
      targetFolder,
      1,
      0
    )

    expect(warnings.map((entry) => entry.message)).toContain(
      'Profile declared more avatar snapshots than will be fetched; the rest were dropped.'
    )
  })
})

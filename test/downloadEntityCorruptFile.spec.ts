import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { hashV0, hashV1 } from '@dcl/hashing'
import { resolve } from 'path'
import { Readable } from 'stream'
import { downloadEntityAndContentFiles } from '../src'
import { test } from './components'

test('downloadEntityAndContentFiles with a corrupt stored entity file', ({ components }) => {
  const contentFolder = resolve('downloads')

  async function downloadWith(storage: IContentStorageComponent, entityId: string): Promise<Error | undefined> {
    try {
      await downloadEntityAndContentFiles(
        { fetcher: components.fetcher, logs: components.logs, metrics: components.metrics, storage },
        entityId,
        [await components.getBaseUrl()],
        new Map(),
        contentFolder,
        10,
        0
      )
      return undefined
    } catch (error: any) {
      return error
    }
  }

  describe('when the stored entity file is empty and fails hash verification', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      storage = createInMemoryStorage()
      // The id addresses the original (non-empty) entity, so the empty local copy is a proven
      // truncated write: hash('') can never equal it.
      entityId = await hashV1(Buffer.from('{"type":"profile","content":[]}'))
      await storage.storeStream(entityId, Readable.from([Buffer.from('')]))
      thrownError = await downloadWith(storage, entityId)
    })

    it('should reject with an error naming the entity instead of a context-free parse error', () => {
      expect(thrownError?.message).toContain(entityId)
    })

    it('should evict the corrupt file so a later retry re-downloads it', async () => {
      expect(await storage.exist(entityId)).toBe(false)
    })
  })

  describe('when the stored entity file is truncated JSON and fails hash verification', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      storage = createInMemoryStorage()
      // A Qm-addressed entity whose local copy lost its tail mid-write.
      entityId = await hashV0(Buffer.from('{"type":"profile","content":[]}'))
      await storage.storeStream(entityId, Readable.from([Buffer.from('{ "type": "profile"')]))
      thrownError = await downloadWith(storage, entityId)
    })

    it('should reject with an error naming the entity instead of a context-free parse error', () => {
      expect(thrownError?.message).toContain(entityId)
    })

    it('should evict the corrupt file so a later retry re-downloads it', async () => {
      expect(await storage.exist(entityId)).toBe(false)
    })
  })

  describe('when the stored bytes are not JSON but match the content hash', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let deleteCalls: string[][]
    let thrownError: Error | undefined

    beforeEach(async () => {
      // A valid, hash-correct non-JSON file (e.g. an image) that a malformed or malicious remote
      // feed advertises as an entityId. Evicting it would delete legitimate cached content.
      const bytes = Buffer.from('not json at all — could be a cached png')
      entityId = await hashV1(bytes)
      const inner = createInMemoryStorage()
      await inner.storeStream(entityId, Readable.from([bytes]))
      deleteCalls = []
      storage = {
        ...inner,
        async delete(ids: string[]) {
          deleteCalls.push(ids)
          return inner.delete(ids)
        }
      }
      thrownError = await downloadWith(storage, entityId)
    })

    it('should surface an entity-scoped parse error explaining the copy was kept', () => {
      expect(thrownError?.message).toContain('the stored bytes match the content hash, so the local copy was kept')
    })

    it('should not delete the hash-valid cached content', () => {
      expect(deleteCalls).toEqual([])
    })

    it('should keep the file in storage', async () => {
      expect(await storage.exist(entityId)).toBe(true)
    })
  })

  describe('when the stored bytes are valid JSON but fail hash verification', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      storage = createInMemoryStorage()
      // A mis-keyed local file: parse-valid entity JSON stored under a DIFFERENT entity's hash.
      // Without the pre-parse hash gate it would be processed as the wrong entity's metadata.
      entityId = await hashV1(Buffer.from('{"type":"scene","content":[{"file":"a.glb","hash":"x"}]}'))
      await storage.storeStream(entityId, Readable.from([Buffer.from('{"type":"profile","content":[]}')]))
      thrownError = await downloadWith(storage, entityId)
    })

    it('should reject with an entity-scoped hash-verification error instead of processing the wrong entity', () => {
      expect(thrownError?.message).toContain(`${entityId} failed content-hash verification`)
    })

    it('should evict the mis-keyed file so a later retry re-downloads it', async () => {
      expect(await storage.exist(entityId)).toBe(false)
    })
  })

  describe('when the entity id has a known prefix but is not a syntactically valid cid', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let deleteCalls: string[][]
    let thrownError: Error | undefined

    beforeEach(async () => {
      // Starts with 'Qm' but is not a valid CID: a computed hash can never equal it, so treating it
      // as verifiable would make EVERY such id grounds for destructive eviction.
      entityId = 'Qmnotavalidcid'
      const inner = createInMemoryStorage()
      await inner.storeStream(entityId, Readable.from([Buffer.from('arbitrary cached bytes')]))
      deleteCalls = []
      storage = {
        ...inner,
        async delete(ids: string[]) {
          deleteCalls.push(ids)
          return inner.delete(ids)
        }
      }
      thrownError = await downloadWith(storage, entityId)
    })

    it('should explain the copy was kept because corruption is unprovable', () => {
      expect(thrownError?.message).toContain('could not be proven corrupt (unverifiable hash scheme)')
    })

    it('should not delete the stored file', () => {
      expect(deleteCalls).toEqual([])
    })
  })

  describe('when the entity id uses an unverifiable hash scheme and the bytes are not JSON', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let deleteCalls: string[][]
    let thrownError: Error | undefined

    beforeEach(async () => {
      // Neither Qm nor ba: corruption cannot be proven, so the copy must never be deleted.
      entityId = 'zUnknownSchemeEntityId'
      const inner = createInMemoryStorage()
      await inner.storeStream(entityId, Readable.from([Buffer.from('')]))
      deleteCalls = []
      storage = {
        ...inner,
        async delete(ids: string[]) {
          deleteCalls.push(ids)
          return inner.delete(ids)
        }
      }
      thrownError = await downloadWith(storage, entityId)
    })

    it('should explain the copy was kept because corruption is unprovable', () => {
      expect(thrownError?.message).toContain('could not be proven corrupt (unverifiable hash scheme)')
    })

    it('should not delete the stored file', () => {
      expect(deleteCalls).toEqual([])
    })
  })

  describe('when the stored entity file is corrupt and evicting it fails', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      entityId = await hashV1(Buffer.from('{"type":"scene","content":[]}'))
      const inner = createInMemoryStorage()
      await inner.storeStream(entityId, Readable.from([Buffer.from('')]))
      storage = {
        ...inner,
        async delete() {
          throw new Error('delete is unavailable')
        }
      }
      thrownError = await downloadWith(storage, entityId)
    })

    it('should still surface the entity-scoped parse error, not the delete error', () => {
      expect(thrownError?.message).toContain(entityId)
    })

    it('should report that the corrupt copy could not be removed', () => {
      expect(thrownError?.message).toContain('could not remove the corrupt local copy')
    })
  })

  describe('when reading the stored entity file fails transiently', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let deleteCalls: string[][]
    let thrownError: Error | undefined

    beforeEach(async () => {
      entityId = await hashV1(Buffer.from('{"type":"scene"}'))
      const inner = createInMemoryStorage()
      // Seed a valid entity so exist() is true (the download is skipped) and there is something to
      // delete; the read itself is then made to fail transiently.
      await inner.storeStream(entityId, Readable.from([Buffer.from('{"type":"scene"}')]))
      deleteCalls = []
      storage = {
        ...inner,
        async retrieve() {
          return {
            async asStream() {
              throw new Error('transient read failure')
            }
          } as any
        },
        async delete(ids: string[]) {
          deleteCalls.push(ids)
          return inner.delete(ids)
        }
      }
      thrownError = await downloadWith(storage, entityId)
    })

    it('should propagate the read error rather than a parse error', () => {
      expect(thrownError?.message).toContain('transient read failure')
    })

    it('should not evict the stored file', () => {
      expect(deleteCalls).toEqual([])
    })
  })
})

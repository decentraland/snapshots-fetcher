import { createInMemoryStorage, IContentStorageComponent } from '@dcl/catalyst-storage'
import { resolve } from 'path'
import { Readable } from 'stream'
import { downloadEntityAndContentFiles } from '../src'
import { test } from './components'

test('downloadEntityAndContentFiles with a corrupt stored entity file', ({ components }) => {
  const contentFolder = resolve('downloads')

  describe('when the stored entity file is empty', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      storage = createInMemoryStorage()
      entityId = 'emptyentityfilepoisonpilltest'
      // Seed an empty file so exist() is true and the download step is skipped, reproducing the
      // truncated local copy an interrupted write leaves behind.
      await storage.storeStream(entityId, Readable.from([Buffer.from('')]))
      thrownError = undefined
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
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should reject with an error naming the entity instead of a context-free parse error', () => {
      expect(thrownError?.message).toContain(entityId)
    })

    it('should evict the corrupt file so a later retry re-downloads it', async () => {
      expect(await storage.exist(entityId)).toBe(false)
    })
  })

  describe('when the stored entity file is not valid JSON', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      storage = createInMemoryStorage()
      entityId = 'invalidjsonentityfilepoisonpilltest'
      await storage.storeStream(entityId, Readable.from([Buffer.from('{ "type": "profile"')]))
      thrownError = undefined
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
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should reject with an error naming the entity instead of a context-free parse error', () => {
      expect(thrownError?.message).toContain(entityId)
    })

    it('should evict the corrupt file so a later retry re-downloads it', async () => {
      expect(await storage.exist(entityId)).toBe(false)
    })
  })

  describe('when the stored entity file is empty and evicting it fails', () => {
    let storage: IContentStorageComponent
    let entityId: string
    let thrownError: Error | undefined

    beforeEach(async () => {
      entityId = 'evictionfailureentitytest'
      const inner = createInMemoryStorage()
      await inner.storeStream(entityId, Readable.from([Buffer.from('')]))
      storage = {
        ...inner,
        async delete() {
          throw new Error('delete is unavailable')
        }
      }
      thrownError = undefined
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
      } catch (error: any) {
        thrownError = error
      }
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
      entityId = 'transientreaderrorentitytest'
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
      thrownError = undefined
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
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should propagate the read error rather than a parse error', () => {
      expect(thrownError?.message).toContain('transient read failure')
    })

    it('should not evict the stored file', () => {
      expect(deleteCalls).toEqual([])
    })
  })
})

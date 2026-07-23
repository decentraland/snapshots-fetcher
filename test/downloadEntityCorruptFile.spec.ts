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
})

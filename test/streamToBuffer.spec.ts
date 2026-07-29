import { Readable } from 'stream'
import { streamToBuffer } from '../src/utils'

describe('streamToBuffer', () => {
  describe('when no maximum is given', () => {
    let result: Buffer

    beforeEach(async () => {
      result = await streamToBuffer(Readable.from([Buffer.from('abc'), Buffer.from('def')]))
    })

    it('should concatenate every chunk', () => {
      expect(result.toString()).toEqual('abcdef')
    })
  })

  describe('when the stream stays within the maximum', () => {
    let result: Buffer

    beforeEach(async () => {
      result = await streamToBuffer(Readable.from([Buffer.alloc(10, 0x61)]), 16)
    })

    it('should return the buffered content', () => {
      expect(result.length).toEqual(10)
    })
  })

  describe('when the stream exceeds the maximum', () => {
    let thrownError: Error | undefined

    beforeEach(async () => {
      thrownError = undefined
      try {
        await streamToBuffer(Readable.from([Buffer.alloc(10, 0x61), Buffer.alloc(10, 0x62)]), 16)
      } catch (error: any) {
        thrownError = error
      }
    })

    it('should reject rather than buffering the whole stream', () => {
      expect(thrownError?.message).toEqual('Stream exceeds the maximum allowed size of 16 bytes')
    })
  })

  describe('and the producer keeps going after the maximum is exceeded', () => {
    let stream: Readable
    let chunksProduced: number

    beforeEach(async () => {
      chunksProduced = 0
      // Counts what the producer was actually asked for, so the assertion shows the stream was torn
      // down instead of being drained into a buffer nobody will read.
      function* chunks() {
        for (let index = 0; index < 1000; index++) {
          chunksProduced++
          yield Buffer.alloc(10, 0x61)
        }
      }
      stream = Readable.from(chunks())
      await streamToBuffer(stream, 16).catch(() => undefined)
    })

    it('should destroy the stream', () => {
      expect(stream.destroyed).toEqual(true)
    })

    it('should stop pulling chunks from the producer', () => {
      expect(chunksProduced).toBeLessThan(1000)
    })
  })
})

import { downloadFileWithRetries } from '../src/downloader'
import { isValidContentHash } from '../src/utils'

describe('isValidContentHash', () => {
  describe('when the hash is a valid CIDv0 (Qm) content hash', () => {
    it('should return true', () => {
      expect(isValidContentHash('QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn')).toBe(true)
    })
  })

  describe('when the hash is a valid CIDv1 (ba) content hash', () => {
    it('should return true', () => {
      expect(isValidContentHash('bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu')).toBe(true)
    })
  })

  describe('when the hash contains a path traversal sequence', () => {
    it('should return false', () => {
      expect(isValidContentHash('../../etc/passwd')).toBe(false)
    })
  })

  describe('when the hash contains a path separator', () => {
    it('should return false', () => {
      expect(isValidContentHash('abc/def')).toBe(false)
    })
  })

  describe('when the hash contains a dot', () => {
    it('should return false', () => {
      expect(isValidContentHash('file.txt')).toBe(false)
    })
  })

  describe('when the hash is empty', () => {
    it('should return false', () => {
      expect(isValidContentHash('')).toBe(false)
    })
  })

  describe('when the hash exceeds the maximum allowed length', () => {
    it('should return false', () => {
      expect(isValidContentHash('a'.repeat(129))).toBe(false)
    })
  })
})

describe('downloadFileWithRetries', () => {
  describe('when the hash to download is not a valid content hash', () => {
    let storage: { exist: jest.Mock }

    beforeEach(() => {
      storage = { exist: jest.fn() }
    })

    afterEach(() => {
      jest.resetAllMocks()
    })

    it('should reject with an invalid content hash error before touching storage', async () => {
      await expect(
        downloadFileWithRetries({ storage } as any, '../../etc/passwd', '/tmp/downloads', [], new Map(), 1, 0)
      ).rejects.toThrow('Invalid content hash')
      expect(storage.exist).not.toHaveBeenCalled()
    })
  })
})

import { assertHash } from '../src/utils'

describe('assertHash', () => {
  describe('when the hash uses an unrecognised algorithm prefix', () => {
    it('should reject naming the unknown algorithm instead of reading the file', async () => {
      await expect(assertHash('test/fixtures/entity-deployment.json', 'sha256-deadbeef')).rejects.toThrow(
        'Unknown hashing algorithm for hash: sha256-deadbeef'
      )
    })
  })

  describe('when the hash is empty', () => {
    it('should reject as an unknown algorithm', async () => {
      await expect(assertHash('test/fixtures/entity-deployment.json', '')).rejects.toThrow(
        'Unknown hashing algorithm'
      )
    })
  })

  describe('when a CIDv0 hash matches the file contents', () => {
    it('should resolve', async () => {
      await expect(
        assertHash(
          'test/fixtures/QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn',
          'QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn'
        )
      ).resolves.toBeUndefined()
    })
  })

  describe('when a CIDv1 hash does not match the file contents', () => {
    it('should reject reporting both the expected and the calculated hash', async () => {
      await expect(
        assertHash(
          'test/fixtures/QmXx5dDq7nnPuCCP43Ngc7iq4kkqDfC5PEJGawUHYLGxUn',
          'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
        )
      ).rejects.toThrow('hashes do not match')
    })
  })
})

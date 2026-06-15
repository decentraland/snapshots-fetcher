import { getSnapshots } from '../src/client'
import { test } from './components'

const validSnapshot = {
  hash: 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu',
  timeRange: { initTimestamp: 0, endTimestamp: 100 },
  replacedSnapshotHashes: []
}
const newerValidSnapshot = {
  hash: 'bafkreico6luxnkk5vxuxvmpsg7hva4upamyz3br2b6ucc7rf3hdlcaehha',
  timeRange: { initTimestamp: 100, endTimestamp: 200 }
}

test('getSnapshots when the response mixes valid and invalid snapshot metadata', ({ components }) => {
  it('prepares the endpoints', () => {
    components.router.get('/snapshots', async () => ({
      body: [
        validSnapshot,
        newerValidSnapshot,
        // path-traversal hash
        { hash: '../../etc/passwd', timeRange: { initTimestamp: 0, endTimestamp: 50 } },
        // non-numeric timestamp
        { hash: 'validlookinghash', timeRange: { initTimestamp: 0, endTimestamp: 'not-a-number' } },
        // missing hash
        { timeRange: { initTimestamp: 0, endTimestamp: 10 } },
        // missing timeRange
        { hash: 'notimerange' },
        // valid hash but a replaced-snapshot hash that is not a valid content hash
        {
          hash: 'bafkreig6sfhegnp4okzecgx3v6gj6pohh5qzw6zjtrdqtggx64743rkmz4',
          timeRange: { initTimestamp: 0, endTimestamp: 60 },
          replacedSnapshotHashes: ['../../evil']
        },
        // not an object
        'not-an-object'
      ]
    }))
  })

  it('returns only the valid snapshots, sorted newest first', async () => {
    const snapshots = await getSnapshots(components, await components.getBaseUrl(), 10)
    expect(snapshots).toEqual([newerValidSnapshot, validSnapshot])
  })
})

test('getSnapshots when the response is not an array', ({ components }) => {
  it('prepares the endpoints', () => {
    components.router.get('/snapshots', async () => ({ body: { not: 'an array' } }))
  })

  it('rejects indicating that an array was expected', async () => {
    await expect(getSnapshots(components, await components.getBaseUrl(), 10)).rejects.toThrow('expected an array')
  })
})

test('getSnapshots when the response contains many invalid entries', ({ components }) => {
  const numberOfInvalidEntries = 150

  it('prepares the endpoints', () => {
    components.router.get('/snapshots', async () => ({
      // every entry has a path-traversal hash, so all are rejected
      body: Array.from({ length: numberOfInvalidEntries }, (_unused, index) => ({ hash: `../bad-${index}` }))
    }))
  })

  it('caps the invalid-entry error logs and still returns no snapshots', async () => {
    const errorMock = jest.fn()
    jest.spyOn(components.logs, 'getLogger').mockReturnValue({
      log: jest.fn(),
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: errorMock
    })

    const snapshots = await getSnapshots(components, await components.getBaseUrl(), 10)

    expect(snapshots).toEqual([])
    // 100 per-entry errors + 1 summary line
    expect(errorMock).toHaveBeenCalledTimes(101)

    jest.restoreAllMocks()
  })
})

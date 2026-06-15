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

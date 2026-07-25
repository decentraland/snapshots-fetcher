import { AuthLinkType } from '@dcl/schemas'
import { createReadStream } from 'fs'
import { resolve } from 'path'
import {
  getDeployedEntitiesStreamFromPointerChanges,
  getDeployedEntitiesStreamFromSnapshot
} from '../src/stream-entities'
import { test } from './components'

const snapshotHash = 'bafkreibivsdakhiouzuth2nr7c4d3iiolbobj32xhat3nzm5uwyi4raxwu'
const authChain = [{ type: AuthLinkType.SIGNER, payload: '0x1', signature: '' }]

test('getDeployedEntitiesStreamFromSnapshot', ({ components }) => {
  const tmpDownloadFolder = resolve('downloads')
  const streamOptions = {
    tmpDownloadFolder,
    requestMaxRetries: 3,
    requestRetryWaitTime: 0
  }

  it('prepares the endpoints', () => {
    components.router.get(`/contents/${snapshotHash}`, async () => ({
      body: createReadStream(`test/fixtures/${snapshotHash}`)
    }))
  })

  describe('when deleting the snapshot after usage fails', () => {
    let servers: Set<string>
    let deleteError: Error

    beforeEach(async () => {
      servers = new Set([await components.getBaseUrl()])
      deleteError = new Error('storage refused the delete')
      jest.spyOn(components.storage, 'delete').mockRejectedValue(deleteError)
    })

    afterEach(() => {
      jest.restoreAllMocks()
    })

    it('should still finish the stream instead of surfacing the cleanup failure', async () => {
      const streamed: string[] = []
      for await (const deployment of getDeployedEntitiesStreamFromSnapshot(
        components,
        streamOptions,
        snapshotHash,
        servers
      )) {
        streamed.push(deployment.entityId)
      }

      expect(streamed.length).toBeGreaterThan(0)
    })
  })

  describe('when deleteSnapshotAfterUsage is disabled', () => {
    let servers: Set<string>
    let deleteSpy: jest.SpyInstance

    beforeEach(async () => {
      servers = new Set([await components.getBaseUrl()])
      deleteSpy = jest.spyOn(components.storage, 'delete')
    })

    afterEach(() => {
      jest.restoreAllMocks()
    })

    it('should keep the snapshot file in storage', async () => {
      for await (const _deployment of getDeployedEntitiesStreamFromSnapshot(
        components,
        { ...streamOptions, deleteSnapshotAfterUsage: false },
        snapshotHash,
        servers
      )) {
        /* drain */
      }

      expect(deleteSpy).not.toHaveBeenCalled()
    })
  })

  describe('when fromTimestamp is newer than every entity in the snapshot', () => {
    let servers: Set<string>

    beforeEach(async () => {
      servers = new Set([await components.getBaseUrl()])
    })

    it('should yield nothing', async () => {
      const streamed: string[] = []
      for await (const deployment of getDeployedEntitiesStreamFromSnapshot(
        components,
        { ...streamOptions, fromTimestamp: Number.MAX_SAFE_INTEGER, deleteSnapshotAfterUsage: false },
        snapshotHash,
        servers
      )) {
        streamed.push(deployment.entityId)
      }

      expect(streamed).toEqual([])
    })
  })
})

function delta(index: number) {
  return {
    entityType: 'profile',
    // 'ba' + 57 chars: the CIDv1 shape the deployment schema requires. Getting the length wrong makes
    // every delta fail validation and yield nothing, which looks like a hang rather than a failure.
    entityId: `ba${String(index).padStart(57, '0')}`,
    entityTimestamp: index,
    localTimestamp: index,
    authChain,
    pointers: ['0x1']
  }
}

test('getDeployedEntitiesStreamFromPointerChanges', ({ components }) => {
  let polls: number
  // How many deployments each poll returns. fetchPointerChanges always builds the
  // `${server}/pointer-changes` URL itself, so per-case payloads have to come from here rather than
  // from a different route.
  let deltasPerPoll: number

  it('prepares the endpoints', () => {
    polls = 0
    deltasPerPoll = 1
    components.router.get('/pointer-changes', async () => {
      polls++
      const startingIndex = (polls - 1) * deltasPerPoll + 1
      return {
        body: {
          deltas: Array.from({ length: deltasPerPoll }, (_unused, offset) => delta(startingIndex + offset)),
          pagination: {}
        }
      }
    })
  })

  describe('when shouldStop turns true after the first deployment', () => {
    let streamed: string[]
    let pollsWhenDone: number

    beforeEach(async () => {
      polls = 0
      streamed = []
      let seen = 0

      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 5 },
        await components.getBaseUrl(),
        () => seen >= 1
      )) {
        streamed.push(deployment.entityId)
        seen++
      }
      pollsWhenDone = polls
    })

    it('should stop the stream at the deployment that tripped the predicate', () => {
      expect(streamed).toHaveLength(1)
    })

    it('should not keep polling the server', () => {
      expect(pollsWhenDone).toBe(1)
    })
  })

  describe('and shouldStop turns true partway through a single page of deployments', () => {
    let streamed: string[]
    let pollsWhenDone: number

    beforeEach(async () => {
      polls = 0
      streamed = []
      // A single poll carrying three deployments, so the predicate has to be honoured mid-page and
      // not merely between polls.
      deltasPerPoll = 3
      let seen = 0

      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 5 },
        await components.getBaseUrl(),
        () => seen >= 2
      )) {
        streamed.push(deployment.entityId)
        seen++
      }
      pollsWhenDone = polls
    })

    afterEach(() => {
      deltasPerPoll = 1
    })

    it('should stop after the deployment that tripped the predicate', () => {
      expect(streamed).toHaveLength(2)
    })

    it('should not have needed a second poll to notice', () => {
      expect(pollsWhenDone).toBe(1)
    })
  })

  describe('when shouldStop is already true before the first poll', () => {
    let streamed: string[]

    beforeEach(async () => {
      polls = 0
      streamed = []

      for await (const deployment of getDeployedEntitiesStreamFromPointerChanges(
        components,
        { pointerChangesWaitTime: 5 },
        await components.getBaseUrl(),
        () => true
      )) {
        streamed.push(deployment.entityId)
      }
    })

    it('should yield nothing', () => {
      expect(streamed).toEqual([])
    })

    it('should never request pointer-changes at all', () => {
      expect(polls).toBe(0)
    })
  })
})

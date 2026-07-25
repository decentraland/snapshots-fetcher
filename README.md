# snapshots-fetcher

[![Coverage Status](https://coveralls.io/repos/github/decentraland/snapshots-fetcher/badge.svg)](https://coveralls.io/github/decentraland/snapshots-fetcher)

Synchronizes deployed entities from a set of Decentraland content servers. It bootstraps from the
servers' snapshot files and then keeps up with their `/pointer-changes` feeds, handing every entity to
a deployer component supplied by the caller.

Requires Node 22 or newer.

## Usage

`createSynchronizer` needs a full set of components. Everything required to build them is exported
from the package root:

```ts
import { createJobQueue, createSynchronizer } from '@dcl/snapshots-fetcher'

const synchronizer = await createSynchronizer(
  {
    metrics,
    fetcher,
    logs,
    storage,
    processedSnapshotStorage,
    snapshotStorage,
    downloadQueue: createJobQueue({ concurrency: 10, autoStart: true }),
    deployer
  },
  {
    bootstrapReconnection: { reconnectTime: 5_000, reconnectRetryTimeExponent: 1.5 },
    syncingReconnection: { reconnectTime: 1_000, reconnectRetryTimeExponent: 1.2 },
    tmpDownloadFolder: 'downloads',
    requestMaxRetries: 10,
    requestRetryWaitTime: 5_000,
    pointerChangesWaitTime: 5_000
  }
)

const syncJob = await synchronizer.syncWithServers(new Set(['https://peer.decentraland.org/content']))
await syncJob.onSyncFinished()
```

`onSyncFinished()` resolves once every server has bootstrapped, and **rejects** if the synchronizer is
stopped before that happens — so a shutdown never leaves the caller waiting.

### Tuning concurrency

The synchronizer's parallelism defaults to 10 on both of its internal queues. Override either one when
your bandwidth, deployer throughput or the remote servers' rate limits call for it:

```ts
await createSynchronizer(components, {
  // …
  concurrency: {
    snapshotDeployments: 4, // snapshots streamed and deployed at once
    snapshotChecks: 20 // processed-snapshot decisions at once (bounds snapshotStorage load)
  }
})
```

Both must be integers `>= 1`; `createSynchronizer` rejects otherwise. The number of content files
fetched in parallel *per entity* is a separate argument to `downloadEntityAndContentFiles`
(`contentFilesConcurrency`, default 10), since that call belongs to your deployer.

The `deployer` implements `IDeployerComponent`. Its `scheduleEntityDeployment` may return before the
entity is actually deployed; calling the entity's `markAsDeployed()` once it is, is what allows the
snapshot it came from to be recorded as processed.

Register `metricsDefinitions` with your metrics component to expose this package's metrics.

## Downloads and integrity

Content files are downloaded to `tmpDownloadFolder`, hash-verified, and only then handed to the
`storage` component keyed by their content hash. Bytes that do not match the hash addressing them are
deleted rather than stored, and a stored entity file that fails verification is evicted so a retry can
re-download it. Downloads are bounded in size, follow a limited number of redirects, and abort on
socket inactivity.

## Development

```bash
yarn install
yarn build        # or: make build
yarn test         # or: make test
yarn lint:check   # or: make lint
```

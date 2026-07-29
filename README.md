# snapshots-fetcher

[![Coverage Status](https://coveralls.io/repos/github/decentraland/snapshots-fetcher/badge.svg)](https://coveralls.io/github/decentraland/snapshots-fetcher)

Synchronizes deployed entities from a set of Decentraland content servers. It bootstraps from the
servers' snapshot files and then keeps up with their `/pointer-changes` feeds, handing every entity to
a deployer component supplied by the caller.

Requires Node 24 or newer.

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

### Tuning transfer limits

Bounds on individual HTTP transfers. Omit `transferLimits`, or any field within it, to keep the values
this package used before they were configurable — the defaults below are exactly those:

```ts
await createSynchronizer(components, {
  // …
  transferLimits: {
    requestTimeoutInMs: 15_000, // inactivity deadline on a JSON body read, refreshed per chunk
    downloadInactivityTimeoutInMs: 30_000, // the same, for a content-file download
    maxDownloadedFileSizeInBytes: 1024 * 1024 * 1024, // 1 GiB ceiling per content file
    minTransferRateInBytesPerSecond: 4096, // floor a transfer must average to continue
    transferRateGracePeriodInMs: 60_000 // how long before the rate is judged at all
  }
})
```

`minTransferRateInBytesPerSecond` is what stops a peer holding a slot by trickling bytes: the timeout
above is an *inactivity* deadline refreshed on every chunk, so it only asks whether bytes are still
arriving, never whether they add up to progress. Lower it on a constrained link, and raise
`transferRateGracePeriodInMs` alongside it if your peers are slow to get going. **Setting it to `0`
disables the check**, restoring the pre-`3.0.0` behaviour where an arbitrarily slow transfer continued
as long as it kept sending something.

Content downloads accept `Content-Encoding: gzip` case-insensitively, plus the legacy `x-gzip` alias.
Any other coding — `br`, `deflate`, or several layered together — is refused up front with a message
naming it, rather than being written undecoded and surfacing as a hash mismatch once the retries are
spent.

`maxDownloadedFileSizeInBytes` is applied on both sides of a gzip boundary: to the decompressed bytes,
which is the gzip-bomb bound, and to the compressed response as well. The second one matters because a
peer can stream valid gzip indefinitely while producing no decompressed output at all — concatenated
empty gzip members are legal — so a bound only on the decompressed side never gets a byte to measure. For
real content the compressed bound never binds: gzip exceeds its input only for incompressible data, and
then by about 0.03%.

`createSynchronizer` validates these up front, so a bad value is rejected at construction rather than
surfacing as a puzzling failure on one download later. Every field must be an integer; the two rate
fields accept `0`, the timeout and size cap require `>= 1`.

Downloads of the same hash are de-duplicated while one is in flight, keyed by the hash alone: a caller
that joins an existing transfer inherits the bounds of whoever started it. The hash is a content address,
so a second transfer under different bounds would spend real bandwidth reaching a byte-identical result.
Under `createSynchronizer` every caller threads the same options, so this only arises if you call the
download helpers directly with differing limits.

The same object is accepted by `getDeployedEntitiesStreamFromSnapshot`,
`getDeployedEntitiesStreamFromPointerChanges` (on their options) and `downloadEntityAndContentFiles` (as
its last argument). `DEFAULT_TRANSFER_LIMITS` and `resolveTransferLimits` are exported if you want to
read or derive from the defaults.

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

Remote servers are treated as untrusted, which constrains where requests may go:

- Redirects are followed only to **publicly routable** addresses. A redirect resolving to a private,
  loopback, link-local (including `169.254.169.254`) or unique-local address is refused, and a host
  whose resolved address changes public↔non-public mid-download is refused as DNS rebinding. The host
  you originally asked for is exempt, so a private catalyst — or loopback in local development — keeps
  working.
- JSON endpoints (`/snapshots`, `/pointer-changes`) **do not follow redirects at all** — a redirect
  response is refused. An origin check cannot help here: a URL origin is a hostname, not an address, so
  a host can answer the first request from a public address and rebind the name before the next one. The
  download path pins the resolved address instead, but the fetch component exposes no hook to do the
  same, so refusing redirects is the only complete answer on this path.
- Pagination follows a `next` link only within the **same origin and the same path** as the request that
  started the call, so a server can move the query string but cannot aim the next request at a different
  endpoint. Note this bounds *what* is requested, not *where* it resolves: DNS rebinding on the first
  request of a poll is not defended against on this path, and closing it needs a fetch component that
  can pin resolution.

## Shutdown

`synchronizer.stop()` waits for in-flight work to finish rather than only signalling it, so once it
resolves nothing is still deploying or mutating state. That includes your deployer: because
`scheduleEntityDeployment` may resolve before the entity is actually deployed, `stop()` awaits
`deployer.onIdle()` last, so an asynchronous or batching deployer is drained before it returns. Retry
ladders and poll intervals are abandoned when stopping, which bounds that wait at roughly one in-flight
request plus whatever your deployer still has queued — so give it room in your termination grace period.

For the same reason, `onIdle()` is awaited at the two bootstrap transitions as well: before a server's
last-entity timestamp is advanced after its snapshots, and before it is promoted from pointer-changes
bootstrap to syncing. Both of those decide where the server resumes from, so they must reflect entities
that were actually deployed rather than merely scheduled. A deployer whose `onIdle()` never resolves will
therefore stall the bootstrap, and one that rejects it leaves the affected servers in bootstrap to be
retried rather than dropping them.

Two consequences worth stating plainly, because your deployer decides both:

- **`markAsDeployed()` is load-bearing, not optional telemetry.** It is the only signal that an entity
  actually deployed. `onIdle()` resolving proves the queue drained, not that every entity reported back,
  so after it drains the snapshot's processed marker is re-read: a snapshot whose entities did not all
  report is treated as incomplete, and the servers advertising it stay in snapshot bootstrap with their
  timestamps held back. A deployer that never calls `markAsDeployed()` will therefore never finish
  bootstrapping — previously it would have advanced anyway and skipped those entities.
- **Deployments must be idempotent.** An incomplete snapshot is retried, so entities that did deploy are
  scheduled again on the next pass.

Anything you schedule through `createJobQueue({ timeout })` must be **idempotent**: a timed-out attempt
is retried, and because a promise cannot be cancelled the original keeps running alongside it.
`onIdle()` and `stop()` do wait for those abandoned executions, so quiescence remains accurate.

## Development

```bash
yarn install
yarn build        # or: make build
yarn test         # or: make test
yarn lint:check   # or: make lint
```

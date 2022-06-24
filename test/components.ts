// This file is the "test-environment" analogous for src/components.ts
// Here we define the test components to be used in the testing environment

import { createRunner } from '@well-known-components/test-helpers'
import { createJobQueue } from '../src/job-queue-port'
import { IIPFSComponent, SnapshotsFetcherComponents } from '../src/types'
import { createFetchComponent, createStorageComponent } from './test-component'

import {
  initTestServerComponents,
  TestServerComponents,
  wireTestServerComponents,
} from './functions-for-wkc-test-helpers'
import { createLogComponent } from '@well-known-components/logger'
import { createTestMetricsComponent } from '@well-known-components/metrics'
import { metricsDefinitions } from '../src/metrics'
import * as ipfsClient from 'ipfs-client'
import { createDotEnvConfigComponent } from '@well-known-components/env-config-provider'

// Record of components
export type TestComponents = SnapshotsFetcherComponents & TestServerComponents<SnapshotsFetcherComponents>

/**
 * Behaves like Jest "describe" function, used to describe a test for a
 * use case, it creates a whole new program and components to run an
 * isolated test.
 *
 * State is persistent within the steps of the test.
 */
export const test = createRunner<TestComponents>({
  async main({ startComponents, components }) {
    await wireTestServerComponents({ components })
    await startComponents()
  },
  async initComponents() {
    const config = await createDotEnvConfigComponent({ path: [".env.default", ".env"] })

    const fetcher = createFetchComponent()
    const downloadQueue = createJobQueue({
      autoStart: true,
      concurrency: 1,
      timeout: 100000,
    })
    const logs = createLogComponent()
    const metrics = createTestMetricsComponent(metricsDefinitions)
    const testServerComponents = await initTestServerComponents()
    const storage = await createStorageComponent()

    const user = await config.requireString("IPFS_USER")
    const password = await config.requireString("IPFS_PASSWORD")
    const base64 = Buffer.from(`${user}:${password}`).toString('base64')
    const ipfs: IIPFSComponent = await ipfsClient.create(
      {
        grpc: '/ipv4/127.0.0.1/tcp/5003/ws',
        http: 'https://peer-historical.decentraland.org/ipfs/api/v0',
        headers: { 'Authorization': `Basic ${base64}` }
      })

    return {
      ...testServerComponents,
      metrics,
      logs,
      downloadQueue,
      fetcher,
      storage,
      ipfs
    }
  },
})

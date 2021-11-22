// This file is the "test-environment" analogous for src/components.ts
// Here we define the test components to be used in the testing environment

import { createRunner } from '@well-known-components/test-helpers'
import { createJobQueue } from '../src/job-queue-port'
import { SnapshotsFetcherComponents } from '../src/types'
import { createFetchComponent } from './test-component'

import {
  initTestServerComponents,
  TestServerComponents,
  wireTestServerComponents,
} from './functions-for-wkc-test-helpers'

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
    const fetcher = createFetchComponent()
    const downloadQueue = createJobQueue({
      autoStart: true,
      concurrency: 1,
      timeout: 100000,
    })

    const testServerComponents = await initTestServerComponents()

    return {
      ...testServerComponents,
      downloadQueue,
      fetcher,
    }
  },
})

// This file is the "test-environment" analogous for src/components.ts
// Here we define the test components to be used in the testing environment

import { createRunner } from '@well-known-components/test-helpers'
import { createJobQueue } from '../src/job-queue-port'
import { SnapshotsFetcherComponents } from '../src/types'
import { createFetchComponent, createProcessedSnapshotStorageComponent, createStorageComponent } from './test-component'

import {
  initTestServerComponents,
  TestServerComponents,
  wireTestServerComponents,
} from './functions-for-wkc-test-helpers'
import { createLogComponent } from '@well-known-components/logger'
import { createTestMetricsComponent } from '@well-known-components/metrics'
import { metricsDefinitions } from '../src'
import { IConfigComponent } from "@well-known-components/interfaces"
import { createConfigComponent } from "@well-known-components/env-config-provider"

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
    const config: IConfigComponent = createConfigComponent({ ...process.env, LOG_LEVEL: "INFO" })
    const logs = await createLogComponent({ config })
    const metrics = createTestMetricsComponent(metricsDefinitions)
    const testServerComponents = await initTestServerComponents()
    const storage = await createStorageComponent()
    const processedSnapshotStorage = createProcessedSnapshotStorageComponent()
    const snapshotStorage = {
      async has(snapshotHash: string) {
        return false
      }
    }

    return {
      ...testServerComponents,
      metrics,
      logs,
      downloadQueue,
      fetcher,
      storage,
      processedSnapshotStorage,
      snapshotStorage
    }
  },
})

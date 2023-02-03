import { validateMetricsDeclaration } from '@well-known-components/metrics'

type ContentServerMetricLabelNames = 'remote_server'
export type ContentServerMetricLabels = Record<ContentServerMetricLabelNames, string>

/**
 * @public
 */
export const metricsDefinitions = validateMetricsDeclaration({
  dcl_content_download_bytes_total: {
    help: 'Total downloaded bytes from other catalysts',
    type: 'counter',
    labelNames: ['remote_server']
  },
  dcl_content_download_duration_seconds: {
    help: 'Total download time from other catalysts',
    type: 'histogram',
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
    labelNames: ['remote_server']
  },
  dcl_content_download_errors_total: {
    help: 'Total downloaded errors in requests',
    type: 'counter',
    labelNames: ['remote_server']
  },
  dcl_content_download_hash_errors_total: {
    help: 'Total hasing errors in downloaded files',
    type: 'counter',
    labelNames: ['remote_server']
  },

  dcl_entities_deployments_processed_total: {
    help: 'Entities processed from remote catalysts',
    type: 'counter',
    labelNames: ['remote_server', 'source']
  },

  dcl_entities_deployments_streamed_total: {
    help: 'Entities streamed from remote catalysts',
    type: 'counter',
    labelNames: ['remote_server', 'source'] // source=snapshots|pointer-changes
  },

  dcl_catalysts_pointer_changes_response_time_seconds: {
    help: 'Counts the connection of a deployment stream',
    type: 'histogram',
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
    labelNames: ['remote_server', 'status_code']
  },

  dcl_deployments_stream_reconnection_count: {
    help: 'Counts the connection of a deployment stream',
    type: 'counter',
    labelNames: ['remote_server']
  },

  dcl_deployments_stream_failure_count: {
    help: 'Counts the failures of a deployment stream',
    type: 'counter',
    labelNames: ['remote_server']
  },

  dcl_content_download_job_succeed_retries: {
    help: 'Summary of how much retries are required for a download job to succeed',
    type: 'histogram',
    buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30],
    labelNames: []
  },

  dcl_available_servers_histogram: {
    help: 'Histogram of available content servers in which a content file is present',
    type: 'histogram',
    buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    labelNames: []
  },

  dcl_bootstrapping_servers: {
    help: 'Servers that are in bootstrapping state',
    type: 'gauge',
    labelNames: ['from'] // from='snapshots'|'pointer-changes'
  },

  dcl_syncing_servers: {
    help: 'Servers that are in bootstrapping state',
    type: 'gauge'
  },

  dcl_processed_snapshots_total: {
    help: 'Total number of processed snapshots that started being streamed.',
    type: 'counter',
    labelNames: ['state'] // state='stream_start'|'stream_end'|'saved'
  }
})

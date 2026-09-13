'use strict';

const TRANSIENT_CODES = new Set(['META_TRANSPORT_FAILED', 'META_SERVICE_UNAVAILABLE', 'META_RATE_LIMITED']);

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function safeCode(error) {
  return error && TRANSIENT_CODES.has(error.safeCode) ? error.safeCode : 'META_REFRESH_FAILED';
}

function createMetaRefreshRunner({ datasetWriter, jobBoundary, telemetry = () => {}, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)), maxAttempts = 3 } = {}) {
  if (!datasetWriter || typeof datasetWriter.ingest !== 'function') throw new TypeError('Meta dataset writer is required');
  if (!jobBoundary || typeof jobBoundary.run !== 'function') throw new TypeError('refresh job boundary is required');
  if (typeof telemetry !== 'function' || typeof sleep !== 'function') throw new TypeError('telemetry and sleep must be functions');
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 5) throw new RangeError('maxAttempts must be an integer between 1 and 5');
  const emit = event => { try { telemetry(Object.freeze(event)); } catch {} };

  return Object.freeze({
    async run(input) {
      const userId = required(input?.context?.userId, 'context.userId');
      const accountId = required(input?.accountId, 'accountId');
      return jobBoundary.run({
        userId, platform: 'meta', platformAccountId: accountId,
        metadata: { jobType: 'meta_v2_refresh', retry_limit: maxAttempts },
        async work({ jobId }) {
          for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            emit({ event: 'meta_refresh_attempt', job_id: jobId, attempt, max_attempts: maxAttempts });
            try {
              const write = await datasetWriter.ingest(input);
              emit({ event: 'meta_refresh_completed', job_id: jobId, attempt, rows_written: write.persisted });
              return Object.freeze({ write, attempts: attempt });
            } catch (error) {
              const code = safeCode(error), retryable = TRANSIENT_CODES.has(code) && attempt < maxAttempts;
              emit({ event: retryable ? 'meta_refresh_retry' : 'meta_refresh_failed', job_id: jobId, attempt, safe_code: code });
              if (!retryable) { const failure = new Error('Meta refresh failed'); failure.safeCode = code; throw failure; }
              await sleep(100 * (2 ** (attempt - 1)));
            }
          }
          throw new Error('Meta refresh failed');
        },
        completed: result => ({ metadata: { job_type: 'meta_v2_refresh', attempts: result.attempts, rows_written: result.write.persisted } })
      });
    }
  });
}

module.exports = Object.freeze({ TRANSIENT_CODES, createMetaRefreshRunner });

'use strict';

const { WorkspaceCanonicalWriteBoundary } = require('../../../funnel-core/workspace-canonical-write-boundary');
const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { verifyProviderResult } = require('../workspace-provider-runtime');
const { closedProviderDate } = require('./read-only-preflight');
const { createKlaviyoWorkspaceRunner } = require('./workspace-runner');

const CONFIRMATION = 'RUN_R6_D2_C6_KLAVIYO_WRITE';
const SAFE_FAILURE_STAGES = new Set([
  'CONNECTION', 'CURRENCY', 'ACCOUNT_SELECTION', 'DATASET_GUARD', 'RUNTIME_CONTEXT',
  'TOKEN_REFRESH',
  'PROVIDER_ACCOUNT', 'PROVIDER_ACCOUNT_VALIDATION', 'PROVIDER_FACTS', 'PROVIDER_RESULT_VALIDATION',
  'FX_RESOLUTION', 'ROW_NORMALIZATION', 'RESULT_VERIFICATION', 'DATASET_PERSISTENCE', 'PERSISTENCE_CARDINALITY',
]);

function codedError(code, status) { return Object.assign(new Error(code), { code, status }); }
function diagnosticFailure(error, fallbackStage) {
  const reason = String(error?.code || error?.message || '');
  const safeCodes = new Map([
    ['KLAVIYO_REAUTHORIZE', ['KLAVIYO_DATASET_ACCEPTANCE_REAUTHORIZE', 409]],
    ['KLAVIYO_ACCESS_FORBIDDEN', ['KLAVIYO_DATASET_ACCEPTANCE_ACCESS_FORBIDDEN', 403]],
    ['KLAVIYO_PROVIDER_RATE_LIMITED', ['KLAVIYO_DATASET_ACCEPTANCE_PROVIDER_RATE_LIMITED', 503]],
    ['KLAVIYO_TOKEN_REFRESH_RATE_LIMITED', ['KLAVIYO_DATASET_ACCEPTANCE_TOKEN_REFRESH_RATE_LIMITED', 503]],
    ['KLAVIYO_TOKEN_REFRESH_UNAVAILABLE', ['KLAVIYO_DATASET_ACCEPTANCE_TOKEN_REFRESH_UNAVAILABLE', 503]],
    ['KLAVIYO_TOKEN_REFRESH_CONFIGURATION_FAILED', ['KLAVIYO_DATASET_ACCEPTANCE_TOKEN_REFRESH_CONFIGURATION_FAILED', 503]],
    ['KLAVIYO_TOKEN_REFRESH_RESPONSE_INVALID', ['KLAVIYO_DATASET_ACCEPTANCE_TOKEN_REFRESH_RESPONSE_INVALID', 503]],
    ['KLAVIYO_TOKEN_REFRESH_FAILED', ['KLAVIYO_DATASET_ACCEPTANCE_TOKEN_REFRESH_FAILED', 503]],
  ]);
  if (safeCodes.has(reason)) {
    const [code, status] = safeCodes.get(reason);
    return codedError(code, status);
  }
  const stage = SAFE_FAILURE_STAGES.has(error?.diagnosticStage) ? error.diagnosticStage : fallbackStage;
  return codedError(`KLAVIYO_DATASET_ACCEPTANCE_FAILED_${SAFE_FAILURE_STAGES.has(stage) ? stage : 'RUNTIME_CONTEXT'}`, 503);
}

function createKlaviyoControlledDatasetAcceptance({ connectionStore, settingsStore, providerClient, tokenLifecycle = null, resolveFxRate, repository, now = () => new Date() } = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  if (!repository || typeof repository.readCanonicalRawFacts !== 'function') throw new TypeError('workspace Dataset V2 read repository is required');
  const runner = createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate });
  const writeBoundary = new WorkspaceCanonicalWriteBoundary({ repository });

  async function execute(authorityInput, confirmation) {
    if (confirmation !== CONFIRMATION) throw codedError('KLAVIYO_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED', 409);
    const authority = requireServerWorkspaceAuthority(authorityInput);
    let stage = 'CONNECTION';
    try {
      let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      stage = 'CURRENCY';
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const providerDate = closedProviderDate(now());
      stage = 'ACCOUNT_SELECTION';
      const selectedAccounts = Array.isArray(connection.selectedAccounts) ? connection.selectedAccounts : [];
      if (selectedAccounts.length !== 1 || typeof selectedAccounts[0]?.id !== 'string' || !selectedAccounts[0].id.trim()) throw new Error('KLAVIYO_SINGLE_ACCOUNT_REQUIRED');
      stage = 'DATASET_GUARD';
      const existingRows = await repository.readCanonicalRawFacts({ workspace_id: authority.workspace_id, from: providerDate, to: providerDate, platform: 'klaviyo', platform_account_id: selectedAccounts[0].id.trim() });
      if (!Array.isArray(existingRows)) throw new Error('WORKSPACE_DATASET_READ_INVALID');
      if (existingRows.length > 0) throw codedError('KLAVIYO_DATASET_ACCEPTANCE_ALREADY_EXECUTED', 409);
      const operation = activeConnection => runner(Object.freeze({ authority, connection: activeConnection, reportingCurrency: currency.reportingCurrency, currencyVersion: currency.currencyVersion, request: Object.freeze({ provider_date: providerDate }) }));
      stage = tokenLifecycle ? 'TOKEN_REFRESH' : 'RUNTIME_CONTEXT';
      const execution = tokenLifecycle
        ? await tokenLifecycle.run({ authority, connection, operation })
        : { value: await operation(connection), connection };
      connection = execution.connection;
      const result = execution.value;
      stage = 'RESULT_VERIFICATION';
      const verified = verifyProviderResult({ provider: 'klaviyo', connection, reportingCurrency: currency.reportingCurrency, result });
      stage = 'DATASET_PERSISTENCE';
      const persisted = await writeBoundary.write(verified.rows);
      stage = 'PERSISTENCE_CARDINALITY';
      if (!Array.isArray(persisted) || persisted.length !== verified.rows.length) throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
      return Object.freeze({ status: 'PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE', attempted: verified.rows.length, persisted: persisted.length, empty_provider_result: verified.providerResultStatus === 'empty', provider_result_status: verified.providerResultStatus, selected_account_count: verified.selectedAccountCount, provider_date: providerDate, production_activation: false, currency_version: currency.currencyVersion });
    } catch (error) {
      if (error?.code === 'KLAVIYO_DATASET_ACCEPTANCE_ALREADY_EXECUTED') throw error;
      throw diagnosticFailure(error, stage);
    }
  }
  return Object.freeze({ execute });
}

module.exports = Object.freeze({ CONFIRMATION, SAFE_FAILURE_STAGES, createKlaviyoControlledDatasetAcceptance });

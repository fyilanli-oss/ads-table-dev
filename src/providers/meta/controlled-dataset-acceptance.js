'use strict';

const { WorkspaceCanonicalWriteBoundary } = require('../../../funnel-core/workspace-canonical-write-boundary');
const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { verifyProviderResult } = require('../workspace-provider-runtime');
const { validateToken } = require('./read-only-preflight');
const { createMetaWorkspaceRunner } = require('./workspace-runner');

const CONFIRMATION = 'RUN_R6_D3_E_META_WRITE';
const SAFE_FAILURE_STAGES = new Set([
  'CONNECTION', 'CURRENCY', 'ACCOUNT_SELECTION', 'DATASET_GUARD', 'PROVIDER_ACCOUNT',
  'PROVIDER_FACTS', 'FX_RESOLUTION', 'RESULT_VERIFICATION', 'DATASET_PERSISTENCE',
  'PERSISTENCE_CARDINALITY',
]);

function codedError(code, status) { return Object.assign(new Error(code), { code, status }); }

function diagnosticFailure(error, fallbackStage) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'META_PREFLIGHT_REAUTHORIZE') return codedError('META_DATASET_ACCEPTANCE_REAUTHORIZE', 409);
  const stage = SAFE_FAILURE_STAGES.has(error?.diagnosticStage) ? error.diagnosticStage : fallbackStage;
  return codedError(`META_DATASET_ACCEPTANCE_FAILED_${SAFE_FAILURE_STAGES.has(stage) ? stage : 'PROVIDER_FACTS'}`, 503);
}

function acceptanceDateWindow(now) {
  const end = new Date(now().getTime());
  const start = new Date(end.getTime());
  start.setUTCDate(start.getUTCDate() - 2);
  return Object.freeze({ from: start.toISOString().slice(0, 10), to: end.toISOString().slice(0, 10) });
}

function selectedAccountIds(connection) {
  const accounts = Array.isArray(connection?.selectedAccounts) ? connection.selectedAccounts : [];
  if (accounts.length < 1 || accounts.length > 3) throw new Error('META_SELECTED_ACCOUNTS_REQUIRED');
  const ids = accounts.map(account => typeof account?.id === 'string' ? account.id.trim() : '');
  if (ids.some(id => !id) || new Set(ids).size !== ids.length) throw new Error('META_SELECTED_ACCOUNTS_INVALID');
  return Object.freeze(ids);
}

function createMetaControlledDatasetAcceptance({ connectionStore, settingsStore, transport, graphVersion, resolveFxRate, repository, now = () => new Date() } = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  if (!repository || typeof repository.readCanonicalRawFacts !== 'function') throw new TypeError('workspace Dataset V2 read repository is required');
  const runner = createMetaWorkspaceRunner({ transport, graphVersion, resolveFxRate, now });
  const writeBoundary = new WorkspaceCanonicalWriteBoundary({ repository });

  async function execute(authorityInput, confirmation) {
    if (confirmation !== CONFIRMATION) throw codedError('META_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED', 409);
    const authority = requireServerWorkspaceAuthority(authorityInput);
    let stage = 'CONNECTION';
    try {
      const connection = await connectionStore.resolveConnected({ authority, provider: 'meta' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      validateToken(connection, now);
      stage = 'CURRENCY';
      const currency = await settingsStore.resolveReportingCurrency(authority);
      stage = 'ACCOUNT_SELECTION';
      const accountIds = selectedAccountIds(connection);
      stage = 'DATASET_GUARD';
      const window = acceptanceDateWindow(now);
      for (const accountId of accountIds) {
        const existingRows = await repository.readCanonicalRawFacts({
          workspace_id: authority.workspace_id,
          from: window.from,
          to: window.to,
          platform: 'meta',
          platform_account_id: accountId,
        });
        if (!Array.isArray(existingRows)) throw new Error('WORKSPACE_DATASET_READ_INVALID');
        if (existingRows.length > 0) throw codedError('META_DATASET_ACCEPTANCE_ALREADY_EXECUTED', 409);
      }
      stage = 'PROVIDER_FACTS';
      const result = await runner(Object.freeze({
        authority,
        connection,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({}),
      }));
      stage = 'RESULT_VERIFICATION';
      const verified = verifyProviderResult({ provider: 'meta', connection, reportingCurrency: currency.reportingCurrency, result });
      stage = 'DATASET_PERSISTENCE';
      const persisted = await writeBoundary.write(verified.rows);
      stage = 'PERSISTENCE_CARDINALITY';
      if (!Array.isArray(persisted) || persisted.length !== verified.rows.length) throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
      return Object.freeze({
        status: 'PASS_R6_D3_E_META_DATASET_WRITE',
        attempted: verified.rows.length,
        persisted: persisted.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        provider_date_strategy: 'previous_closed_business_date_per_account_timezone',
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      if (error?.code === 'META_DATASET_ACCEPTANCE_ALREADY_EXECUTED') throw error;
      throw diagnosticFailure(error, stage);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({
  CONFIRMATION,
  SAFE_FAILURE_STAGES,
  acceptanceDateWindow,
  createMetaControlledDatasetAcceptance,
});

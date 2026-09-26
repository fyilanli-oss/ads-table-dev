'use strict';

const {WorkspaceCanonicalWriteBoundary} = require('../../../funnel-core/workspace-canonical-write-boundary');
const {requireServerWorkspaceAuthority} = require('../../../funnel-core/workspace-dataset-runtime');
const {verifyProviderResult} = require('../workspace-provider-runtime');
const {selectedAccounts, createGoogleWorkspaceRunner} = require('./workspace-runner');

const CONFIRMATION = 'RUN_R6_D4_E_GOOGLE_WRITE';
const SAFE_FAILURE_STAGES = new Set([
  'CONNECTION', 'CURRENCY', 'ACCOUNT_SELECTION', 'DATASET_GUARD', 'TOKEN_LIFECYCLE',
  'PROVIDER_FACTS', 'RESULT_VERIFICATION', 'DATASET_PERSISTENCE', 'PERSISTENCE_CARDINALITY',
]);

function codedError(code, status) { return Object.assign(new Error(code), {code, status}); }

function preserveDiagnostics(target, source) {
  target.providerStage = source?.providerStage || null;
  target.upstreamStatus = source?.upstreamStatus ?? null;
  target.upstreamCode = source?.upstreamCode || null;
  target.upstreamRequestId = source?.upstreamRequestId || null;
  return target;
}

function diagnosticFailure(error, fallbackStage) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'GOOGLE_REAUTHORIZE') return preserveDiagnostics(codedError('GOOGLE_DATASET_ACCEPTANCE_REAUTHORIZE', 409), error);
  const stage = SAFE_FAILURE_STAGES.has(error?.diagnosticStage) ? error.diagnosticStage : fallbackStage;
  return preserveDiagnostics(codedError(`GOOGLE_DATASET_ACCEPTANCE_FAILED_${SAFE_FAILURE_STAGES.has(stage) ? stage : 'PROVIDER_FACTS'}`, 503), error);
}

function acceptanceDateWindow(now) {
  const end = new Date(now().getTime());
  const start = new Date(end.getTime());
  start.setUTCDate(start.getUTCDate() - 2);
  return Object.freeze({from: start.toISOString().slice(0, 10), to: end.toISOString().slice(0, 10)});
}

function createGoogleControlledDatasetAcceptance({connectionStore, settingsStore, tokenLifecycle, search, resolveFxRate, repository, now = () => new Date()} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  if (!tokenLifecycle || typeof tokenLifecycle.run !== 'function') throw new TypeError('Google token lifecycle is required');
  if (!repository || typeof repository.readCanonicalRawFacts !== 'function') throw new TypeError('workspace Dataset V2 read repository is required');
  const runner = createGoogleWorkspaceRunner({search, resolveFxRate, now});
  const writeBoundary = new WorkspaceCanonicalWriteBoundary({repository});

  async function execute(authorityInput, confirmation) {
    if (confirmation !== CONFIRMATION) throw codedError('GOOGLE_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED', 409);
    const authority = requireServerWorkspaceAuthority(authorityInput);
    let stage = 'CONNECTION';
    try {
      const connection = await connectionStore.resolveConnected({authority, provider: 'google_ads'});
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      stage = 'CURRENCY';
      const currency = await settingsStore.resolveReportingCurrency(authority);
      stage = 'ACCOUNT_SELECTION';
      const accounts = selectedAccounts(connection);
      stage = 'DATASET_GUARD';
      const window = acceptanceDateWindow(now);
      for (const account of accounts) {
        const existingRows = await repository.readCanonicalRawFacts({
          workspace_id: authority.workspace_id,
          from: window.from,
          to: window.to,
          platform: 'google',
          platform_account_id: account.id,
        });
        if (!Array.isArray(existingRows)) throw new Error('WORKSPACE_DATASET_READ_INVALID');
        if (existingRows.length > 0) throw codedError('GOOGLE_DATASET_ACCEPTANCE_ALREADY_EXECUTED', 409);
      }
      stage = 'TOKEN_LIFECYCLE';
      const lifecycle = await tokenLifecycle.run({
        authority,
        connection,
        operation: current => {
          stage = 'PROVIDER_FACTS';
          return runner({
            authority,
            connection: current,
            reportingCurrency: currency.reportingCurrency,
            currencyVersion: currency.currencyVersion,
            request: Object.freeze({}),
          });
        },
      });
      stage = 'RESULT_VERIFICATION';
      const verified = verifyProviderResult({
        provider: 'google_ads',
        connection: lifecycle.connection,
        reportingCurrency: currency.reportingCurrency,
        result: lifecycle.value,
      });
      stage = 'DATASET_PERSISTENCE';
      const persisted = await writeBoundary.write(verified.rows);
      stage = 'PERSISTENCE_CARDINALITY';
      if (!Array.isArray(persisted) || persisted.length !== verified.rows.length) throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
      return Object.freeze({
        status: 'PASS_R6_D4_E_GOOGLE_DATASET_WRITE',
        attempted: verified.rows.length,
        persisted: persisted.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        standard_row_count: lifecycle.value.standard_row_count,
        pmax_row_count: lifecycle.value.pmax_row_count,
        provider_date_strategy: 'previous_closed_business_date_per_customer_timezone',
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      if (error?.code === 'GOOGLE_DATASET_ACCEPTANCE_ALREADY_EXECUTED') throw error;
      throw diagnosticFailure(error, stage);
    }
  }

  return Object.freeze({execute});
}

module.exports = Object.freeze({
  CONFIRMATION,
  SAFE_FAILURE_STAGES,
  acceptanceDateWindow,
  createGoogleControlledDatasetAcceptance,
});


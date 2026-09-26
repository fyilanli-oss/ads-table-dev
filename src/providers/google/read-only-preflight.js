'use strict';

const { verifyProviderResult } = require('../workspace-provider-runtime');
const { createGoogleWorkspaceRunner } = require('./workspace-runner');

function codedError(code, status) { return Object.assign(new Error(code), {code, status}); }
function preserveDiagnostics(target, source) {
  target.providerStage = source?.providerStage || null;
  target.upstreamStatus = source?.upstreamStatus ?? null;
  target.upstreamCode = source?.upstreamCode || null;
  target.upstreamRequestId = source?.upstreamRequestId || null;
  return target;
}
function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'GOOGLE_CANONICAL_CONNECTION_REQUIRED') return codedError('GOOGLE_PREFLIGHT_CONNECTION_REQUIRED', 409);
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) return codedError('GOOGLE_PREFLIGHT_CURRENCY_REQUIRED', 409);
  if (reason === 'GOOGLE_REAUTHORIZE') return preserveDiagnostics(codedError(reason, 409), error);
  return preserveDiagnostics(codedError('GOOGLE_PREFLIGHT_FAILED', 503), error);
}

function createGoogleReadOnlyPreflight({connectionStore, settingsStore, tokenLifecycle, search, resolveFxRate, now = () => new Date()} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  if (!tokenLifecycle || typeof tokenLifecycle.run !== 'function') throw new TypeError('Google token lifecycle is required');
  const runner = createGoogleWorkspaceRunner({search, resolveFxRate, now});
  async function execute(authority) {
    try {
      const connection = await connectionStore.resolveConnected({authority, provider: 'google_ads'});
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const lifecycle = await tokenLifecycle.run({authority, connection, operation: current => runner({
        authority,
        connection: current,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({}),
      })});
      const verified = verifyProviderResult({provider: 'google_ads', connection: lifecycle.connection, reportingCurrency: currency.reportingCurrency, result: lifecycle.value});
      return Object.freeze({
        status: 'PASS_R6_D4_D_GOOGLE_READ_ONLY_PREFLIGHT',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        row_count: verified.rows.length,
        standard_row_count: lifecycle.value.standard_row_count,
        pmax_row_count: lifecycle.value.pmax_row_count,
        empty_provider_result: verified.providerResultStatus === 'empty',
        customer_metadata_verified: true,
        standard_and_pmax_verified: true,
        time_fx_verified: true,
        provider_date_strategy: 'previous_closed_business_date_per_customer_timezone',
        dataset_v2_write: false,
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) { throw safeFailure(error); }
  }
  return Object.freeze({execute});
}

module.exports = Object.freeze({createGoogleReadOnlyPreflight, preserveDiagnostics, safeFailure});


'use strict';

const { verifyProviderResult } = require('../workspace-provider-runtime');
const { createMetaWorkspaceRunner } = require('./workspace-runner');

function codedError(code, status) { return Object.assign(new Error(code), { code, status }); }

function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'META_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('META_PREFLIGHT_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) return codedError('META_PREFLIGHT_CURRENCY_REQUIRED', 409);
  if (reason === 'META_PREFLIGHT_REAUTHORIZE') return codedError(reason, 409);
  return codedError('META_PREFLIGHT_FAILED', 503);
}

function validateToken(connection, now) {
  if (!connection?.accessToken) throw codedError('META_PREFLIGHT_REAUTHORIZE', 409);
  const scopes = Array.isArray(connection.grantedScopes) ? connection.grantedScopes : [];
  if (!scopes.includes('ads_read')) throw codedError('META_PREFLIGHT_REAUTHORIZE', 409);
  const expiry = Date.parse(connection.accessTokenExpiresAt || '');
  if (!Number.isFinite(expiry) || expiry <= now().getTime()) throw codedError('META_PREFLIGHT_REAUTHORIZE', 409);
}

function createMetaReadOnlyPreflight({ connectionStore, settingsStore, transport, graphVersion, resolveFxRate, now = () => new Date() } = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  const runner = createMetaWorkspaceRunner({ transport, graphVersion, resolveFxRate, now });

  async function execute(authority) {
    try {
      const connection = await connectionStore.resolveConnected({ authority, provider: 'meta' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      validateToken(connection, now);
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const result = await runner(Object.freeze({
        authority,
        connection,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({}),
      }));
      const verified = verifyProviderResult({ provider: 'meta', connection, reportingCurrency: currency.reportingCurrency, result });
      return Object.freeze({
        status: 'PASS_R6_D3_D_META_READ_ONLY_PREFLIGHT',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        row_count: verified.rows.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        account_api_verified: true,
        insights_verified: true,
        time_fx_verified: true,
        provider_date_strategy: 'previous_closed_business_date_per_account_timezone',
        dataset_v2_write: false,
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) { throw safeFailure(error); }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({ createMetaReadOnlyPreflight, safeFailure, validateToken });

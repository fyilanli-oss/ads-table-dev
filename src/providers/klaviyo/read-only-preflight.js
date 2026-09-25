'use strict';

const { verifyProviderResult } = require('../workspace-provider-runtime');
const { createKlaviyoWorkspaceRunner } = require('./workspace-runner');

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

function closedProviderDate(now) {
  const instant = now instanceof Date ? now : new Date(now);
  if (Number.isNaN(instant.getTime())) throw new TypeError('now must be a valid Date');
  return new Date(instant.getTime() - (48 * 60 * 60 * 1000)).toISOString().slice(0, 10);
}

function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'KLAVIYO_REAUTHORIZE') return codedError('KLAVIYO_REAUTHORIZE', 409);
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'KLAVIYO_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) {
    return codedError('KLAVIYO_PREFLIGHT_CURRENCY_REQUIRED', 409);
  }
  return codedError('KLAVIYO_PREFLIGHT_FAILED', 503);
}

function createKlaviyoReadOnlyPreflight({
  connectionStore,
  settingsStore,
  providerClient,
  resolveFxRate,
  now = () => new Date(),
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') {
    throw new TypeError('canonical connection store is required');
  }
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') {
    throw new TypeError('workspace settings store is required');
  }
  const runner = createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate });

  async function execute(authority) {
    try {
      const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const providerDate = closedProviderDate(now());
      const result = await runner(Object.freeze({
        authority,
        connection,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({ provider_date: providerDate }),
      }));
      const verified = verifyProviderResult({
        provider: 'klaviyo',
        connection,
        reportingCurrency: currency.reportingCurrency,
        result,
      });
      return Object.freeze({
        status: 'PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        row_count: verified.rows.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        account_api_verified: true,
        campaign_reporting_verified: true,
        flow_reporting_verified: true,
        time_fx_verified: true,
        dataset_v2_write: false,
        production_activation: false,
        provider_date: providerDate,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      throw safeFailure(error);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({ closedProviderDate, createKlaviyoReadOnlyPreflight });


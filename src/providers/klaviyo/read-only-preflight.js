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

function resolveProviderDate(requestedProviderDate, now) {
  const latestClosedDate = closedProviderDate(now);
  if (requestedProviderDate === undefined || requestedProviderDate === null || requestedProviderDate === '') {
    return latestClosedDate;
  }
  if (typeof requestedProviderDate !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(requestedProviderDate)) {
    throw codedError('KLAVIYO_PROVIDER_DATE_INVALID', 409);
  }
  const parsed = new Date(`${requestedProviderDate}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== requestedProviderDate) {
    throw codedError('KLAVIYO_PROVIDER_DATE_INVALID', 409);
  }
  if (requestedProviderDate > latestClosedDate) {
    throw codedError('KLAVIYO_PROVIDER_DATE_NOT_CLOSED', 409);
  }
  const earliest = new Date(`${latestClosedDate}T00:00:00.000Z`);
  earliest.setUTCDate(earliest.getUTCDate() - 31);
  if (requestedProviderDate < earliest.toISOString().slice(0, 10)) {
    throw codedError('KLAVIYO_PROVIDER_DATE_OUT_OF_RANGE', 409);
  }
  return requestedProviderDate;
}

function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (['KLAVIYO_PROVIDER_DATE_INVALID', 'KLAVIYO_PROVIDER_DATE_NOT_CLOSED', 'KLAVIYO_PROVIDER_DATE_OUT_OF_RANGE'].includes(reason)) {
    return codedError(reason, 409);
  }
  if (reason === 'KLAVIYO_REAUTHORIZE') return codedError('KLAVIYO_REAUTHORIZE', 409);
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'KLAVIYO_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) {
    return codedError('KLAVIYO_PREFLIGHT_CURRENCY_REQUIRED', 409);
  }
  if (reason === 'connection.conversionMetric.id is required' || reason.startsWith('connection.journeyMetrics.')) {
    return codedError('KLAVIYO_PREFLIGHT_METRIC_REQUIRED', 409);
  }
  return codedError('KLAVIYO_PREFLIGHT_FAILED', 503);
}

function safeDiagnosticFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (['KLAVIYO_PROVIDER_DATE_INVALID', 'KLAVIYO_PROVIDER_DATE_NOT_CLOSED', 'KLAVIYO_PROVIDER_DATE_OUT_OF_RANGE'].includes(reason)) {
    return codedError(reason, 409);
  }
  if (reason === 'KLAVIYO_REAUTHORIZE') return codedError('KLAVIYO_REAUTHORIZE', 409);
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'KLAVIYO_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('KLAVIYO_JOURNEY_DIAGNOSTIC_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) {
    return codedError('KLAVIYO_JOURNEY_DIAGNOSTIC_CURRENCY_REQUIRED', 409);
  }
  if (reason === 'connection.conversionMetric.id is required' || reason.startsWith('connection.journeyMetrics.')) {
    return codedError('KLAVIYO_JOURNEY_DIAGNOSTIC_METRIC_REQUIRED', 409);
  }
  return codedError('KLAVIYO_JOURNEY_DIAGNOSTIC_FAILED', 503);
}

function createKlaviyoReadOnlyPreflight({
  connectionStore,
  settingsStore,
  providerClient,
  tokenLifecycle = null,
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

  async function execute(authority, requestedProviderDate = null) {
    try {
      let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const providerDate = resolveProviderDate(requestedProviderDate, now());
      const operation = activeConnection => runner(Object.freeze({
          authority,
          connection: activeConnection,
          reportingCurrency: currency.reportingCurrency,
          currencyVersion: currency.currencyVersion,
          request: Object.freeze({ provider_date: providerDate }),
        }));
      const execution = tokenLifecycle
        ? await tokenLifecycle.run({ authority, connection, operation })
        : { value: await operation(connection), connection };
      connection = execution.connection;
      const result = execution.value;
      const verified = verifyProviderResult({
        provider: 'klaviyo',
        connection,
        reportingCurrency: currency.reportingCurrency,
        result,
      });
      const campaignRowCount = verified.rows.filter(row => row.entity.root_entity_type === 'campaign').length;
      const flowRowCount = verified.rows.filter(row => row.entity.root_entity_type === 'flow').length;
      return Object.freeze({
        status: 'PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        row_count: verified.rows.length,
        campaign_row_count: campaignRowCount,
        flow_row_count: flowRowCount,
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

  async function executeDiagnostic(authority, requestedProviderDate = null) {
    try {
      let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const providerDate = resolveProviderDate(requestedProviderDate, now());
      const operation = activeConnection => runner(Object.freeze({
        authority,
        connection: activeConnection,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({ provider_date: providerDate, include_diagnostics: true }),
      }));
      const execution = tokenLifecycle
        ? await tokenLifecycle.run({ authority, connection, operation })
        : { value: await operation(connection), connection };
      connection = execution.connection;
      const result = execution.value;
      const verified = verifyProviderResult({
        provider: 'klaviyo',
        connection,
        reportingCurrency: currency.reportingCurrency,
        result,
      });
      return Object.freeze({
        status: 'PASS_R6_D5_KLAVIYO_JOURNEY_DIAGNOSTIC',
        provider_date: providerDate,
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        row_count: verified.rows.length,
        campaign_row_count: verified.rows.filter(row => row.entity.root_entity_type === 'campaign').length,
        flow_row_count: verified.rows.filter(row => row.entity.root_entity_type === 'flow').length,
        journey_diagnostics: result.provider_diagnostics,
        dataset_v2_write: false,
        production_activation: false,
      });
    } catch (error) {
      throw safeDiagnosticFailure(error);
    }
  }

  return Object.freeze({ execute, executeDiagnostic });
}

module.exports = Object.freeze({ closedProviderDate, resolveProviderDate, createKlaviyoReadOnlyPreflight });




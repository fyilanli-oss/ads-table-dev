'use strict';

const {
  requireServerWorkspaceAuthority,
  rejectCallerTenantFields,
} = require('../../funnel-core/workspace-dataset-runtime');

const ACTIVE_PROVIDERS = new Set(['meta', 'google_ads', 'klaviyo']);
const CANONICAL_PLATFORM = Object.freeze({ meta: 'meta', google_ads: 'google', klaviyo: 'klaviyo' });

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function activeProvider(value) {
  const provider = required(value, 'provider');
  if (!ACTIVE_PROVIDERS.has(provider)) throw new Error('PROVIDER_NOT_ACTIVE_IN_R6');
  return provider;
}

function selectedAccountMap(connection, provider) {
  const accounts = Array.isArray(connection.selectedAccounts) ? connection.selectedAccounts : [];
  const fallback = connection.activeAccountId ? [{
    id: connection.activeAccountId,
    currency: connection.sourceCurrency,
  }] : [];
  const source = accounts.length > 0 ? accounts : fallback;
  const maximum = provider === 'klaviyo' ? 1 : 3;
  if (source.length < 1 || source.length > maximum) throw new Error('CANONICAL_SELECTED_ACCOUNTS_INVALID');
  const mapped = new Map();
  for (const account of source) {
    const id = required(account?.id, 'selected_account.id');
    const currency = required(account?.currency, 'selected_account.currency').toUpperCase();
    if (mapped.has(id)) throw new Error('CANONICAL_SELECTED_ACCOUNTS_INVALID');
    mapped.set(id, currency);
  }
  return mapped;
}

function verifyProviderResult({ provider, connection, reportingCurrency, result } = {}) {
  if (!result || !Array.isArray(result.rows)) throw new Error('PROVIDER_RUNTIME_ROWS_REQUIRED');
  const selected = selectedAccountMap(connection, provider);
  const checked = Array.isArray(result.checked_account_ids) ? result.checked_account_ids : [];
  if (checked.length !== selected.size || new Set(checked).size !== checked.length ||
    checked.some(accountId => !selected.has(accountId))) {
    throw new Error('PROVIDER_RUNTIME_ACCOUNT_COVERAGE_INVALID');
  }
  const expectedStatus = result.rows.length === 0 ? 'empty' : 'non_empty';
  if (result.provider_result_status !== expectedStatus) throw new Error('PROVIDER_RUNTIME_RESULT_NOT_VERIFIED');
  const target = required(reportingCurrency, 'reportingCurrency').toUpperCase();
  const canonicalPlatform = CANONICAL_PLATFORM[provider];
  for (const row of result.rows) {
    const accountId = required(row?.identity?.platform_account_id, 'row.identity.platform_account_id');
    if (!selected.has(accountId)) throw new Error('PROVIDER_RUNTIME_UNSELECTED_ACCOUNT_ROW');
    if (row.identity.platform !== canonicalPlatform) throw new Error('PROVIDER_RUNTIME_PLATFORM_MISMATCH');
    if (row?.provenance?.synthetic !== false) throw new Error('PROVIDER_RUNTIME_SYNTHETIC_ROW_REJECTED');
    if (required(row?.currency?.source_currency, 'row.currency.source_currency').toUpperCase() !== selected.get(accountId)) {
      throw new Error('PROVIDER_RUNTIME_SOURCE_CURRENCY_MISMATCH');
    }
    if (required(row?.currency?.target_currency, 'row.currency.target_currency').toUpperCase() !== target) {
      throw new Error('PROVIDER_RUNTIME_TARGET_CURRENCY_MISMATCH');
    }
    required(row?.time?.source_timezone, 'row.time.source_timezone');
    required(row?.time?.business_date, 'row.time.business_date');
  }
  return Object.freeze({ rows: result.rows, selectedAccountCount: selected.size, providerResultStatus: expectedStatus });
}

function createWorkspaceProviderRuntime({
  resolveAuthority,
  connectionStore,
  settingsStore,
  datasetRuntime,
  runners = {},
} = {}) {
  if (typeof resolveAuthority !== 'function') throw new TypeError('workspace authority resolver is required');
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') {
    throw new TypeError('canonical connection store is required');
  }
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') {
    throw new TypeError('workspace settings store is required');
  }
  if (!datasetRuntime || typeof datasetRuntime.write !== 'function') {
    throw new TypeError('workspace Dataset V2 runtime is required');
  }

  async function run({ authority_input, provider: providerInput, request = {} } = {}) {
    rejectCallerTenantFields(authority_input, 'authority_input');
    rejectCallerTenantFields(request, 'provider_request');
    const provider = activeProvider(providerInput);
    const authority = requireServerWorkspaceAuthority(await resolveAuthority(authority_input));
    const connection = await connectionStore.resolveConnected({ authority, provider });
    if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
    if (connection.provider !== provider || connection.status !== 'connected') {
      throw new Error('CANONICAL_PROVIDER_CONNECTION_INVALID');
    }
    const currency = await settingsStore.resolveReportingCurrency(authority);
    const runner = runners[provider];
    if (typeof runner !== 'function') throw new Error('PROVIDER_RUNTIME_NOT_READY');

    const result = await runner(Object.freeze({
      authority,
      connection,
      reportingCurrency: currency.reportingCurrency,
      currencyVersion: currency.currencyVersion,
      request: Object.freeze({ ...request }),
    }));
    const verified = verifyProviderResult({
      provider,
      connection,
      reportingCurrency: currency.reportingCurrency,
      result,
    });
    const persisted = await datasetRuntime.write({ authority_input, rows: verified.rows });
    if (!Array.isArray(persisted) || persisted.length !== verified.rows.length) {
      throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
    }
    return Object.freeze({
      provider,
      workspace_id: authority.workspace_id,
      attempted: verified.rows.length,
      persisted: persisted.length,
      empty_provider_result: verified.providerResultStatus === 'empty',
      selected_account_count: verified.selectedAccountCount,
      provider_result_status: verified.providerResultStatus,
      production_activation: false,
      currency_version: currency.currencyVersion,
    });
  }

  return Object.freeze({ run });
}

module.exports = Object.freeze({ ACTIVE_PROVIDERS, CANONICAL_PLATFORM, verifyProviderResult, createWorkspaceProviderRuntime });


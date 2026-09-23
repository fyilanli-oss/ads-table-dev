'use strict';

const {
  requireServerWorkspaceAuthority,
  rejectCallerTenantFields,
} = require('../../funnel-core/workspace-dataset-runtime');

const ACTIVE_PROVIDERS = new Set(['meta', 'google_ads', 'klaviyo']);

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function activeProvider(value) {
  const provider = required(value, 'provider');
  if (!ACTIVE_PROVIDERS.has(provider)) throw new Error('PROVIDER_NOT_ACTIVE_IN_R6');
  return provider;
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
    if (!result || !Array.isArray(result.rows)) throw new Error('PROVIDER_RUNTIME_ROWS_REQUIRED');
    const persisted = await datasetRuntime.write({ authority_input, rows: result.rows });
    if (!Array.isArray(persisted) || persisted.length !== result.rows.length) {
      throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
    }
    return Object.freeze({
      provider,
      workspace_id: authority.workspace_id,
      attempted: result.rows.length,
      persisted: persisted.length,
      empty_provider_result: result.rows.length === 0,
      currency_version: currency.currencyVersion,
    });
  }

  return Object.freeze({ run });
}

module.exports = Object.freeze({ ACTIVE_PROVIDERS, createWorkspaceProviderRuntime });


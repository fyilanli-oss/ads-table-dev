'use strict';

const { requireServerWorkspaceAuthority } = require('../../funnel-core/workspace-dataset-runtime');
const { normalizeCurrencyCode } = require('../../funnel-core/fx-service');

const TABLE = 'workspace_settings';
const SUPPORTED_REPORTING_CURRENCIES = Object.freeze([
  'USD', 'EUR', 'TRY', 'GBP', 'JPY', 'CNY', 'AUD', 'CAD', 'CHF', 'SEK', 'NOK', 'DKK', 'PLN'
]);

function createWorkspaceSettingsStore({ client } = {}) {
  if (!client || typeof client.from !== 'function') {
    throw new TypeError('Supabase server client is required');
  }

  async function resolveReportingCurrency(authorityInput) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const { data, error } = await client.from(TABLE)
      .select('reporting_currency,reporting_currency_source,reporting_currency_version')
      .eq('workspace_id', authority.workspace_id)
      .maybeSingle();
    if (error) throw new Error('WORKSPACE_SETTINGS_READ_FAILED');
    if (!data) throw new Error('WORKSPACE_REPORTING_CURRENCY_REQUIRED');
    if (data.reporting_currency_source !== 'merchant_selected') {
      throw new Error('WORKSPACE_REPORTING_CURRENCY_SOURCE_INVALID');
    }
    if (!Number.isInteger(data.reporting_currency_version) || data.reporting_currency_version < 1) {
      throw new Error('WORKSPACE_REPORTING_CURRENCY_VERSION_INVALID');
    }
    return Object.freeze({
      reportingCurrency: normalizeCurrencyCode(data.reporting_currency, 'reporting_currency'),
      currencyVersion: data.reporting_currency_version,
      source: data.reporting_currency_source,
    });
  }

  async function readStatus(authorityInput) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const { data, error } = await client.from(TABLE)
      .select('reporting_currency,reporting_currency_source,reporting_currency_version')
      .eq('workspace_id', authority.workspace_id)
      .maybeSingle();
    if (error) throw new Error('WORKSPACE_SETTINGS_READ_FAILED');
    if (!data) return Object.freeze({ status: 'currency_required', supported: SUPPORTED_REPORTING_CURRENCIES });
    if (data.reporting_currency_source !== 'merchant_selected') throw new Error('WORKSPACE_REPORTING_CURRENCY_SOURCE_INVALID');
    return Object.freeze({
      status: 'configured',
      reporting_currency: normalizeCurrencyCode(data.reporting_currency, 'reporting_currency'),
      reporting_currency_version: data.reporting_currency_version,
      supported: SUPPORTED_REPORTING_CURRENCIES,
    });
  }

  async function selectReportingCurrency({ authority: authorityInput, currency } = {}) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    let normalized;
    try { normalized = normalizeCurrencyCode(currency, 'reporting_currency'); }
    catch { throw new Error('REPORTING_CURRENCY_NOT_SUPPORTED'); }
    if (!SUPPORTED_REPORTING_CURRENCIES.includes(normalized)) throw new Error('REPORTING_CURRENCY_NOT_SUPPORTED');
    const { data, error } = await client.from(TABLE).insert({
      workspace_id: authority.workspace_id,
      reporting_currency: normalized,
      reporting_currency_source: 'merchant_selected',
    }).select('reporting_currency,reporting_currency_source,reporting_currency_version').maybeSingle();
    if (error?.code === '23505') throw new Error('REPORTING_CURRENCY_ALREADY_CONFIGURED');
    if (error) throw new Error('WORKSPACE_SETTINGS_WRITE_FAILED');
    return Object.freeze({
      status: 'configured',
      reporting_currency: data.reporting_currency,
      reporting_currency_version: data.reporting_currency_version,
    });
  }

  return Object.freeze({ readStatus, selectReportingCurrency, resolveReportingCurrency });
}

module.exports = Object.freeze({ TABLE, SUPPORTED_REPORTING_CURRENCIES, createWorkspaceSettingsStore });

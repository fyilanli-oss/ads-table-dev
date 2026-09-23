'use strict';

const { requireServerWorkspaceAuthority } = require('../../funnel-core/workspace-dataset-runtime');
const { normalizeCurrencyCode } = require('../../funnel-core/fx-service');

const TABLE = 'workspace_settings';

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

  return Object.freeze({ resolveReportingCurrency });
}

module.exports = Object.freeze({ TABLE, createWorkspaceSettingsStore });


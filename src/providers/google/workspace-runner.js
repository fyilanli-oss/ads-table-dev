'use strict';

const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { normalizeCurrencyCode, normalizeMonetaryRawFields } = require('../../../funnel-core/fx-service');
const { createGoogleAdapter } = require('./adapter');
const { GOOGLE_CUSTOMER_METADATA_QUERY, normalizeGoogleCustomerMetadata } = require('./account-metadata');
const { mergeConversions, querySet } = require('./live-refresh');

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(`${field} is required`);
  return value.trim();
}

function selectedAccounts(connection) {
  const source = Array.isArray(connection?.selectedAccounts) ? connection.selectedAccounts : [];
  if (source.length < 1 || source.length > 3) throw new Error('GOOGLE_SELECTED_ACCOUNTS_REQUIRED');
  const seen = new Set();
  return Object.freeze(source.map(account => {
    const id = required(account?.id, 'selected_account.id');
    const loginCustomerId = required(account?.login_customer_id || account?.loginCustomerId, 'selected_account.login_customer_id');
    if (!/^\d+$/.test(id) || !/^\d+$/.test(loginCustomerId) || seen.has(id)) throw new Error('GOOGLE_SELECTED_ACCOUNTS_INVALID');
    seen.add(id);
    return Object.freeze({id, loginCustomerId, currency: normalizeCurrencyCode(account.currency, 'selected_account.currency')});
  }));
}

function previousDate(value) {
  const date = new Date(`${required(value, 'customer.business_date')}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() - 1);
  return date.toISOString().slice(0, 10);
}

function createGoogleWorkspaceRunner({search, resolveFxRate, now = () => new Date()} = {}) {
  if (typeof search !== 'function') throw new TypeError('Google search is required');
  if (typeof resolveFxRate !== 'function') throw new TypeError('FX resolver is required');
  return async function runGoogleWorkspace(context = {}) {
    const authority = requireServerWorkspaceAuthority(context.authority);
    const connection = context.connection;
    if (!connection || connection.provider !== 'google_ads' || connection.status !== 'connected') throw new Error('GOOGLE_CANONICAL_CONNECTION_REQUIRED');
    const accounts = selectedAccounts(connection);
    const reportingCurrency = normalizeCurrencyCode(context.reportingCurrency, 'reportingCurrency');
    const rows = [];
    let standardRows = 0;
    let pmaxRows = 0;

    for (const selected of accounts) {
      const request = query => search({
        accessToken: required(connection.accessToken, 'connection.accessToken'),
        customerId: selected.id,
        loginCustomerId: selected.loginCustomerId,
        query,
      });
      const observed = normalizeGoogleCustomerMetadata(await request(GOOGLE_CUSTOMER_METADATA_QUERY), {requestedCustomerId: selected.id, observedAt: now()});
      if (observed.source_currency !== selected.currency) throw new Error('GOOGLE_PROVIDER_CURRENCY_MISMATCH');
      const customer = Object.freeze({...observed, business_date: previousDate(observed.business_date)});
      const fx = await resolveFxRate(customer.source_currency, reportingCurrency, {rateDate: customer.business_date});
      const client = {
        fetchStandardAdRows: async () => {
          const queries = querySet('standard', customer.business_date);
          return {results: mergeConversions((await request(queries.performance)).results, (await request(queries.conversions)).results, 'standard')};
        },
        fetchPmaxAssetGroupRows: async () => {
          const queries = querySet('performance_max', customer.business_date);
          return {results: mergeConversions((await request(queries.performance)).results, (await request(queries.conversions)).results, 'performance_max')};
        },
      };
      const adapter = createGoogleAdapter({client});
      for (const campaignType of ['standard', 'performance_max']) {
        const mapped = await adapter.fetchCanonicalRows({campaignType, context: {workspaceId: authority.workspace_id, customer}});
        if (campaignType === 'standard') standardRows += mapped.length; else pmaxRows += mapped.length;
        rows.push(...mapped.map(result => normalizeMonetaryRawFields(result.row, {
          sourceCurrency: customer.source_currency,
          targetCurrency: reportingCurrency,
          fxRate: fx.fx_rate,
          fxRateDate: fx.fx_rate_date || customer.business_date,
          fxProvider: fx.fx_provider,
        })));
      }
    }
    return Object.freeze({
      rows: Object.freeze(rows),
      checked_account_ids: Object.freeze(accounts.map(account => account.id)),
      provider_result_status: rows.length === 0 ? 'empty' : 'non_empty',
      standard_row_count: standardRows,
      pmax_row_count: pmaxRows,
    });
  };
}

module.exports = Object.freeze({createGoogleWorkspaceRunner, previousDate, selectedAccounts});


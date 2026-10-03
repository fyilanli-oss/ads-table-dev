'use strict';

const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { normalizeCurrencyCode, normalizeMonetaryRawFields } = require('../../../funnel-core/fx-service');
const { createGoogleAdapter } = require('./adapter');
const { GOOGLE_CUSTOMER_METADATA_QUERY, normalizeGoogleCustomerMetadata } = require('./account-metadata');
const { mergeConversions, querySet } = require('./live-refresh');

const MAX_EVIDENCE_LOOKBACK_DAYS = 31;

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

function subtractDays(value, days) {
  const date = new Date(`${required(value, 'provider_date')}T00:00:00.000Z`);
  if (!Number.isInteger(days) || days < 0) throw new Error('GOOGLE_LOOKBACK_DAYS_INVALID');
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

function evidenceRequest(input) {
  if (input?.providerResponseEvidence !== true) return null;
  const lookbackDays = input.lookbackDays;
  if (!Number.isInteger(lookbackDays) || lookbackDays < 1 || lookbackDays > MAX_EVIDENCE_LOOKBACK_DAYS) {
    throw new Error('GOOGLE_EVIDENCE_LOOKBACK_INVALID');
  }
  return Object.freeze({lookbackDays});
}

function queryShape(query) {
  const normalized = required(query, 'query').replace(/\s+/g, ' ').trim();
  const select = normalized.match(/^SELECT (.+?) FROM /i)?.[1] || '';
  const resource = normalized.match(/ FROM ([a-z_]+)/i)?.[1] || 'unknown';
  return Object.freeze({
    resource,
    selected_fields: Object.freeze(select.split(',').map(value => value.trim()).filter(value => /^[a-z0-9_.]+$/i.test(value))),
  });
}

function safeQueryEvidence({accountOrdinal, label, query, response}) {
  const shape = queryShape(query);
  const provider = response?.response_evidence || {};
  return Object.freeze({
    account_ordinal: accountOrdinal,
    label,
    resource: shape.resource,
    selected_fields: shape.selected_fields,
    result_count: Array.isArray(response?.results) ? response.results.length : 0,
    stream_chunk_count: Number.isInteger(provider.stream_chunk_count) ? provider.stream_chunk_count : null,
    chunks_with_results: Number.isInteger(provider.chunks_with_results) ? provider.chunks_with_results : null,
    field_mask_paths: Object.freeze(Array.isArray(provider.field_mask_paths) ? provider.field_mask_paths : []),
    raw_response_status: typeof provider.raw_response_status === 'string' ? provider.raw_response_status : 'unavailable',
    raw_response_body: typeof provider.raw_response_body === 'string' ? provider.raw_response_body : null,
  });
}

function evidenceQuerySet(campaignType, from, to) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) throw new Error('Google evidence date range is invalid');
  const date = `segments.date BETWEEN '${from}' AND '${to}'`;
  if (campaignType === 'standard') return Object.freeze({
    structure: "SELECT campaign.id, campaign.name, campaign.advertising_channel_type, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_ad.ad.id, ad_group_ad.status FROM ad_group_ad WHERE campaign.advertising_channel_type != 'PERFORMANCE_MAX' LIMIT 10000",
    performance: `SELECT segments.date, campaign.id, ad_group.id, ad_group_ad.ad.id, metrics.impressions, metrics.clicks, metrics.cost_micros FROM ad_group_ad WHERE ${date} AND campaign.advertising_channel_type != 'PERFORMANCE_MAX' LIMIT 10000`,
    conversions: `SELECT segments.date, campaign.id, ad_group_ad.ad.id, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value FROM ad_group_ad WHERE ${date} AND campaign.advertising_channel_type != 'PERFORMANCE_MAX' AND metrics.all_conversions > 0 LIMIT 10000`,
  });
  if (campaignType === 'performance_max') return Object.freeze({
    structure: "SELECT campaign.id, campaign.name, campaign.advertising_channel_type, campaign.status, asset_group.id, asset_group.name, asset_group.status FROM asset_group WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX' LIMIT 10000",
    performance: `SELECT segments.date, campaign.id, asset_group.id, metrics.impressions, metrics.clicks, metrics.cost_micros FROM asset_group WHERE ${date} AND campaign.advertising_channel_type = 'PERFORMANCE_MAX' LIMIT 10000`,
    conversions: `SELECT segments.date, campaign.id, asset_group.id, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value FROM asset_group WHERE ${date} AND campaign.advertising_channel_type = 'PERFORMANCE_MAX' AND metrics.all_conversions > 0 LIMIT 10000`,
  });
  throw new Error('Unsupported Google evidence campaign type');
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
    const evidence = evidenceRequest(context.request);
    const rows = [];
    const providerResponseEvidence = [];
    let standardRows = 0;
    let pmaxRows = 0;

    for (const [accountIndex, selected] of accounts.entries()) {
      const request = async (query, label = null) => {
        const response = await search({
          accessToken: required(connection.accessToken, 'connection.accessToken'),
          customerId: selected.id,
          loginCustomerId: selected.loginCustomerId,
          query,
          captureRawResponse: Boolean(label && label !== 'customer_metadata'),
        });
        if (label) providerResponseEvidence.push(safeQueryEvidence({accountOrdinal: accountIndex + 1, label, query, response}));
        return response;
      };
      const observed = normalizeGoogleCustomerMetadata(await request(GOOGLE_CUSTOMER_METADATA_QUERY, evidence ? 'customer_metadata' : null), {requestedCustomerId: selected.id, observedAt: now()});
      if (observed.source_currency !== selected.currency) throw new Error('GOOGLE_PROVIDER_CURRENCY_MISMATCH');
      const customer = Object.freeze({...observed, business_date: previousDate(observed.business_date)});

      if (evidence) {
        const from = subtractDays(customer.business_date, evidence.lookbackDays - 1);
        for (const campaignType of ['standard', 'performance_max']) {
          const queries = evidenceQuerySet(campaignType, from, customer.business_date);
          await request(queries.structure, `${campaignType}.structure`);
          await request(queries.performance, `${campaignType}.history_performance`);
          await request(queries.conversions, `${campaignType}.history_conversions`);
        }
      }

      const fx = await resolveFxRate(customer.source_currency, reportingCurrency, {rateDate: customer.business_date});
      const client = {
        fetchStandardAdRows: async () => {
          const queries = querySet('standard', customer.business_date);
          const performance = await request(queries.performance, evidence ? 'standard.previous_day_performance' : null);
          const conversions = await request(queries.conversions, evidence ? 'standard.previous_day_conversions' : null);
          return {results: mergeConversions(performance.results, conversions.results, 'standard')};
        },
        fetchPmaxAssetGroupRows: async () => {
          const queries = querySet('performance_max', customer.business_date);
          const performance = await request(queries.performance, evidence ? 'performance_max.previous_day_performance' : null);
          const conversions = await request(queries.conversions, evidence ? 'performance_max.previous_day_conversions' : null);
          return {results: mergeConversions(performance.results, conversions.results, 'performance_max')};
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
      provider_response_evidence: Object.freeze(providerResponseEvidence),
    });
  };
}

module.exports = Object.freeze({
  MAX_EVIDENCE_LOOKBACK_DAYS,
  createGoogleWorkspaceRunner,
  evidenceQuerySet,
  evidenceRequest,
  previousDate,
  queryShape,
  safeQueryEvidence,
  selectedAccounts,
  subtractDays,
});

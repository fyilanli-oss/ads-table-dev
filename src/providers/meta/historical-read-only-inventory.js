'use strict';

const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');
const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { createMetaAdapter } = require('./adapter');
const { createMetaClient } = require('./client');
const { validateToken } = require('./read-only-preflight');
const { previousClosedBusinessDate, selectedAccounts } = require('./workspace-runner');

const DEFAULT_LOOKBACK_DAYS = 31;

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

function subtractDays(value, days) {
  const date = new Date(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(date.getTime())) throw new Error('META_HISTORICAL_DATE_INVALID');
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'META_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('META_HISTORICAL_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) return codedError('META_HISTORICAL_CURRENCY_REQUIRED', 409);
  if (reason === 'META_PREFLIGHT_REAUTHORIZE') return codedError('META_HISTORICAL_REAUTHORIZE', 409);
  return codedError('META_HISTORICAL_INVENTORY_FAILED', 503);
}

function metricTotal(rows, key) {
  return rows.reduce((total, row) => {
    const value = row?.raw_metrics?.[key];
    return total + (typeof value === 'number' && Number.isFinite(value) ? value : 0);
  }, 0);
}

function supportSummary(rows, key) {
  return Object.freeze({
    supported_rows: rows.filter(row => row?.metric_support?.[key] === 'supported').length,
    unknown_rows: rows.filter(row => row?.metric_support?.[key] === 'unknown').length,
    unsupported_rows: rows.filter(row => row?.metric_support?.[key] === 'unsupported').length,
  });
}

function createMetaHistoricalReadOnlyInventory({
  connectionStore,
  settingsStore,
  transport,
  graphVersion,
  resolveFxRate,
  now = () => new Date(),
  lookbackDays = DEFAULT_LOOKBACK_DAYS,
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') throw new TypeError('canonical connection store is required');
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') throw new TypeError('workspace settings store is required');
  if (typeof transport !== 'function') throw new TypeError('Meta transport is required');
  if (typeof resolveFxRate !== 'function') throw new TypeError('FX resolver is required');
  if (!Number.isInteger(lookbackDays) || lookbackDays < 1 || lookbackDays > 31) throw new RangeError('lookbackDays must be between 1 and 31');

  async function execute(authorityInput) {
    try {
      const authority = requireServerWorkspaceAuthority(authorityInput);
      const connection = await connectionStore.resolveConnected({ authority, provider: 'meta' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      validateToken(connection, now);
      const accounts = selectedAccounts(connection);
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const reportingCurrency = normalizeCurrencyCode(currency.reportingCurrency, 'reportingCurrency');
      const client = createMetaClient({
        accessToken: connection.accessToken,
        graphVersion,
        transport,
      });
      const response = await client.listAccounts();
      if (!response || !Array.isArray(response.data)) throw new Error('META_ACCOUNTS_RESPONSE_INVALID');
      const providerAccounts = new Map(response.data.map(account => [String(account?.id || ''), account]));
      const canonicalRows = [];
      const candidateDates = [];
      const scanStarts = [];
      const scanEnds = [];

      for (const selected of accounts) {
        const account = providerAccounts.get(selected.id);
        if (!account) throw new Error('META_PROVIDER_ACCOUNT_MISMATCH');
        const sourceCurrency = normalizeCurrencyCode(account.currency, 'provider_account.currency');
        if (sourceCurrency !== selected.currency) throw new Error('META_PROVIDER_CURRENCY_MISMATCH');
        const until = previousClosedBusinessDate(now(), account.timezone_name);
        const since = subtractDays(until, lookbackDays - 1);
        scanStarts.push(since);
        scanEnds.push(until);
        const inventory = await client.fetchAdInsights({ accountId: selected.id, since, until });
        const dates = inventory.data.map(row => String(row?.date_start || ''))
          .filter(date => /^\d{4}-\d{2}-\d{2}$/.test(date) && date >= since && date <= until)
          .sort();
        const providerDate = dates.at(-1);
        if (!providerDate) continue;
        candidateDates.push(providerDate);
        const fx = await resolveFxRate(sourceCurrency, reportingCurrency, { rateDate: providerDate });
        const adapter = createMetaAdapter({ client });
        const mapped = await adapter.fetchCanonicalRows({
          accountId: selected.id,
          since: providerDate,
          until: providerDate,
          context: {
            workspaceId: authority.workspace_id,
            account,
            targetCurrency: reportingCurrency,
            fxRate: fx.fx_rate,
            fxRateDate: fx.fx_rate_date || providerDate,
            fxProvider: fx.fx_provider,
          },
        });
        canonicalRows.push(...mapped.map(result => result.row));
      }

      const campaignKeys = new Set(canonicalRows.map(row => row.entity.root_entity_id));
      const adsetKeys = new Set(canonicalRows.map(row => row.entity.parent_entity_id));
      const adKeys = new Set(canonicalRows.map(row => row.entity.entity_id));
      return Object.freeze({
        status: 'PASS_R6_D5_M1_META_HISTORICAL_READ_ONLY',
        provider_result_status: canonicalRows.length === 0 ? 'empty' : 'non_empty',
        selected_account_count: accounts.length,
        lookback_days: lookbackDays,
        scan_start: scanStarts.length ? scanStarts.sort()[0] : null,
        scan_end: scanEnds.length ? scanEnds.sort().at(-1) : null,
        candidate_date_count: new Set(candidateDates).size,
        provider_date_start: candidateDates.length ? candidateDates.sort()[0] : null,
        provider_date_end: candidateDates.length ? candidateDates.sort().at(-1) : null,
        row_count: canonicalRows.length,
        campaign_count: campaignKeys.size,
        adset_count: adsetKeys.size,
        ad_count: adKeys.size,
        impression_total: metricTotal(canonicalRows, 'impression'),
        ad_click_total: metricTotal(canonicalRows, 'ad_click'),
        spend_total: metricTotal(canonicalRows, 'spend_value'),
        reporting_currency: reportingCurrency,
        conversion_support: Object.freeze({
          add_to_cart: supportSummary(canonicalRows, 'add_to_cart'),
          checkout: supportSummary(canonicalRows, 'checkout'),
          purchase: supportSummary(canonicalRows, 'purchase'),
        }),
        account_api_verified: true,
        insights_verified: true,
        time_fx_verified: canonicalRows.length > 0,
        dataset_v2_write: false,
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      throw safeFailure(error);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({
  DEFAULT_LOOKBACK_DAYS,
  createMetaHistoricalReadOnlyInventory,
  metricTotal,
  safeFailure,
  subtractDays,
  supportSummary,
});

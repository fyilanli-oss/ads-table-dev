'use strict';

const { businessDateFromTimestamp } = require('../../../funnel-core/time-service');
const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');
const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { createMetaAdapter } = require('./adapter');
const { createMetaClient } = require('./client');

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function selectedAccounts(connection) {
  const source = Array.isArray(connection?.selectedAccounts) ? connection.selectedAccounts : [];
  if (source.length < 1 || source.length > 3) throw new Error('META_SELECTED_ACCOUNTS_REQUIRED');
  const seen = new Set();
  return Object.freeze(source.map(account => {
    const id = required(account?.id, 'selected_account.id');
    if (seen.has(id)) throw new Error('META_SELECTED_ACCOUNTS_INVALID');
    seen.add(id);
    return Object.freeze({ id, currency: normalizeCurrencyCode(account.currency, 'selected_account.currency') });
  }));
}

function previousClosedBusinessDate(now, timeZone) {
  const current = businessDateFromTimestamp(now, required(timeZone, 'provider_account.timezone_name'));
  const date = new Date(`${current}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() - 1);
  return date.toISOString().slice(0, 10);
}

function createMetaWorkspaceRunner({ transport = globalThis.fetch, graphVersion = 'v23.0', resolveFxRate, now = () => new Date() } = {}) {
  if (typeof transport !== 'function') throw new TypeError('Meta transport is required');
  if (typeof resolveFxRate !== 'function') throw new TypeError('FX resolver is required');

  return async function runMetaWorkspace(context = {}) {
    const authority = requireServerWorkspaceAuthority(context.authority);
    const connection = context.connection;
    if (!connection || connection.provider !== 'meta' || connection.status !== 'connected') {
      throw new Error('META_CANONICAL_CONNECTION_REQUIRED');
    }
    const accounts = selectedAccounts(connection);
    const reportingCurrency = normalizeCurrencyCode(context.reportingCurrency, 'reportingCurrency');
    const accessToken = required(connection.accessToken, 'connection.accessToken');
    const client = createMetaClient({ accessToken, graphVersion, transport });
    const response = await client.listAccounts();
    if (!response || !Array.isArray(response.data)) throw new Error('META_ACCOUNTS_RESPONSE_INVALID');
    const providerAccounts = new Map(response.data.map(account => [String(account?.id || ''), account]));
    const rows = [];

    for (const selected of accounts) {
      const account = providerAccounts.get(selected.id);
      if (!account) throw new Error('META_PROVIDER_ACCOUNT_MISMATCH');
      const sourceCurrency = normalizeCurrencyCode(account.currency, 'provider_account.currency');
      if (sourceCurrency !== selected.currency) throw new Error('META_PROVIDER_CURRENCY_MISMATCH');
      const providerDate = previousClosedBusinessDate(now(), account.timezone_name);
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
      rows.push(...mapped.map(result => result.row));
    }

    return Object.freeze({
      rows: Object.freeze(rows),
      checked_account_ids: Object.freeze(accounts.map(account => account.id)),
      provider_result_status: rows.length === 0 ? 'empty' : 'non_empty',
    });
  };
}

module.exports = Object.freeze({ createMetaWorkspaceRunner, previousClosedBusinessDate, selectedAccounts });

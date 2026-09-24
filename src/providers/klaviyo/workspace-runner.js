'use strict';

const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');
const { normalizeKlaviyoTimeFxMessage } = require('./runtime');

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function positiveCost(value) {
  const cost = Number(value);
  if (!Number.isFinite(cost) || cost < 0) throw new Error('KLAVIYO_MONTHLY_PLAN_COST_REQUIRED');
  return cost;
}

function onlySelectedAccount(connection) {
  const accounts = Array.isArray(connection?.selectedAccounts) ? connection.selectedAccounts : [];
  if (accounts.length !== 1) throw new Error('KLAVIYO_SINGLE_SELECTED_ACCOUNT_REQUIRED');
  return Object.freeze({
    id: required(accounts[0]?.id, 'selected_account.id'),
    currency: normalizeCurrencyCode(accounts[0]?.currency, 'selected_account.currency'),
  });
}

function createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate } = {}) {
  if (!providerClient || typeof providerClient.fetchAccount !== 'function' ||
    typeof providerClient.fetchMessageFacts !== 'function') {
    throw new TypeError('Klaviyo provider client is required');
  }
  if (typeof resolveFxRate !== 'function') throw new TypeError('FX resolver is required');

  return async function runKlaviyoWorkspace(context = {}) {
    const workspaceId = required(context?.authority?.workspace_id, 'authority.workspace_id');
    const connection = context.connection;
    if (!connection || connection.provider !== 'klaviyo' || connection.status !== 'connected') {
      throw new Error('KLAVIYO_CANONICAL_CONNECTION_REQUIRED');
    }
    const selected = onlySelectedAccount(connection);
    const providerDate = required(context?.request?.provider_date, 'request.provider_date');
    const reportingCurrency = normalizeCurrencyCode(context.reportingCurrency, 'reportingCurrency');
    const account = await providerClient.fetchAccount({
      accessToken: required(connection.accessToken, 'connection.accessToken'),
      accountId: selected.id,
    });
    if (!account || required(account.id, 'provider_account.id') !== selected.id) {
      throw new Error('KLAVIYO_PROVIDER_ACCOUNT_MISMATCH');
    }
    const sourceCurrency = normalizeCurrencyCode(account.currency, 'provider_account.currency');
    if (sourceCurrency !== selected.currency || sourceCurrency !== normalizeCurrencyCode(connection.sourceCurrency, 'connection.sourceCurrency')) {
      throw new Error('KLAVIYO_PROVIDER_CURRENCY_MISMATCH');
    }
    const timezone = required(account.timezone, 'provider_account.timezone');
    const monthlyPlanCost = positiveCost(connection.monthlyPlanCost);
    const providerResult = await providerClient.fetchMessageFacts({
      accessToken: connection.accessToken,
      account: Object.freeze({ id: selected.id, currency: sourceCurrency, timezone }),
      providerDate,
    });
    if (!providerResult || !Array.isArray(providerResult.rows)) throw new Error('KLAVIYO_PROVIDER_RESULT_INVALID');
    if (providerResult.rows.length === 0 && providerResult.verified_empty !== true) {
      throw new Error('KLAVIYO_EMPTY_RESULT_NOT_VERIFIED');
    }
    const fx = await resolveFxRate(sourceCurrency, reportingCurrency, { rateDate: providerDate });
    const keys = new Set();
    const rows = providerResult.rows.map(input => {
      const spendAllocation = input.channel === 'email'
        ? { ...(input.spend_allocation || {}), monthlyPlanCost }
        : { ...(input.spend_allocation || {}) };
      const normalized = normalizeKlaviyoTimeFxMessage({ ...input, spend_allocation: spendAllocation }, {
        workspaceId,
        accountId: selected.id,
        account: { id: selected.id, currency: sourceCurrency, timezone },
        providerDate,
        targetCurrency: reportingCurrency,
        fxRate: fx.fx_rate,
        fxRateDate: fx.fx_rate_date || providerDate,
        fxProvider: fx.fx_provider,
      });
      const key = `${normalized.row.identity.date}:${normalized.entityKey}`;
      if (keys.has(key)) throw new Error('KLAVIYO_DUPLICATE_PROVIDER_FACT');
      keys.add(key);
      return normalized.row;
    });
    return Object.freeze({
      rows,
      checked_account_ids: [selected.id],
      provider_result_status: rows.length === 0 ? 'empty' : 'non_empty',
    });
  };
}

module.exports = Object.freeze({ createKlaviyoWorkspaceRunner });


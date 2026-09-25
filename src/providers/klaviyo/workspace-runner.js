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

function stageFailure(stage, error, preserveInternalMessage = false) {
  const failure = new Error(preserveInternalMessage ? String(error?.message || 'KLAVIYO_RUNTIME_STAGE_FAILED') : 'KLAVIYO_RUNTIME_STAGE_FAILED');
  const code = String(error?.code || '');
  if (/^[A-Z0-9_]{1,96}$/.test(code)) failure.code = code;
  else if (!preserveInternalMessage) failure.code = 'KLAVIYO_RUNTIME_STAGE_FAILED';
  failure.diagnosticStage = stage;
  return failure;
}

function syncStage(stage, action) {
  try { return action(); }
  catch (error) { throw stageFailure(stage, error, true); }
}

async function asyncStage(stage, action) {
  try { return await action(); }
  catch (error) { throw stageFailure(stage, error, false); }
}

function createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate } = {}) {
  if (!providerClient || typeof providerClient.fetchAccount !== 'function' ||
    typeof providerClient.fetchMessageFacts !== 'function') {
    throw new TypeError('Klaviyo provider client is required');
  }
  if (typeof resolveFxRate !== 'function') throw new TypeError('FX resolver is required');

  return async function runKlaviyoWorkspace(context = {}) {
    const runtime = syncStage('RUNTIME_CONTEXT', () => {
      const workspaceId = required(context?.authority?.workspace_id, 'authority.workspace_id');
      const connection = context.connection;
      if (!connection || connection.provider !== 'klaviyo' || connection.status !== 'connected') {
        throw new Error('KLAVIYO_CANONICAL_CONNECTION_REQUIRED');
      }
      return Object.freeze({
        workspaceId,
        connection,
        selected: onlySelectedAccount(connection),
        conversionMetricId: required(connection?.conversionMetric?.id, 'connection.conversionMetric.id'),
        providerDate: required(context?.request?.provider_date, 'request.provider_date'),
        reportingCurrency: normalizeCurrencyCode(context.reportingCurrency, 'reportingCurrency'),
        accessToken: required(connection.accessToken, 'connection.accessToken'),
        monthlyPlanCost: positiveCost(connection.monthlyPlanCost),
      });
    });
    const account = await asyncStage('PROVIDER_ACCOUNT', () => providerClient.fetchAccount({
      accessToken: runtime.accessToken,
      accountId: runtime.selected.id,
    }));
    const verifiedAccount = syncStage('PROVIDER_ACCOUNT_VALIDATION', () => {
      if (!account || required(account.id, 'provider_account.id') !== runtime.selected.id) {
        throw new Error('KLAVIYO_PROVIDER_ACCOUNT_MISMATCH');
      }
      const sourceCurrency = normalizeCurrencyCode(account.currency, 'provider_account.currency');
      if (sourceCurrency !== runtime.selected.currency || sourceCurrency !== normalizeCurrencyCode(runtime.connection.sourceCurrency, 'connection.sourceCurrency')) {
        throw new Error('KLAVIYO_PROVIDER_CURRENCY_MISMATCH');
      }
      return Object.freeze({ sourceCurrency, timezone: required(account.timezone, 'provider_account.timezone') });
    });
    const providerResult = await asyncStage('PROVIDER_FACTS', () => providerClient.fetchMessageFacts({
      accessToken: runtime.accessToken,
      account: Object.freeze({ id: runtime.selected.id, currency: verifiedAccount.sourceCurrency, timezone: verifiedAccount.timezone }),
      providerDate: runtime.providerDate,
      conversionMetricId: runtime.conversionMetricId,
    }));
    syncStage('PROVIDER_RESULT_VALIDATION', () => {
      if (!providerResult || !Array.isArray(providerResult.rows)) throw new Error('KLAVIYO_PROVIDER_RESULT_INVALID');
      if (providerResult.rows.length === 0 && providerResult.verified_empty !== true) throw new Error('KLAVIYO_EMPTY_RESULT_NOT_VERIFIED');
    });
    const fx = await asyncStage('FX_RESOLUTION', () => resolveFxRate(verifiedAccount.sourceCurrency, runtime.reportingCurrency, { rateDate: runtime.providerDate }));
    const rows = syncStage('ROW_NORMALIZATION', () => {
      const keys = new Set();
      return providerResult.rows.map(input => {
        const spendAllocation = input.channel === 'email'
          ? { ...(input.spend_allocation || {}), monthlyPlanCost: runtime.monthlyPlanCost }
          : { ...(input.spend_allocation || {}) };
        const normalized = normalizeKlaviyoTimeFxMessage({ ...input, spend_allocation: spendAllocation }, {
          workspaceId: runtime.workspaceId,
          accountId: runtime.selected.id,
          account: { id: runtime.selected.id, currency: verifiedAccount.sourceCurrency, timezone: verifiedAccount.timezone },
          providerDate: runtime.providerDate,
          targetCurrency: runtime.reportingCurrency,
          fxRate: fx.fx_rate,
          fxRateDate: fx.fx_rate_date || runtime.providerDate,
          fxProvider: fx.fx_provider,
        });
        const key = `${normalized.row.identity.date}:${normalized.entityKey}`;
        if (keys.has(key)) throw new Error('KLAVIYO_DUPLICATE_PROVIDER_FACT');
        keys.add(key);
        return normalized.row;
      });
    });
    return Object.freeze({ rows, checked_account_ids: [runtime.selected.id], provider_result_status: rows.length === 0 ? 'empty' : 'non_empty' });
  };
}

module.exports = Object.freeze({ createKlaviyoWorkspaceRunner });
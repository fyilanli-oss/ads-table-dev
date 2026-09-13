'use strict';

const { validateCanonicalRow } = require('../../../funnel-core/canonical-contract');
const { buildEntityKey } = require('../../../funnel-core/entity-hierarchy');
const { normalizeCurrencyCode, normalizeMonetaryRawFields } = require('../../../funnel-core/fx-service');
const { normalizeBusinessDate } = require('../../../funnel-core/time-service');

function requiredText(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function normalizeMetaAccount(account, requestedAccountId) {
  if (!account || typeof account !== 'object' || Array.isArray(account)) throw new TypeError('context.account is required');
  const id = requiredText(account.id, 'context.account.id');
  if (id !== requiredText(requestedAccountId, 'accountId')) throw new Error('Meta account identity mismatch');
  return Object.freeze({
    id,
    sourceTimezone: requiredText(account.timezone_name, 'context.account.timezone_name'),
    sourceCurrency: normalizeCurrencyCode(account.currency, 'context.account.currency')
  });
}

function normalizeMetaMappedResult(mapped, insight, input) {
  if (!mapped || !mapped.row) throw new TypeError('mapped Meta result is required');
  const context = input?.context;
  if (!context || typeof context !== 'object' || Array.isArray(context)) throw new TypeError('context is required');
  const account = normalizeMetaAccount(context.account, input.accountId);
  const insightCurrency = normalizeCurrencyCode(insight?.account_currency, 'insight.account_currency');
  if (insightCurrency !== account.sourceCurrency) throw new Error('Meta account currency mismatch');
  const time = normalizeBusinessDate({ providerDate: insight.date_start, sourceTimezone: account.sourceTimezone });
  if (insight.date_stop !== insight.date_start) throw new Error('Meta insight must represent one business day');
  const targetCurrency = normalizeCurrencyCode(context.targetCurrency || account.sourceCurrency, 'context.targetCurrency');
  const crossCurrency = targetCurrency !== account.sourceCurrency;
  const fxProvider = crossCurrency ? requiredText(context.fxProvider, 'context.fxProvider') : (context.fxProvider || 'same_currency');
  const row = normalizeMonetaryRawFields(mapped.row, {
    sourceCurrency: account.sourceCurrency,
    targetCurrency,
    fxRate: context.fxRate ?? null,
    fxRateDate: context.fxRateDate || time.business_date,
    fxProvider
  });
  row.identity.date = time.business_date;
  row.time = time;
  validateCanonicalRow(row);
  return Object.freeze({ row: Object.freeze(row), entityKey: buildEntityKey(row.identity, row.entity) });
}

module.exports = Object.freeze({ normalizeMetaAccount, normalizeMetaMappedResult });

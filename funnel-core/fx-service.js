'use strict';

const { MONETARY_METRICS, cloneCanonicalRow } = require('./canonical-contract');

const FX_ENGINE_VERSION = 'v1';

function normalizeCurrencyCode(value, field) {
  if (typeof value !== 'string' || !/^[A-Za-z]{3}$/.test(value)) {
    throw new Error(`${field} must be a 3-letter currency code`);
  }
  return value.toUpperCase();
}

function resolveRate(sourceCurrency, targetCurrency, fxRate) {
  if (sourceCurrency === targetCurrency) {
    if (fxRate === null || fxRate === undefined) return 1;
    if (typeof fxRate !== 'number' || !Number.isFinite(fxRate) || fxRate <= 0) {
      throw new Error('fx_rate must be a positive number');
    }
    if (fxRate !== 1) throw new Error('Same-currency normalization must use fx_rate=1');
    return 1;
  }

  if (typeof fxRate !== 'number' || !Number.isFinite(fxRate) || fxRate <= 0) {
    throw new Error('Cross-currency normalization requires a positive fx_rate; rate=1 is never a silent fallback');
  }
  return fxRate;
}

function normalizeMonetaryRawFields(row, {
  sourceCurrency = row?.currency?.source_currency,
  targetCurrency = row?.currency?.target_currency,
  fxRate = row?.currency?.fx_rate,
  fxRateDate = row?.currency?.fx_rate_date ?? row?.identity?.date,
  fxProvider = row?.currency?.fx_provider ?? 'fixture'
} = {}) {
  const source = normalizeCurrencyCode(sourceCurrency, 'source_currency');
  const target = normalizeCurrencyCode(targetCurrency, 'target_currency');
  const rate = resolveRate(source, target, fxRate);

  const next = cloneCanonicalRow(row);
  for (const metric of MONETARY_METRICS) {
    const support = next.metric_support?.[metric];
    const value = next.raw_metrics?.[metric];

    if (support === 'supported') {
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new Error(`Supported monetary metric ${metric} must be a finite number`);
      }
      next.raw_metrics[metric] = value * rate;
    } else {
      if (value !== null) throw new Error(`${support} monetary metric ${metric} must remain null during FX normalization`);
      next.raw_metrics[metric] = null;
    }
  }

  next.currency = {
    source_currency: source,
    target_currency: target,
    fx_rate: rate,
    fx_rate_date: fxRateDate,
    fx_provider: fxProvider,
    fx_engine_version: FX_ENGINE_VERSION
  };

  return next;
}

module.exports = Object.freeze({
  FX_ENGINE_VERSION,
  normalizeCurrencyCode,
  resolveRate,
  normalizeMonetaryRawFields
});

'use strict';

const { RAW_METRICS } = require('./canonical-contract');

const ANALYSIS_SCOPES = Object.freeze({
  PAID: 'paid',
  ORGANIC: 'organic',
  BLEND: 'blend'
});

const SUPPORT_PRIORITY = Object.freeze({ supported: 0, unknown: 1, unsupported: 2 });

function normalizeScope(scope) {
  const value = String(scope || '').toLowerCase();
  if (!Object.values(ANALYSIS_SCOPES).includes(value)) throw new Error(`Unsupported analysis_scope: ${scope}`);
  return value;
}

function rowsForScope(rows, scope) {
  const normalized = normalizeScope(scope);
  if (normalized === ANALYSIS_SCOPES.PAID) return rows.filter((row) => row.identity.traffic_type === 'paid');
  if (normalized === ANALYSIS_SCOPES.ORGANIC) return rows.filter((row) => row.identity.traffic_type === 'organic');
  return rows.filter((row) => row.identity.traffic_type === 'paid' || row.identity.traffic_type === 'organic');
}

function mergeSupportEntries(entries) {
  if (!entries.length) return { value: null, support: 'unknown' };

  const statuses = new Set();
  for (const entry of entries) {
    if (!(entry.support in SUPPORT_PRIORITY)) throw new Error(`Invalid metric support: ${entry.support}`);
    statuses.add(entry.support);
  }

  if (statuses.size === 1 && statuses.has('supported')) {
    return {
      value: entries.reduce((sum, entry) => sum + entry.value, 0),
      support: 'supported'
    };
  }

  // All rows explicitly unsupported means the aggregate metric is unsupported.
  if (statuses.size === 1 && statuses.has('unsupported')) {
    return { value: null, support: 'unsupported' };
  }

  // Any mixed support state is incomplete/partial. The canonical metric_support
  // enum has no separate `partial` value, so aggregate completeness is `unknown`.
  // Most importantly, the partial values are never summed into a fake complete total.
  return { value: null, support: 'unknown' };
}

function metricEntries(rows, metric) {
  return rows.map((row) => ({
    value: row.raw_metrics[metric],
    support: row.metric_support[metric]
  }));
}

function clickEntries(rows) {
  return rows.map((row) => {
    const metric = row.identity.traffic_type === 'organic' ? 'session' : 'ad_click';
    return {
      value: row.raw_metrics[metric],
      support: row.metric_support[metric]
    };
  });
}

function aggregateScope(rows, scope) {
  const selectedRows = rowsForScope(rows, scope);
  const support = {};
  const aggregate = {};

  const metricMap = {
    funnel_impression: 'impression',
    funnel_spend: 'spend_value',
    funnel_add_to_cart: 'add_to_cart',
    funnel_add_to_cart_value: 'add_to_cart_value',
    funnel_checkout: 'checkout',
    funnel_checkout_value: 'checkout_value',
    funnel_purchase: 'purchase',
    funnel_sales: 'purchase_value'
  };

  const click = mergeSupportEntries(clickEntries(selectedRows));
  aggregate.funnel_click = click.value;
  support.funnel_click = click.support;

  for (const [outputMetric, rawMetric] of Object.entries(metricMap)) {
    const merged = mergeSupportEntries(metricEntries(selectedRows, rawMetric));
    aggregate[outputMetric] = merged.value;
    support[outputMetric] = merged.support;
  }

  return {
    analysis_scope: normalizeScope(scope),
    row_count: selectedRows.length,
    ...aggregate,
    metric_support: support
  };
}

function aggregateIntentPaid(rows) {
  const paidRows = rows.filter((row) => row.identity.traffic_type === 'paid');
  const sourceMap = {
    paid_ad_click: 'ad_click',
    paid_add_to_cart: 'add_to_cart',
    paid_checkout: 'checkout',
    paid_purchase: 'purchase'
  };
  const result = { analysis_scope: ANALYSIS_SCOPES.PAID, metric_support: {} };

  for (const [outputMetric, rawMetric] of Object.entries(sourceMap)) {
    const merged = mergeSupportEntries(metricEntries(paidRows, rawMetric));
    result[outputMetric] = merged.value;
    result.metric_support[outputMetric] = merged.support;
  }
  return result;
}

function aggregateRawMetrics(rows) {
  const result = {};
  const support = {};
  for (const metric of RAW_METRICS) {
    const merged = mergeSupportEntries(metricEntries(rows, metric));
    result[metric] = merged.value;
    support[metric] = merged.support;
  }
  return { raw_metrics: result, metric_support: support };
}

module.exports = Object.freeze({
  ANALYSIS_SCOPES,
  normalizeScope,
  rowsForScope,
  mergeSupportEntries,
  aggregateScope,
  aggregateIntentPaid,
  aggregateRawMetrics
});

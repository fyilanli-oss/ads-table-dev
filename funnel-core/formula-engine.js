'use strict';

const FORMULA_ENGINE_VERSION = 'v1';

function isMeasured(value, support) {
  return support === 'supported' && typeof value === 'number' && Number.isFinite(value);
}

function binaryMetric(left, leftSupport, right, rightSupport, operation) {
  if (!isMeasured(left, leftSupport) || !isMeasured(right, rightSupport)) return null;
  return operation(left, right);
}

function ratioMetric(numerator, numeratorSupport, denominator, denominatorSupport, multiplier = 1) {
  if (!isMeasured(numerator, numeratorSupport) || !isMeasured(denominator, denominatorSupport)) return null;
  if (denominator === 0) return null;
  return (numerator / denominator) * multiplier;
}

function calculateFunnelMetrics(aggregate) {
  const s = aggregate.metric_support || {};
  const sales = isMeasured(aggregate.funnel_sales, s.funnel_sales) ? aggregate.funnel_sales : null;
  const abandoned = binaryMetric(
    aggregate.funnel_checkout, s.funnel_checkout,
    aggregate.funnel_purchase, s.funnel_purchase,
    (checkout, purchase) => Math.max(checkout - purchase, 0)
  );
  const abandonedValue = binaryMetric(
    aggregate.funnel_checkout_value, s.funnel_checkout_value,
    aggregate.funnel_sales, s.funnel_sales,
    (checkoutValue, purchaseValue) => Math.max(checkoutValue - purchaseValue, 0)
  );
  const profit = binaryMetric(
    aggregate.funnel_sales, s.funnel_sales,
    aggregate.funnel_spend, s.funnel_spend,
    (salesValue, spend) => salesValue - spend
  );

  return {
    sales,
    abandoned,
    abandoned_value: abandonedValue,
    profit,
    margin: profit === null ? null : ratioMetric(profit, 'supported', aggregate.funnel_sales, s.funnel_sales, 100),
    ctr: ratioMetric(aggregate.funnel_click, s.funnel_click, aggregate.funnel_impression, s.funnel_impression, 100),
    cpc: ratioMetric(aggregate.funnel_spend, s.funnel_spend, aggregate.funnel_click, s.funnel_click),
    roas: ratioMetric(aggregate.funnel_sales, s.funnel_sales, aggregate.funnel_spend, s.funnel_spend),
    cps: ratioMetric(aggregate.funnel_spend, s.funnel_spend, aggregate.funnel_purchase, s.funnel_purchase),
    formula_engine_version: FORMULA_ENGINE_VERSION
  };
}

function calculateIntentMetrics(paidAggregate) {
  const s = paidAggregate.metric_support || {};
  const abandoned = binaryMetric(
    paidAggregate.paid_checkout, s.paid_checkout,
    paidAggregate.paid_purchase, s.paid_purchase,
    (checkout, purchase) => Math.max(checkout - purchase, 0)
  );
  const abandonedSupport = abandoned === null ? 'unknown' : 'supported';

  return {
    add_to_cart_rate: ratioMetric(paidAggregate.paid_add_to_cart, s.paid_add_to_cart, paidAggregate.paid_ad_click, s.paid_ad_click, 100),
    checkout_rate: ratioMetric(paidAggregate.paid_checkout, s.paid_checkout, paidAggregate.paid_add_to_cart, s.paid_add_to_cart, 100),
    abandoned_rate: ratioMetric(abandoned, abandonedSupport, paidAggregate.paid_checkout, s.paid_checkout, 100),
    purchase_rate: ratioMetric(paidAggregate.paid_purchase, s.paid_purchase, paidAggregate.paid_checkout, s.paid_checkout, 100),
    formula_engine_version: FORMULA_ENGINE_VERSION
  };
}

module.exports = Object.freeze({
  FORMULA_ENGINE_VERSION,
  calculateFunnelMetrics,
  calculateIntentMetrics
});

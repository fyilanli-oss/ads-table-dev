'use strict';

const REPORT_CONTRACT_VERSION = 'tiktok-report-v1';
const REPORT_ENDPOINT = '/v1.3/report/integrated/get/';
const OFFICIAL_REFERENCE = 'https://business-api.tiktok.com/portal/docs?id=1738864834038786';

const LEVELS = Object.freeze({
  campaign: Object.freeze({ data_level: 'AUCTION_CAMPAIGN', dimensions: Object.freeze(['campaign_id']) }),
  adgroup: Object.freeze({ data_level: 'AUCTION_ADGROUP', dimensions: Object.freeze(['adgroup_id']) }),
  ad: Object.freeze({ data_level: 'AUCTION_AD', dimensions: Object.freeze(['ad_id']) })
});

const METRICS = Object.freeze({
  spend: Object.freeze({ semantic: 'spend_value', support: 'supported' }),
  impressions: Object.freeze({ semantic: 'impression', support: 'supported' }),
  clicks: Object.freeze({ semantic: 'ad_click', support: 'supported' }),
  add_to_cart: Object.freeze({ semantic: 'add_to_cart', support: 'tracking_dependent' }),
  initiate_checkout: Object.freeze({ semantic: 'checkout', support: 'tracking_dependent' }),
  complete_payment: Object.freeze({ semantic: 'purchase', support: 'tracking_dependent' }),
  total_complete_payment_value: Object.freeze({ semantic: 'purchase_value', support: 'tracking_dependent' })
});

const FORBIDDEN_PURCHASE_FALLBACKS = Object.freeze(['conversion', 'conversions', 'conversion_value']);

function reportContract() {
  return Object.freeze({
    version: REPORT_CONTRACT_VERSION,
    endpoint: REPORT_ENDPOINT,
    report_type: 'BASIC',
    official_reference: OFFICIAL_REFERENCE,
    levels: LEVELS,
    metrics: METRICS,
    hard_rules: Object.freeze({
      production_leaf_level: 'ad',
      hierarchy_levels_are_not_additive: true,
      generic_conversion_is_not_purchase: true,
      missing_is_not_zero: true,
      synthetic_rows_are_not_production_performance: true
    })
  });
}

function buildReportSelection(level = 'ad') {
  const selected = LEVELS[String(level || '').toLowerCase()];
  if (!selected) throw new Error('Unsupported TikTok report level');
  return Object.freeze({
    report_type: 'BASIC',
    data_level: selected.data_level,
    dimensions: selected.dimensions,
    metrics: Object.freeze(Object.keys(METRICS))
  });
}

function assertNoGenericPurchaseFallback(row) {
  if (!row || typeof row !== 'object') throw new TypeError('TikTok report row must be an object');
  const sources = [row, row.metrics].filter(value => value && typeof value === 'object');
  for (const field of FORBIDDEN_PURCHASE_FALLBACKS) {
    if (sources.some(source => Object.prototype.hasOwnProperty.call(source, field))) {
      throw new Error(`TikTok ${field} cannot be treated as purchase evidence`);
    }
  }
  return true;
}

module.exports = Object.freeze({
  FORBIDDEN_PURCHASE_FALLBACKS,
  LEVELS,
  METRICS,
  OFFICIAL_REFERENCE,
  REPORT_CONTRACT_VERSION,
  REPORT_ENDPOINT,
  assertNoGenericPurchaseFallback,
  buildReportSelection,
  reportContract
});

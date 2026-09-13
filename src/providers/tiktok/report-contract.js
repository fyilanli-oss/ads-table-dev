'use strict';

const OFFICIAL_SDK_COMMIT = 'f809c396520df2d7b201a9ccc5378d822b728ed3';

const TIKTOK_REPORT_CONTRACT = Object.freeze({
  version: 'e6-t1-v1',
  provider_api_version: 'v1.3',
  official_source: Object.freeze({
    repository: 'https://github.com/tiktok/tiktok-business-api-sdk',
    commit: OFFICIAL_SDK_COMMIT,
    sdk_document: 'js_sdk/docs/ReportingApi.md',
    portal_document_id: '1740302848100353'
  }),
  request: Object.freeze({
    method: 'GET',
    endpoint: '/open_api/v1.3/report/integrated/get/',
    report_type: 'BASIC',
    service_type: 'AUCTION',
    data_level: 'AUCTION_AD',
    dimensions: Object.freeze(['ad_id']),
    delivery_metrics: Object.freeze(['spend', 'impressions', 'clicks'])
  }),
  production_grain: Object.freeze({
    root: 'campaign',
    parent: 'adgroup',
    leaf: 'ad',
    additive_level: 'AUCTION_AD'
  }),
  canonical_support: Object.freeze({
    impression: 'supported',
    ad_click: 'supported',
    spend_value: 'supported',
    add_to_cart: 'unknown',
    add_to_cart_value: 'unknown',
    checkout: 'unknown',
    checkout_value: 'unknown',
    purchase: 'unknown',
    purchase_value: 'unknown'
  }),
  hard_rules: Object.freeze({
    production_rows_are_ad_leaf_only: true,
    campaign_and_adgroup_are_lineage_not_additive_rows: true,
    generic_conversion_is_not_purchase: true,
    missing_metric_is_not_zero: true,
    synthetic_rows_forbidden: true,
    event_metrics_require_separate_live_characterization: true
  })
});

function validateTikTokReportContract(contract = TIKTOK_REPORT_CONTRACT) {
  if (contract.request.endpoint !== '/open_api/v1.3/report/integrated/get/') throw new Error('TikTok report endpoint is not frozen');
  if (contract.request.report_type !== 'BASIC' || contract.request.data_level !== 'AUCTION_AD') throw new Error('TikTok production report grain is not frozen');
  if (contract.request.delivery_metrics.includes('conversion')) throw new Error('Generic TikTok conversion metric is forbidden');
  if (!contract.hard_rules.generic_conversion_is_not_purchase || !contract.hard_rules.missing_metric_is_not_zero) throw new Error('TikTok fail-closed metric rules are required');
  return true;
}

module.exports = Object.freeze({ OFFICIAL_SDK_COMMIT, TIKTOK_REPORT_CONTRACT, validateTikTokReportContract });

'use strict';

const { RAW_METRICS } = require('../canonical-contract');

function allSupported(metrics = {}) {
  const raw = {};
  const support = {};
  for (const metric of RAW_METRICS) {
    if (Object.prototype.hasOwnProperty.call(metrics, metric)) {
      raw[metric] = metrics[metric];
      support[metric] = metrics[metric] === null ? 'unknown' : 'supported';
    } else {
      raw[metric] = 0;
      support[metric] = 'supported';
    }
  }
  return { raw, support };
}

function makeRow({
  userId = 'user-1',
  platform,
  trafficType = 'paid',
  sourceSystem,
  channel = null,
  accountId,
  date = '2026-08-15',
  entity,
  metrics = {},
  metricSupport = {},
  sourceCurrency = 'TRY',
  targetCurrency = 'TRY',
  fxRate = 1,
  fxRateDate = date,
  fxProvider = 'fixture',
  timezone = 'Europe/Istanbul',
  adapterVersion = 'fixture-v1',
  sourceConfidence = 'real',
  synthetic = false,
  ga4PropertyId = null,
  rawReference = { fixture: true }
}) {
  const seeded = allSupported(metrics);
  const rawMetrics = seeded.raw;
  const support = seeded.support;

  for (const [metric, status] of Object.entries(metricSupport)) {
    support[metric] = status;
    if (status !== 'supported') rawMetrics[metric] = null;
  }

  return {
    identity: {
      user_id: userId,
      platform,
      traffic_type: trafficType,
      source_system: sourceSystem,
      channel,
      platform_account_id: accountId,
      date
    },
    entity,
    raw_metrics: rawMetrics,
    metric_support: support,
    currency: {
      source_currency: sourceCurrency,
      target_currency: targetCurrency,
      fx_rate: fxRate,
      fx_rate_date: fxRateDate,
      fx_provider: fxProvider,
      fx_engine_version: 'v1'
    },
    time: {
      source_timezone: timezone,
      business_date: date,
      time_engine_version: 'v1'
    },
    provenance: {
      source_system: sourceSystem,
      adapter_version: adapterVersion,
      source_confidence: sourceConfidence,
      synthetic,
      ga4_property_id: ga4PropertyId,
      raw_reference: rawReference
    }
  };
}

function metaPaid(overrides = {}) {
  return makeRow({
    platform: 'meta',
    sourceSystem: 'meta_ads',
    accountId: 'meta-acct-1',
    entity: {
      campaign_type: null,
      root_entity_type: 'campaign',
      root_entity_id: 'meta-campaign-1',
      root_entity_name: 'Meta Campaign',
      parent_entity_type: 'adset',
      parent_entity_id: 'meta-adset-1',
      parent_entity_name: 'Meta AdSet',
      entity_type: 'ad',
      entity_id: 'meta-ad-1',
      entity_name: 'Meta Ad'
    },
    metrics: {
      impression: 1000,
      ad_click: 100,
      session: 0,
      spend_value: 100,
      add_to_cart: 20,
      add_to_cart_value: 400,
      checkout: 10,
      checkout_value: 300,
      purchase: 5,
      purchase_value: 250
    },
    ...overrides
  });
}

function googleStandard(overrides = {}) {
  return makeRow({
    platform: 'google',
    sourceSystem: 'google_ads',
    accountId: 'google-acct-1',
    entity: {
      campaign_type: 'standard',
      root_entity_type: 'campaign',
      root_entity_id: 'google-campaign-1',
      root_entity_name: 'Google Campaign',
      parent_entity_type: 'adgroup',
      parent_entity_id: 'google-adgroup-1',
      parent_entity_name: 'Google AdGroup',
      entity_type: 'ad',
      entity_id: 'google-ad-1',
      entity_name: 'Google Ad'
    },
    metrics: {
      impression: 800,
      ad_click: 80,
      session: 0,
      spend_value: 120,
      add_to_cart: 16,
      add_to_cart_value: 320,
      checkout: 8,
      checkout_value: 240,
      purchase: 4,
      purchase_value: 220
    },
    ...overrides
  });
}

function googlePmax(overrides = {}) {
  return makeRow({
    platform: 'google',
    sourceSystem: 'google_ads',
    accountId: 'google-acct-1',
    entity: {
      campaign_type: 'performance_max',
      root_entity_type: 'campaign',
      root_entity_id: 'pmax-campaign-1',
      root_entity_name: 'PMax Campaign',
      parent_entity_type: null,
      parent_entity_id: null,
      parent_entity_name: null,
      entity_type: 'asset_group',
      entity_id: 'asset-group-1',
      entity_name: 'Asset Group 1'
    },
    metrics: {
      impression: 600,
      ad_click: 60,
      session: 0,
      spend_value: 90,
      add_to_cart: 12,
      add_to_cart_value: 260,
      checkout: 6,
      checkout_value: 210,
      purchase: 3,
      purchase_value: 180
    },
    ...overrides
  });
}

function tiktokPaid(overrides = {}) {
  return makeRow({
    platform: 'tiktok',
    sourceSystem: 'tiktok_ads',
    accountId: 'tiktok-acct-1',
    entity: {
      campaign_type: null,
      root_entity_type: 'campaign',
      root_entity_id: 'tiktok-campaign-1',
      root_entity_name: 'TikTok Campaign',
      parent_entity_type: 'adgroup',
      parent_entity_id: 'tiktok-adgroup-1',
      parent_entity_name: 'TikTok AdGroup',
      entity_type: 'ad',
      entity_id: 'tiktok-ad-1',
      entity_name: 'TikTok Ad'
    },
    metrics: {
      impression: 500,
      ad_click: 50,
      session: 0,
      spend_value: 70,
      add_to_cart: 10,
      add_to_cart_value: 180,
      checkout: 5,
      checkout_value: 140,
      purchase: 2,
      purchase_value: 120
    },
    ...overrides
  });
}

function klaviyoCampaignEmail(overrides = {}) {
  return makeRow({
    platform: 'klaviyo',
    sourceSystem: 'klaviyo',
    channel: 'email',
    accountId: 'klaviyo-acct-1',
    entity: {
      campaign_type: null,
      root_entity_type: 'campaign',
      root_entity_id: 'klaviyo-campaign-1',
      root_entity_name: 'Email Campaign',
      parent_entity_type: null,
      parent_entity_id: null,
      parent_entity_name: null,
      entity_type: 'campaign_message',
      entity_id: 'message-1',
      entity_name: 'Campaign Message 1'
    },
    metrics: {
      impression: 1000,
      ad_click: 90,
      session: 0,
      spend_value: 30,
      add_to_cart: 15,
      add_to_cart_value: 280,
      checkout: 8,
      checkout_value: 210,
      purchase: 5,
      purchase_value: 190
    },
    ...overrides
  });
}

function klaviyoFlowSms(overrides = {}) {
  return makeRow({
    platform: 'klaviyo',
    sourceSystem: 'klaviyo',
    channel: 'sms',
    accountId: 'klaviyo-acct-1',
    entity: {
      campaign_type: null,
      root_entity_type: 'flow',
      root_entity_id: 'klaviyo-flow-1',
      root_entity_name: 'SMS Flow',
      parent_entity_type: null,
      parent_entity_id: null,
      parent_entity_name: null,
      entity_type: 'flow_message',
      entity_id: 'message-1',
      entity_name: 'Flow Message 1'
    },
    metrics: {
      impression: 300,
      ad_click: 40,
      session: 0,
      spend_value: 20,
      add_to_cart: 8,
      add_to_cart_value: 160,
      checkout: 4,
      checkout_value: 110,
      purchase: 2,
      purchase_value: 90
    },
    ...overrides
  });
}

function metaOrganic(overrides = {}) {
  return makeRow({
    platform: 'meta',
    trafficType: 'organic',
    sourceSystem: 'ga4',
    accountId: 'meta-acct-1',
    ga4PropertyId: 'ga4-property-1',
    entity: {
      campaign_type: null,
      root_entity_type: 'organic',
      root_entity_id: 'meta-acct-1:organic',
      root_entity_name: 'Meta Organic',
      parent_entity_type: null,
      parent_entity_id: null,
      parent_entity_name: null,
      entity_type: 'organic',
      entity_id: 'meta-acct-1:organic',
      entity_name: 'Meta Organic'
    },
    metrics: {
      impression: 200,
      ad_click: 0,
      session: 50,
      spend_value: 0,
      add_to_cart: 10,
      add_to_cart_value: 200,
      checkout: 5,
      checkout_value: 140,
      purchase: 3,
      purchase_value: 120
    },
    ...overrides
  });
}

module.exports = Object.freeze({
  makeRow,
  metaPaid,
  googleStandard,
  googlePmax,
  tiktokPaid,
  klaviyoCampaignEmail,
  klaviyoFlowSms,
  metaOrganic
});

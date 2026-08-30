'use strict';

const META_CAPABILITIES = Object.freeze({
  platform: 'meta',
  sourceSystem: 'meta_ads',
  trafficType: 'paid',
  channel: null,
  hierarchy: Object.freeze({ root: 'campaign', parent: 'adset', leaf: 'ad' }),
  canonicalFacts: Object.freeze([
    'impression', 'ad_click', 'spend_value',
    'add_to_cart', 'add_to_cart_value',
    'checkout', 'checkout_value', 'purchase', 'purchase_value'
  ]),
  unsupportedFacts: Object.freeze(['session']),
  evidenceOnlyFields: Object.freeze(['clicks', 'reach', 'ctr', 'cpc', 'cost_per_action_type', 'conversion_rate_ranking']),
  actionPriority: Object.freeze({
    ad_click: Object.freeze(['link_click']),
    add_to_cart: Object.freeze(['add_to_cart', 'omni_add_to_cart']),
    add_to_cart_value: Object.freeze(['add_to_cart', 'omni_add_to_cart']),
    checkout: Object.freeze(['initiate_checkout', 'checkout', 'omni_initiated_checkout']),
    checkout_value: Object.freeze(['initiate_checkout', 'checkout', 'omni_initiated_checkout']),
    purchase: Object.freeze(['purchase', 'omni_purchase']),
    purchase_value: Object.freeze(['purchase', 'omni_purchase'])
  })
});

module.exports = Object.freeze({ META_CAPABILITIES });

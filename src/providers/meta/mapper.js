'use strict';

const { validateCanonicalRow } = require('../../../funnel-core/canonical-contract');
const { buildEntityKey, validateEntityHierarchy } = require('../../../funnel-core/entity-hierarchy');
const { META_CAPABILITIES } = require('./capabilities');

const ADAPTER_VERSION = 'meta-v1';
function text(value, field) { if (typeof value !== 'string' || value.trim() === '') throw new Error(`${field} is required`); return value.trim(); }
function metric(value, field) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value); if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`${field} must be a non-negative number`); return parsed;
}
function actionValue(list, priority) {
  if (!Array.isArray(list)) return null;
  for (const type of priority) {
    const match = list.find(item => item && item.action_type === type);
    if (match) return metric(match.value, `action.${type}`);
  }
  return null;
}
function supported(value) { return value === null ? 'unknown' : 'supported'; }

function mapMetaInsight(insight, context) {
  if (!insight || typeof insight !== 'object' || Array.isArray(insight)) throw new TypeError('Meta insight is required');
  if (!context || typeof context !== 'object' || Array.isArray(context)) throw new TypeError('Meta mapping context is required');
  const identity = {
    user_id: text(context.userId, 'context.userId'), platform: 'meta', traffic_type: 'paid', source_system: 'meta_ads', channel: null,
    platform_account_id: text(context.accountId, 'context.accountId'), date: text(context.businessDate || insight.date_start, 'context.businessDate')
  };
  const entity = {
    campaign_type: null, root_entity_type: 'campaign', root_entity_id: text(insight.campaign_id, 'campaign_id'),
    root_entity_name: text(insight.campaign_name, 'campaign_name'), parent_entity_type: 'adset',
    parent_entity_id: text(insight.adset_id, 'adset_id'), parent_entity_name: text(insight.adset_name, 'adset_name'),
    entity_type: 'ad', entity_id: text(insight.ad_id, 'ad_id'), entity_name: text(insight.ad_name, 'ad_name')
  };
  const values = {
    impression: metric(insight.impressions, 'impressions'),
    ad_click: actionValue(insight.actions, META_CAPABILITIES.actionPriority.ad_click),
    session: null,
    spend_value: metric(insight.spend, 'spend'),
    add_to_cart: actionValue(insight.actions, META_CAPABILITIES.actionPriority.add_to_cart),
    add_to_cart_value: actionValue(insight.action_values, META_CAPABILITIES.actionPriority.add_to_cart_value),
    checkout: actionValue(insight.actions, META_CAPABILITIES.actionPriority.checkout),
    checkout_value: actionValue(insight.action_values, META_CAPABILITIES.actionPriority.checkout_value),
    purchase: actionValue(insight.actions, META_CAPABILITIES.actionPriority.purchase),
    purchase_value: actionValue(insight.action_values, META_CAPABILITIES.actionPriority.purchase_value)
  };
  const support = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, key === 'session' ? 'unsupported' : supported(value)]));
  const sourceCurrency = text(context.sourceCurrency || insight.account_currency, 'context.sourceCurrency');
  const targetCurrency = text(context.targetCurrency || sourceCurrency, 'context.targetCurrency');
  if (sourceCurrency !== targetCurrency) throw new Error('Cross-currency Meta mapping requires E4-T4 FX binding');
  const row = {
    identity, entity, raw_metrics: values, metric_support: support,
    currency: { source_currency: sourceCurrency, target_currency: targetCurrency, fx_rate: context.fxRate ?? null,
      fx_rate_date: context.fxRateDate ?? null, fx_provider: context.fxProvider ?? null, fx_engine_version: context.fxEngineVersion ?? null },
    time: { source_timezone: text(context.sourceTimezone, 'context.sourceTimezone'), business_date: identity.date,
      time_engine_version: text(context.timeEngineVersion || 'v1', 'context.timeEngineVersion') },
    provenance: { source_system: 'meta_ads', adapter_version: ADAPTER_VERSION, source_confidence: 'real', synthetic: false,
      ga4_property_id: null, raw_reference: { date_start: insight.date_start || null, date_stop: insight.date_stop || null,
        action_types: [...new Set([...(insight.actions || []), ...(insight.action_values || [])].map(item => item && item.action_type).filter(Boolean))].sort() } }
  };
  validateCanonicalRow(row); validateEntityHierarchy(identity, entity);
  return Object.freeze({ row, entityKey: buildEntityKey(identity, entity) });
}

module.exports = Object.freeze({ ADAPTER_VERSION, actionValue, mapMetaInsight });

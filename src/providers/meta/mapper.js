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
function selectAction(list, priority, sourceField) {
  if (!Array.isArray(list)) return Object.freeze({ value: null, source_field: sourceField, action_type: null, fallback_used: false });
  for (let index = 0; index < priority.length; index += 1) {
    const type = priority[index];
    const match = list.find(item => item && item.action_type === type);
    if (match) return Object.freeze({ value: metric(match.value, `action.${type}`), source_field: sourceField, action_type: type, fallback_used: index > 0 });
  }
  return Object.freeze({ value: null, source_field: sourceField, action_type: null, fallback_used: false });
}
function actionValue(list, priority) { return selectAction(list, priority, 'actions').value; }
function sourceRef(selection) { return { source_field: selection.source_field, action_type: selection.action_type, fallback_used: selection.fallback_used }; }

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
  const selections = {
    ad_click: selectAction(insight.actions, META_CAPABILITIES.actionPriority.ad_click, 'actions'),
    add_to_cart: selectAction(insight.actions, META_CAPABILITIES.actionPriority.add_to_cart, 'actions'),
    add_to_cart_value: selectAction(insight.action_values, META_CAPABILITIES.actionPriority.add_to_cart_value, 'action_values'),
    checkout: selectAction(insight.actions, META_CAPABILITIES.actionPriority.checkout, 'actions'),
    checkout_value: selectAction(insight.action_values, META_CAPABILITIES.actionPriority.checkout_value, 'action_values'),
    purchase: selectAction(insight.actions, META_CAPABILITIES.actionPriority.purchase, 'actions'),
    purchase_value: selectAction(insight.action_values, META_CAPABILITIES.actionPriority.purchase_value, 'action_values')
  };
  const values = {
    impression: metric(insight.impressions, 'impressions'), ad_click: selections.ad_click.value, session: null,
    spend_value: metric(insight.spend, 'spend'), add_to_cart: selections.add_to_cart.value,
    add_to_cart_value: selections.add_to_cart_value.value, checkout: selections.checkout.value,
    checkout_value: selections.checkout_value.value, purchase: selections.purchase.value, purchase_value: selections.purchase_value.value
  };
  const support = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, key === 'session' ? 'unsupported' : supported(value)]));
  const sourceCurrency = text(context.sourceCurrency || insight.account_currency, 'context.sourceCurrency');
  const targetCurrency = text(context.targetCurrency || sourceCurrency, 'context.targetCurrency');
  if (sourceCurrency !== targetCurrency) throw new Error('Cross-currency Meta mapping requires E4-T4 FX binding');
  const hasUnknown = Object.entries(support).some(([key, value]) => key !== 'session' && value === 'unknown');
  const hasFallback = Object.values(selections).some(selection => selection.fallback_used);
  const metricSources = {
    impression: { source_field: 'impressions', action_type: null, fallback_used: false },
    ad_click: sourceRef(selections.ad_click), session: { source_field: null, action_type: null, fallback_used: false },
    spend_value: { source_field: 'spend', action_type: null, fallback_used: false },
    add_to_cart: sourceRef(selections.add_to_cart), add_to_cart_value: sourceRef(selections.add_to_cart_value),
    checkout: sourceRef(selections.checkout), checkout_value: sourceRef(selections.checkout_value),
    purchase: sourceRef(selections.purchase), purchase_value: sourceRef(selections.purchase_value)
  };
  const row = {
    identity, entity, raw_metrics: values, metric_support: support,
    currency: { source_currency: sourceCurrency, target_currency: targetCurrency, fx_rate: context.fxRate ?? null,
      fx_rate_date: context.fxRateDate ?? null, fx_provider: context.fxProvider ?? null, fx_engine_version: context.fxEngineVersion ?? null },
    time: { source_timezone: text(context.sourceTimezone, 'context.sourceTimezone'), business_date: identity.date,
      time_engine_version: text(context.timeEngineVersion || 'v1', 'context.timeEngineVersion') },
    provenance: { source_system: 'meta_ads', adapter_version: ADAPTER_VERSION, source_confidence: hasUnknown ? 'partial' : hasFallback ? 'fallback' : 'real', synthetic: false,
      ga4_property_id: null, source_job_id: context.sourceJobId || null, raw_reference: { date_start: insight.date_start || null, date_stop: insight.date_stop || null, metric_sources: metricSources } }
  };
  validateCanonicalRow(row); validateEntityHierarchy(identity, entity);
  return Object.freeze({ row, entityKey: buildEntityKey(identity, entity) });
}

module.exports = Object.freeze({ ADAPTER_VERSION, actionValue, mapMetaInsight, selectAction });

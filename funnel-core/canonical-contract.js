'use strict';

const CANONICAL_CONTRACT_VERSION = 'v1';

const PLATFORMS = Object.freeze(['meta', 'google', 'tiktok', 'klaviyo']);
const TRAFFIC_TYPES = Object.freeze(['paid', 'organic']);
const SOURCE_SYSTEMS = Object.freeze(['meta_ads', 'google_ads', 'tiktok_ads', 'klaviyo', 'ga4']);
const CHANNELS = Object.freeze(['email', 'sms']);
const METRIC_SUPPORT = Object.freeze(['supported', 'unsupported', 'unknown']);
const SOURCE_CONFIDENCE = Object.freeze(['real', 'fallback', 'partial']);

const RAW_METRICS = Object.freeze([
  'impression',
  'ad_click',
  'session',
  'spend_value',
  'add_to_cart',
  'add_to_cart_value',
  'checkout',
  'checkout_value',
  'purchase',
  'purchase_value'
]);

const MONETARY_METRICS = Object.freeze([
  'spend_value',
  'add_to_cart_value',
  'checkout_value',
  'purchase_value'
]);

const SOURCE_BY_PLATFORM = Object.freeze({
  meta: 'meta_ads',
  google: 'google_ads',
  tiktok: 'tiktok_ads',
  klaviyo: 'klaviyo'
});

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isDateOnly(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function assertString(value, field, { nullable = false } = {}) {
  if (nullable && value === null) return;
  assert(typeof value === 'string' && value.trim() !== '', `${field} must be a non-empty string${nullable ? ' or null' : ''}`);
}

function validateIdentity(identity) {
  assert(isPlainObject(identity), 'identity must be an object');
  assertString(identity.user_id, 'identity.user_id');
  assert(PLATFORMS.includes(identity.platform), `Unsupported platform: ${identity.platform}`);
  assert(TRAFFIC_TYPES.includes(identity.traffic_type), `Unsupported traffic_type: ${identity.traffic_type}`);
  assert(SOURCE_SYSTEMS.includes(identity.source_system), `Unsupported source_system: ${identity.source_system}`);
  assertString(identity.platform_account_id, 'identity.platform_account_id');
  assert(isDateOnly(identity.date), 'identity.date must be YYYY-MM-DD');

  const channel = identity.channel === undefined ? null : identity.channel;
  assert(channel === null || CHANNELS.includes(channel), `Unsupported channel: ${channel}`);

  if (identity.traffic_type === 'organic') {
    assert(identity.source_system === 'ga4', 'Organic rows must use source_system=ga4');
    assert(channel === null, 'Organic rows must have channel=null');
  } else {
    assert(identity.source_system === SOURCE_BY_PLATFORM[identity.platform], `Paid ${identity.platform} row must use source_system=${SOURCE_BY_PLATFORM[identity.platform]}`);
    if (identity.platform === 'klaviyo') {
      assert(CHANNELS.includes(channel), 'Paid Klaviyo rows must carry channel=email|sms');
    } else {
      assert(channel === null, `Paid ${identity.platform} rows must have channel=null`);
    }
  }
}

function validateRawMetrics(rawMetrics, metricSupport) {
  assert(isPlainObject(rawMetrics), 'raw_metrics must be an object');
  assert(isPlainObject(metricSupport), 'metric_support must be an object');

  for (const metric of RAW_METRICS) {
    assert(Object.prototype.hasOwnProperty.call(rawMetrics, metric), `raw_metrics.${metric} is required`);
    assert(Object.prototype.hasOwnProperty.call(metricSupport, metric), `metric_support.${metric} is required`);

    const value = rawMetrics[metric];
    const support = metricSupport[metric];

    assert(METRIC_SUPPORT.includes(support), `Invalid metric support for ${metric}: ${support}`);
    assert(value === null || (typeof value === 'number' && Number.isFinite(value)), `raw_metrics.${metric} must be number|null`);

    if (support === 'supported') {
      assert(value !== null, `Supported metric ${metric} must carry a measured number (0 is valid)`);
    } else {
      assert(value === null, `${support} metric ${metric} must be null, never a synthetic zero`);
    }
  }
}

function validateCurrency(currency) {
  assert(isPlainObject(currency), 'currency must be an object');
  assertString(currency.source_currency, 'currency.source_currency');
  assertString(currency.target_currency, 'currency.target_currency');

  if (currency.fx_rate !== null && currency.fx_rate !== undefined) {
    assert(typeof currency.fx_rate === 'number' && Number.isFinite(currency.fx_rate) && currency.fx_rate > 0, 'currency.fx_rate must be a positive number|null');
  }
  if (currency.fx_rate_date !== null && currency.fx_rate_date !== undefined) {
    assert(isDateOnly(currency.fx_rate_date), 'currency.fx_rate_date must be YYYY-MM-DD|null');
  }
  if (currency.fx_provider !== null && currency.fx_provider !== undefined) {
    assertString(currency.fx_provider, 'currency.fx_provider');
  }
  if (currency.fx_engine_version !== null && currency.fx_engine_version !== undefined) {
    assertString(currency.fx_engine_version, 'currency.fx_engine_version');
  }
}

function validateTime(time, identity) {
  assert(isPlainObject(time), 'time must be an object');
  assertString(time.source_timezone, 'time.source_timezone');
  assert(isDateOnly(time.business_date), 'time.business_date must be YYYY-MM-DD');
  assert(time.business_date === identity.date, 'identity.date and time.business_date must describe the same canonical business date');
  assertString(time.time_engine_version, 'time.time_engine_version');
}

function validateProvenance(provenance, identity, { allowSynthetic = false } = {}) {
  assert(isPlainObject(provenance), 'provenance must be an object');
  assert(provenance.source_system === identity.source_system, 'provenance.source_system must match identity.source_system');
  assertString(provenance.adapter_version, 'provenance.adapter_version');
  assert(SOURCE_CONFIDENCE.includes(provenance.source_confidence), `Invalid source_confidence: ${provenance.source_confidence}`);
  assert(typeof provenance.synthetic === 'boolean', 'provenance.synthetic must be boolean');
  assert(allowSynthetic || provenance.synthetic === false, 'Synthetic rows cannot be treated as production canonical performance');
  assert(isPlainObject(provenance.raw_reference), 'provenance.raw_reference must be an object');

  const ga4PropertyId = provenance.ga4_property_id === undefined ? null : provenance.ga4_property_id;
  if (identity.traffic_type === 'organic') {
    assertString(ga4PropertyId, 'provenance.ga4_property_id');
  } else {
    assert(ga4PropertyId === null || typeof ga4PropertyId === 'string', 'provenance.ga4_property_id must be string|null');
  }
}

function validateEntityShape(entity) {
  assert(isPlainObject(entity), 'entity must be an object');
  const allowedCampaignTypes = ['standard', 'performance_max', null];
  assert(allowedCampaignTypes.includes(entity.campaign_type ?? null), `Unsupported campaign_type: ${entity.campaign_type}`);

  const nullableStrings = [
    'root_entity_type', 'root_entity_id', 'root_entity_name',
    'parent_entity_type', 'parent_entity_id', 'parent_entity_name'
  ];
  for (const field of nullableStrings) {
    const value = entity[field] === undefined ? null : entity[field];
    assert(value === null || (typeof value === 'string' && value.trim() !== ''), `entity.${field} must be string|null`);
  }

  assertString(entity.entity_type, 'entity.entity_type');
  assertString(entity.entity_id, 'entity.entity_id');
  assertString(entity.entity_name, 'entity.entity_name');
}

function validateCanonicalRow(row, options = {}) {
  assert(isPlainObject(row), 'canonical row must be an object');
  validateIdentity(row.identity);
  validateEntityShape(row.entity);
  validateRawMetrics(row.raw_metrics, row.metric_support);
  validateCurrency(row.currency);
  validateTime(row.time, row.identity);
  validateProvenance(row.provenance, row.identity, options);
  return row;
}

function cloneCanonicalRow(row) {
  return JSON.parse(JSON.stringify(row));
}

module.exports = Object.freeze({
  CANONICAL_CONTRACT_VERSION,
  PLATFORMS,
  TRAFFIC_TYPES,
  SOURCE_SYSTEMS,
  CHANNELS,
  METRIC_SUPPORT,
  SOURCE_CONFIDENCE,
  RAW_METRICS,
  MONETARY_METRICS,
  SOURCE_BY_PLATFORM,
  isDateOnly,
  validateIdentity,
  validateRawMetrics,
  validateCurrency,
  validateTime,
  validateProvenance,
  validateEntityShape,
  validateCanonicalRow,
  cloneCanonicalRow
});

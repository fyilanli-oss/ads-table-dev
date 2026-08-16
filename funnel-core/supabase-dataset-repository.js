'use strict';

const { DatasetRepository } = require('./dataset-repository');
const {
  CANONICAL_CONTRACT_VERSION,
  RAW_METRICS,
  validateCanonicalRow,
  cloneCanonicalRow
} = require('./canonical-contract');
const { validateEntityHierarchy, buildEntityKey } = require('./entity-hierarchy');

const TABLE = 'performance_dataset_rows_v2';
const UPSERT_CONFLICT = 'user_id,platform,platform_account_id,business_date,traffic_type,entity_key';

const METRIC_TO_COLUMN = Object.freeze({
  impression: 'impressions',
  ad_click: 'ad_clicks',
  session: 'sessions',
  spend_value: 'spend',
  add_to_cart: 'add_to_cart',
  add_to_cart_value: 'add_to_cart_value',
  checkout: 'checkout',
  checkout_value: 'checkout_value',
  purchase: 'purchase',
  purchase_value: 'purchase_value'
});

function requireNonEmpty(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(`${field} is required for Dataset V2 persistence`);
  return value;
}

function requirePersistenceReadyCurrency(currency) {
  requireNonEmpty(currency.source_currency, 'currency.source_currency');
  requireNonEmpty(currency.target_currency, 'currency.target_currency');
  if (typeof currency.fx_rate !== 'number' || !Number.isFinite(currency.fx_rate) || currency.fx_rate <= 0) {
    throw new Error('currency.fx_rate must be a positive number before Dataset V2 persistence');
  }
  requireNonEmpty(currency.fx_rate_date, 'currency.fx_rate_date');
  requireNonEmpty(currency.fx_provider, 'currency.fx_provider');
  requireNonEmpty(currency.fx_engine_version, 'currency.fx_engine_version');
}

function canonicalToDbRow(row) {
  validateCanonicalRow(row);
  validateEntityHierarchy(row.identity, row.entity);
  requirePersistenceReadyCurrency(row.currency);

  const entityKey = buildEntityKey(row.identity, row.entity);
  const db = {
    user_id: row.identity.user_id,
    platform: row.identity.platform,
    traffic_type: row.identity.traffic_type,
    source_system: row.identity.source_system,
    channel: row.identity.channel ?? null,
    platform_account_id: row.identity.platform_account_id,
    business_date: row.time.business_date,

    campaign_type: row.entity.campaign_type ?? null,
    root_entity_type: row.entity.root_entity_type,
    root_entity_id: row.entity.root_entity_id,
    root_entity_name: row.entity.root_entity_name ?? null,
    parent_entity_type: row.entity.parent_entity_type ?? null,
    parent_entity_id: row.entity.parent_entity_id ?? null,
    parent_entity_name: row.entity.parent_entity_name ?? null,
    entity_type: row.entity.entity_type,
    entity_id: row.entity.entity_id,
    entity_name: row.entity.entity_name,
    entity_key: entityKey,

    metric_support: JSON.parse(JSON.stringify(row.metric_support)),

    source_currency: row.currency.source_currency,
    target_currency: row.currency.target_currency,
    fx_rate: row.currency.fx_rate,
    fx_rate_date: row.currency.fx_rate_date,
    fx_provider: row.currency.fx_provider,
    fx_engine_version: row.currency.fx_engine_version,

    source_timezone: row.time.source_timezone,
    time_engine_version: row.time.time_engine_version,

    canonical_contract_version: CANONICAL_CONTRACT_VERSION,
    adapter_version: row.provenance.adapter_version,
    source_confidence: row.provenance.source_confidence,
    synthetic: row.provenance.synthetic,
    ga4_property_id: row.provenance.ga4_property_id ?? null,
    source_job_id: row.provenance.source_job_id ?? null,
    raw: JSON.parse(JSON.stringify(row.provenance.raw_reference || {})),
    updated_at: new Date().toISOString()
  };

  for (const metric of RAW_METRICS) db[METRIC_TO_COLUMN[metric]] = row.raw_metrics[metric];
  return db;
}

function numberOrNull(value) {
  if (value === null || value === undefined) return null;
  const number = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(number)) throw new Error(`Invalid numeric value read from Dataset V2: ${value}`);
  return number;
}

function dbToCanonicalRow(db) {
  if (!db || typeof db !== 'object') throw new Error('Dataset V2 DB row must be an object');

  const rawMetrics = {};
  for (const metric of RAW_METRICS) rawMetrics[metric] = numberOrNull(db[METRIC_TO_COLUMN[metric]]);

  const row = {
    identity: {
      user_id: db.user_id,
      platform: db.platform,
      traffic_type: db.traffic_type,
      source_system: db.source_system,
      channel: db.channel ?? null,
      platform_account_id: db.platform_account_id,
      date: db.business_date
    },
    entity: {
      campaign_type: db.campaign_type ?? null,
      root_entity_type: db.root_entity_type,
      root_entity_id: db.root_entity_id,
      root_entity_name: db.root_entity_name ?? null,
      parent_entity_type: db.parent_entity_type ?? null,
      parent_entity_id: db.parent_entity_id ?? null,
      parent_entity_name: db.parent_entity_name ?? null,
      entity_type: db.entity_type,
      entity_id: db.entity_id,
      entity_name: db.entity_name
    },
    raw_metrics: rawMetrics,
    metric_support: JSON.parse(JSON.stringify(db.metric_support)),
    currency: {
      source_currency: db.source_currency,
      target_currency: db.target_currency,
      fx_rate: numberOrNull(db.fx_rate),
      fx_rate_date: db.fx_rate_date,
      fx_provider: db.fx_provider,
      fx_engine_version: db.fx_engine_version
    },
    time: {
      source_timezone: db.source_timezone,
      business_date: db.business_date,
      time_engine_version: db.time_engine_version
    },
    provenance: {
      source_system: db.source_system,
      adapter_version: db.adapter_version,
      source_confidence: db.source_confidence,
      synthetic: db.synthetic,
      ga4_property_id: db.ga4_property_id ?? null,
      source_job_id: db.source_job_id ?? null,
      raw_reference: JSON.parse(JSON.stringify(db.raw || {}))
    },
    entity_key: db.entity_key,
    canonical_contract_version: db.canonical_contract_version
  };

  validateCanonicalRow(row);
  validateEntityHierarchy(row.identity, row.entity);
  const expectedKey = buildEntityKey(row.identity, row.entity);
  if (db.entity_key !== expectedKey) throw new Error('Dataset V2 entity_key does not match canonical hierarchy identity');
  return row;
}

class SupabaseDatasetRepository extends DatasetRepository {
  constructor(supabaseClient) {
    super();
    if (!supabaseClient || typeof supabaseClient.from !== 'function') {
      throw new Error('A server-side Supabase client is required');
    }
    this.supabase = supabaseClient;
  }

  async upsertCanonicalRawFacts(rows) {
    if (!Array.isArray(rows)) throw new Error('rows must be an array');
    if (rows.length === 0) return [];

    const payload = rows.map(canonicalToDbRow);
    const { data, error } = await this.supabase
      .from(TABLE)
      .upsert(payload, { onConflict: UPSERT_CONFLICT })
      .select('*');

    if (error) throw new Error(`Dataset V2 upsert failed: ${error.message || error}`);
    return (data || []).map(dbToCanonicalRow).map(cloneCanonicalRow);
  }

  async readCanonicalRawFacts({ user_id, from, to, platform = null, platform_account_id = null, traffic_type = null, entity_key = null } = {}) {
    if (!user_id) throw new Error('user_id is required');
    if (!from || !to) throw new Error('from and to are required');

    let query = this.supabase
      .from(TABLE)
      .select('*')
      .eq('user_id', user_id)
      .gte('business_date', from)
      .lte('business_date', to);

    if (platform) query = query.eq('platform', platform);
    if (platform_account_id) query = query.eq('platform_account_id', platform_account_id);
    if (traffic_type) query = query.eq('traffic_type', traffic_type);
    if (entity_key) query = query.eq('entity_key', entity_key);

    const { data, error } = await query.order('business_date', { ascending: true }).order('entity_key', { ascending: true });
    if (error) throw new Error(`Dataset V2 read failed: ${error.message || error}`);
    return (data || []).map(dbToCanonicalRow).map(cloneCanonicalRow);
  }
}

module.exports = Object.freeze({
  TABLE,
  UPSERT_CONFLICT,
  METRIC_TO_COLUMN,
  canonicalToDbRow,
  dbToCanonicalRow,
  SupabaseDatasetRepository
});

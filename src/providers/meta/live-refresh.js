'use strict';

const { CanonicalWriteBoundary } = require('../../../funnel-core/canonical-write-boundary');
const { SupabaseDatasetRepository } = require('../../../funnel-core/supabase-dataset-repository');
const { createMetaAdapter } = require('./adapter');
const { AD_INSIGHT_FIELDS, createMetaClient } = require('./client');
const { createMetaDatasetWriter } = require('./dataset-writer');

const LIVE_BACKFILL_DAY_COUNT = 30;

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function isoDate(value, field) {
  const normalized = required(value, field);
  const parsed = new Date(`${normalized}T00:00:00.000Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized) || Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== normalized) throw new TypeError(`${field} must be YYYY-MM-DD`);
  return normalized;
}

function backfillRange(until, dayCount = LIVE_BACKFILL_DAY_COUNT) {
  const end = isoDate(until, 'until');
  if (!Number.isInteger(dayCount) || dayCount < 1 || dayCount > LIVE_BACKFILL_DAY_COUNT) throw new RangeError(`dayCount must be between 1 and ${LIVE_BACKFILL_DAY_COUNT}`);
  const start = new Date(`${end}T00:00:00.000Z`); start.setUTCDate(start.getUTCDate() - dayCount + 1);
  return Object.freeze({ since: start.toISOString().slice(0, 10), until: end, dayCount });
}

function createMetaLiveRefresh({ supabaseClient, resolveTargetCurrency, resolveFxRate, graphVersion } = {}) {
  if (!supabaseClient || typeof supabaseClient.from !== 'function') throw new TypeError('server-side Supabase client is required');
  if (typeof resolveTargetCurrency !== 'function' || typeof resolveFxRate !== 'function') throw new TypeError('currency resolvers are required');
  const repository = new SupabaseDatasetRepository(supabaseClient);
  const writeBoundary = new CanonicalWriteBoundary({ repository });

  return Object.freeze({
    async run({ userId, accessToken, accountId, until, dayCount = LIVE_BACKFILL_DAY_COUNT, sourceJobId = null, limit = 100 } = {}) {
      const range = backfillRange(until, dayCount);
      const client = createMetaClient({ accessToken, graphVersion });
      const accounts = await client.listAccounts();
      const account = accounts.data?.find(item => item?.id === required(accountId, 'accountId'));
      if (!account) throw new Error('Selected Meta account was not returned by Meta');
      const targetCurrency = required(await resolveTargetCurrency(required(userId, 'userId')), 'targetCurrency');
      const response = await client.fetchAdInsights({ accountId: account.id, since: range.since, until: range.until, limit });
      const byDate = new Map();
      for (const insight of response.data) {
        const date = isoDate(insight?.date_start, 'insight.date_start');
        if (date < range.since || date > range.until || insight.date_stop !== date) throw new Error('Meta insight is outside the requested daily range');
        if (!byDate.has(date)) byDate.set(date, []); byDate.get(date).push(insight);
      }
      const mappedResults = [];
      for (const [date, insights] of [...byDate.entries()].sort(([left], [right]) => left.localeCompare(right))) {
        const fx = await resolveFxRate(required(account.currency, 'Meta account currency'), targetCurrency, { rateDate: date });
        const adapter = createMetaAdapter({ client: { fetchAdInsights: async () => ({ data: insights }) } });
        mappedResults.push(...await adapter.fetchCanonicalRows({ accountId: account.id, since: date, until: date, limit, context: { userId, account, sourceJobId, targetCurrency, fxRate: fx.fx_rate, fxRateDate: fx.fx_rate_date || date, fxProvider: fx.fx_provider } }));
      }
      const writer = createMetaDatasetWriter({ adapter: { fetchCanonicalRows: async () => mappedResults }, writeBoundary });
      const write = await writer.ingest({ accountId: account.id, context: { userId, account } });
      const { attempted, persisted } = write;
      const providerResponse = client.getLastInsightsEvidence();
      const metaV2Evidence = Object.freeze({
        evidence_version: 'e4-meta-live-v1',
        provider_request: Object.freeze({ level: 'ad', since: range.since, until: range.until, day_count: range.dayCount, time_increment: 1, requested_fields: AD_INSIGHT_FIELDS }),
        provider_response: providerResponse,
        mapping: Object.freeze({ accepted_row_count: attempted, rejected_row_count: 0, rejection_codes: Object.freeze([]) }),
        dataset_v2: Object.freeze({ attempted, persisted, empty_provider_result: persisted === 0 })
      });
      return Object.freeze({
        mode: 'v2_upsert', snapshot: null,
        row_counts: { ad: persisted, total: persisted },
        performance_spread_result: null,
        dataset_v2: metaV2Evidence.dataset_v2,
        meta_v2_evidence: metaV2Evidence
      });
    }
  });
}

module.exports = Object.freeze({ LIVE_BACKFILL_DAY_COUNT, backfillRange, createMetaLiveRefresh });

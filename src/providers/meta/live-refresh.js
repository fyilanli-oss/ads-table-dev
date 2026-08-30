'use strict';

const { CanonicalWriteBoundary } = require('../../../funnel-core/canonical-write-boundary');
const { SupabaseDatasetRepository } = require('../../../funnel-core/supabase-dataset-repository');
const { createMetaAdapter } = require('./adapter');
const { AD_INSIGHT_FIELDS, createMetaClient } = require('./client');
const { createMetaDatasetWriter } = require('./dataset-writer');

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function businessDateRange(since, until) {
  const start = required(since, 'since'), end = required(until, 'until');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || start !== end) throw new Error('Meta Refresh must use one business date');
  const parsed = new Date(`${start}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== start) throw new TypeError('business date must be YYYY-MM-DD');
  return Object.freeze({ since: start, until: end });
}

function createMetaLiveRefresh({ supabaseClient, resolveTargetCurrency, resolveFxRate, graphVersion } = {}) {
  if (!supabaseClient || typeof supabaseClient.from !== 'function') throw new TypeError('server-side Supabase client is required');
  if (typeof resolveTargetCurrency !== 'function' || typeof resolveFxRate !== 'function') throw new TypeError('currency resolvers are required');
  const repository = new SupabaseDatasetRepository(supabaseClient);
  const writeBoundary = new CanonicalWriteBoundary({ repository });

  return Object.freeze({
    async run({ userId, accessToken, accountId, since, until, sourceJobId = null, limit = 100 } = {}) {
      const range = businessDateRange(since, until);
      const client = createMetaClient({ accessToken, graphVersion });
      const accounts = await client.listAccounts();
      const account = accounts.data?.find(item => item?.id === required(accountId, 'accountId'));
      if (!account) throw new Error('Selected Meta account was not returned by Meta');
      const targetCurrency = required(await resolveTargetCurrency(required(userId, 'userId')), 'targetCurrency');
      const fx = await resolveFxRate(required(account.currency, 'Meta account currency'), targetCurrency, { rateDate: range.since });
      const adapter = createMetaAdapter({ client });
      const writer = createMetaDatasetWriter({ adapter, writeBoundary });
      const write = await writer.ingest({ accountId: account.id, since: range.since, until: range.until, limit, context: { userId, account, sourceJobId, targetCurrency, fxRate: fx.fx_rate, fxRateDate: fx.fx_rate_date || range.since, fxProvider: fx.fx_provider } });
      const { attempted, persisted } = write;
      const metaV2Evidence = Object.freeze({
        evidence_version: 'e4-meta-live-v1',
        provider_request: Object.freeze({ level: 'ad', since: range.since, until: range.until, day_count: 1, time_increment: 1, requested_fields: AD_INSIGHT_FIELDS }),
        provider_response: client.getLastInsightsEvidence(),
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

module.exports = Object.freeze({ businessDateRange, createMetaLiveRefresh });

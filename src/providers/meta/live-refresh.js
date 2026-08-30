'use strict';

const { CanonicalWriteBoundary } = require('../../../funnel-core/canonical-write-boundary');
const { SupabaseDatasetRepository } = require('../../../funnel-core/supabase-dataset-repository');
const { createMetaAdapter } = require('./adapter');
const { createMetaClient } = require('./client');
const { createMetaDatasetWriter } = require('./dataset-writer');

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function createMetaLiveRefresh({ supabaseClient, resolveTargetCurrency, resolveFxRate, graphVersion } = {}) {
  if (!supabaseClient || typeof supabaseClient.from !== 'function') throw new TypeError('server-side Supabase client is required');
  if (typeof resolveTargetCurrency !== 'function' || typeof resolveFxRate !== 'function') throw new TypeError('currency resolvers are required');
  const repository = new SupabaseDatasetRepository(supabaseClient);
  const writeBoundary = new CanonicalWriteBoundary({ repository });

  return Object.freeze({
    async run({ userId, accessToken, accountId, since, until, sourceJobId = null, limit = 100 } = {}) {
      const client = createMetaClient({ accessToken, graphVersion });
      const accounts = await client.listAccounts();
      const account = accounts.data?.find(item => item?.id === required(accountId, 'accountId'));
      if (!account) throw new Error('Selected Meta account was not returned by Meta');
      const targetCurrency = required(await resolveTargetCurrency(required(userId, 'userId')), 'targetCurrency');
      const fx = await resolveFxRate(required(account.currency, 'Meta account currency'), targetCurrency, { rateDate: required(since, 'since') });
      const adapter = createMetaAdapter({ client });
      const writer = createMetaDatasetWriter({ adapter, writeBoundary });
      const write = await writer.ingest({
        accountId: account.id, since, until, limit,
        context: {
          userId, account, sourceJobId, targetCurrency,
          fxRate: fx.fx_rate, fxRateDate: fx.fx_rate_date || since,
          fxProvider: fx.fx_provider
        }
      });
      return Object.freeze({
        mode: 'v2_upsert', snapshot: null,
        row_counts: { ad: write.persisted, total: write.persisted },
        performance_spread_result: null,
        dataset_v2: { attempted: write.attempted, persisted: write.persisted, empty_provider_result: write.persisted === 0 }
      });
    }
  });
}

module.exports = Object.freeze({ createMetaLiveRefresh });

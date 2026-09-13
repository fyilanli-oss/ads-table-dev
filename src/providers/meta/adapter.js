'use strict';

const { META_CAPABILITIES } = require('./capabilities');
const { mapMetaInsight } = require('./mapper');
const { normalizeMetaAccount, normalizeMetaMappedResult } = require('./normalization');

function createMetaAdapter({ client } = {}) {
  if (!client || typeof client.fetchAdInsights !== 'function') throw new TypeError('Meta client is required');
  return Object.freeze({
    capabilities: META_CAPABILITIES,
    async fetchCanonicalRows(input) {
      const account = normalizeMetaAccount(input?.context?.account, input?.accountId);
      const response = await client.fetchAdInsights(input);
      if (!response || !Array.isArray(response.data)) throw new Error('Meta insights response must contain data[]');
      return response.data.map((insight) => {
        const mapped = mapMetaInsight(insight, {
          ...input.context, accountId: account.id, businessDate: insight.date_start,
          sourceTimezone: account.sourceTimezone, sourceCurrency: account.sourceCurrency,
          targetCurrency: account.sourceCurrency, fxRate: 1, fxRateDate: insight.date_start,
          fxProvider: 'same_currency', fxEngineVersion: 'v1', timeEngineVersion: 'v1'
        });
        return normalizeMetaMappedResult(mapped, insight, input);
      });
    }
  });
}

module.exports = Object.freeze({ createMetaAdapter });

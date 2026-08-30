'use strict';

const { META_CAPABILITIES } = require('./capabilities');
const { mapMetaInsight } = require('./mapper');

function createMetaAdapter({ client } = {}) {
  if (!client || typeof client.fetchAdInsights !== 'function') throw new TypeError('Meta client is required');
  return Object.freeze({
    capabilities: META_CAPABILITIES,
    async fetchCanonicalRows(input) {
      const response = await client.fetchAdInsights(input);
      if (!response || !Array.isArray(response.data)) throw new Error('Meta insights response must contain data[]');
      return response.data.map(insight => mapMetaInsight(insight, input.context));
    }
  });
}

module.exports = Object.freeze({ createMetaAdapter });

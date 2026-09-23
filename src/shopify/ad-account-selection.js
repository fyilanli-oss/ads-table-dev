'use strict';

const { discoverGoogleCustomers } = require('../providers/google/customer-discovery');

const PROVIDERS = Object.freeze(['meta', 'google_ads']);

function failure(code, status = 503) { return Object.assign(new Error(code), {code, status}); }
function validAccount(item) {
  if (!item || typeof item.id !== 'string' || !item.id || typeof item.name !== 'string' || !item.name || !/^[A-Z]{3}$/.test(item.currency || '')) {
    throw failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
  }
  return Object.freeze({id: item.id.slice(0, 256), name: item.name.slice(0, 256), currency: item.currency});
}

function createMetaAccountDiscovery({fetchImpl = fetch, graphVersion = 'v20.0'} = {}) {
  return async accessToken => {
    const response = await fetchImpl(`https://graph.facebook.com/${graphVersion}/me/adaccounts?fields=id,name,currency,account_status&limit=100`, {
      headers: {Authorization: `Bearer ${accessToken}`}, signal: AbortSignal.timeout(15000), redirect: 'error',
    });
    if (response.status === 401 || response.status === 403) throw failure('PROVIDER_REAUTHORIZE', 409);
    if (!response.ok) throw failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
    const body = await response.json();
    return (Array.isArray(body?.data) ? body.data : []).map(item => validAccount({id: item.id, name: item.name || item.id, currency: item.currency}));
  };
}

function createGoogleAdsAccountDiscovery({fetchImpl = fetch, developerToken, apiVersion = 'v25'} = {}) {
  if (typeof developerToken !== 'string' || !developerToken.trim()) throw new TypeError('Google Ads developer token is required');
  const headers = accessToken => ({Authorization: `Bearer ${accessToken}`, 'developer-token': developerToken, 'Content-Type': 'application/json'});
  return async accessToken => {
    const accessible = await fetchImpl(`https://googleads.googleapis.com/${apiVersion}/customers:listAccessibleCustomers`, {
      headers: headers(accessToken), signal: AbortSignal.timeout(15000), redirect: 'error',
    });
    if (accessible.status === 401 || accessible.status === 403) throw failure('PROVIDER_REAUTHORIZE', 409);
    if (!accessible.ok) throw failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
    const body = await accessible.json();
    const resourceNames = Array.isArray(body?.resourceNames) ? body.resourceNames : [];
    const discovered = await discoverGoogleCustomers({resourceNames, search: async ({customerId, query}) => {
      const response = await fetchImpl(`https://googleads.googleapis.com/${apiVersion}/customers/${customerId}/googleAds:searchStream`, {
        method: 'POST', headers: headers(accessToken), body: JSON.stringify({query}), signal: AbortSignal.timeout(15000), redirect: 'error',
      });
      if (!response.ok) throw new Error('GOOGLE_CUSTOMER_DISCOVERY_FAILED');
      const chunks = await response.json();
      return {results: (Array.isArray(chunks) ? chunks : []).flatMap(chunk => Array.isArray(chunk?.results) ? chunk.results : [])};
    }});
    return discovered.customers.map(item => validAccount({id: item.customerId, name: item.account_name, currency: item.currency}));
  };
}

function createAdAccountSelection({store, discoverByProvider} = {}) {
  return Object.freeze({
    async status(authority, provider) {
      const row = await store.readStatus({authority, provider});
      return {status: row?.status || 'not_connected'};
    },
    async list(authority, provider) {
      if (!PROVIDERS.includes(provider)) throw failure('PROVIDER_NOT_FOUND', 404);
      const connection = await store.readPendingProvider({authority, provider});
      if (!connection) return {status: 'not_connected', accounts: []};
      if (connection.status !== 'pending_account_selection' || !connection.accessToken) return {status: connection.status, accounts: []};
      const accounts = await discoverByProvider[provider](connection.accessToken);
      return {status: connection.status, accounts};
    },
    async complete(authority, provider, input) {
      if (typeof input?.account_id !== 'string') throw failure('INVALID_ACCOUNT', 400);
      const connection = await store.readPendingProvider({authority, provider});
      if (!connection || connection.status !== 'pending_account_selection' || !connection.accessToken) throw failure('INVALID_ACCOUNT', 400);
      const accounts = await discoverByProvider[provider](connection.accessToken);
      const account = accounts.find(item => item.id === input.account_id);
      if (!account) throw failure('INVALID_ACCOUNT', 400);
      await store.completeAccountSelection({authority, provider, version: connection.connection_version, account});
      return {status: 'connected', active_account_id: account.id, account_name: account.name, currency: account.currency};
    },
  });
}

module.exports = Object.freeze({PROVIDERS, createMetaAccountDiscovery, createGoogleAdsAccountDiscovery, createAdAccountSelection});

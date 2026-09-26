'use strict';

const { discoverGoogleCustomers } = require('../providers/google/customer-discovery');

const PROVIDERS = Object.freeze(['meta', 'google_ads']);

function failure(code, status = 503) { return Object.assign(new Error(code), {code, status}); }
function googleFailure(stage, response, payload) {
  const error = failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
  error.providerStage = stage;
  error.upstreamStatus = Number.isInteger(response?.status) ? response.status : null;
  error.upstreamCode = typeof payload?.error?.status === 'string' ? payload.error.status.slice(0, 64) : null;
  const requestId = response?.headers?.get?.('request-id') || payload?.requestId || payload?.request_id;
  error.upstreamRequestId = typeof requestId === 'string' ? requestId.slice(0, 128) : null;
  return error;
}
function googleReauthorize(stage, response, payload) {
  const error = failure('PROVIDER_REAUTHORIZE', 409);
  const diagnostic = googleFailure(stage, response, payload);
  error.providerStage = diagnostic.providerStage;
  error.upstreamStatus = diagnostic.upstreamStatus;
  error.upstreamCode = diagnostic.upstreamCode;
  error.upstreamRequestId = diagnostic.upstreamRequestId;
  return error;
}
const ACCOUNT_LIMIT = 3;
function validAccount(item) {
  if (!item || typeof item.id !== 'string' || !item.id || typeof item.name !== 'string' || !item.name || !/^[A-Z]{3}$/.test(item.currency || '')) {
    throw failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
  }
  return Object.freeze({id: item.id.slice(0, 256), name: item.name.slice(0, 256), currency: item.currency});
}

function validGoogleAccount(item) {
  const account = validAccount(item);
  const loginCustomerId = String(item?.loginCustomerId || item?.login_customer_id || '');
  if (!/^\d+$/.test(loginCustomerId)) throw failure('PROVIDER_ACCOUNTS_UNAVAILABLE');
  return Object.freeze({...account, loginCustomerId});
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
    const body = await accessible.json().catch(() => { throw googleFailure('list_accessible_customers_response', accessible, null); });
    if (accessible.status === 401) throw googleReauthorize('list_accessible_customers', accessible, body);
    if (!accessible.ok) throw googleFailure('list_accessible_customers', accessible, body);
    const resourceNames = Array.isArray(body?.resourceNames) ? body.resourceNames : [];
    let discovered;
    try { discovered = await discoverGoogleCustomers({resourceNames, search: async ({customerId, query}) => {
      const response = await fetchImpl(`https://googleads.googleapis.com/${apiVersion}/customers/${customerId}/googleAds:searchStream`, {
        method: 'POST', headers: headers(accessToken), body: JSON.stringify({query}), signal: AbortSignal.timeout(15000), redirect: 'error',
      });
      const payload = await response.json().catch(() => { throw googleFailure('customer_client_response', response, null); });
      if (response.status === 401) throw googleReauthorize('customer_client_search', response, payload);
      if (!response.ok) throw googleFailure('customer_client_search', response, payload);
      return {results: (Array.isArray(payload) ? payload : []).flatMap(chunk => Array.isArray(chunk?.results) ? chunk.results : [])};
    }}); } catch (error) {
      if (!error.providerStage) error.providerStage = 'customer_client_transform';
      throw error;
    }
    try {
      return discovered.customers.map(item => validGoogleAccount({
        id: item.customerId,
        name: item.account_name,
        currency: item.currency,
        loginCustomerId: item.loginCustomerId,
      }));
    } catch (error) {
      if (!error.providerStage) error.providerStage = 'verified_account_shape';
      throw error;
    }
  };
}

function createAdAccountSelection({store, discoverByProvider, tokenLifecycleByProvider = {}} = {}) {
  async function discover(authority, provider, connection) {
    const lifecycle = tokenLifecycleByProvider[provider];
    try {
      if (!lifecycle) return {accounts: await discoverByProvider[provider](connection.accessToken), connection};
      const result = await lifecycle.run({
        authority,
        connection,
        operation: current => discoverByProvider[provider](current.accessToken),
      });
      return {accounts: result.value, connection: result.connection};
    } catch (error) {
      if (provider === 'google_ads' && !error.providerStage) {
        const code = String(error?.code || error?.message || '');
        error.providerStage = /^GOOGLE_(?:TOKEN|REAUTHORIZE|ACCESS_TOKEN)/.test(code) ? 'token_lifecycle' : 'account_discovery_internal';
      }
      throw error;
    }
  }
  return Object.freeze({
    async status(authority, provider) {
      const row = await store.readStatus({authority, provider});
      return {status: row?.status || 'not_connected', accounts: Array.isArray(row?.selected_accounts) ? row.selected_accounts : []};
    },
    async list(authority, provider) {
      if (!PROVIDERS.includes(provider)) throw failure('PROVIDER_NOT_FOUND', 404);
      const connection = await store.readPendingProvider({authority, provider});
      if (!connection) return {status: 'not_connected', accounts: []};
      if (connection.status !== 'pending_account_selection' || !connection.accessToken) return {status: connection.status, accounts: []};
      const {accounts} = await discover(authority, provider, connection);
      return {status: connection.status, accounts};
    },
    async complete(authority, provider, input) {
      if (!Array.isArray(input?.account_ids)) throw failure('INVALID_ACCOUNT', 400);
      if (input.account_ids.length < 1 || input.account_ids.length > ACCOUNT_LIMIT) throw failure('ACCOUNT_SELECTION_LIMIT', 400);
      if (input.account_ids.some(value => typeof value !== 'string' || !value)) throw failure('INVALID_ACCOUNT', 400);
      const accountIds = [...new Set(input.account_ids)];
      if (accountIds.length !== input.account_ids.length) throw failure('INVALID_ACCOUNT', 400);
      const connection = await store.readPendingProvider({authority, provider});
      if (!connection || connection.status !== 'pending_account_selection' || !connection.accessToken) throw failure('INVALID_ACCOUNT', 400);
      const discovered = await discover(authority, provider, connection);
      const accounts = discovered.accounts;
      const verified = accountIds.map(id => accounts.find(item => item.id === id));
      if (verified.some(account => !account)) throw failure('INVALID_ACCOUNT', 400);
      const version = discovered.connection.version ?? discovered.connection.connection_version;
      await store.completeAccountSelection({authority, provider, version, accounts: verified});
      return {status: 'connected', accounts: verified};
    },
  });
}

module.exports = Object.freeze({PROVIDERS, ACCOUNT_LIMIT, createMetaAccountDiscovery, createGoogleAdsAccountDiscovery, createAdAccountSelection, validGoogleAccount});


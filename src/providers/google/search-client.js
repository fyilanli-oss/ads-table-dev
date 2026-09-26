'use strict';

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}
function failure(code, status, response, payload) {
  const error = Object.assign(new Error(code), {code, status});
  error.providerStage = 'google_ads_search';
  error.upstreamStatus = Number.isInteger(response?.status) ? response.status : null;
  error.upstreamCode = typeof payload?.error?.status === 'string' ? payload.error.status.slice(0, 64) : null;
  const requestId = response?.headers?.get?.('request-id') || payload?.requestId || payload?.request_id;
  error.upstreamRequestId = typeof requestId === 'string' ? requestId.slice(0, 128) : null;
  return error;
}
function createGoogleAdsSearchClient({fetchImpl = fetch, developerToken, apiVersion = 'v25'} = {}) {
  const token = required(developerToken, 'Google Ads developer token');
  if (typeof fetchImpl !== 'function') throw new TypeError('fetchImpl is required');
  return async function search({accessToken, customerId, loginCustomerId, query} = {}) {
    const customer = required(customerId, 'customerId');
    const login = required(loginCustomerId, 'loginCustomerId');
    if (!/^\d+$/.test(customer) || !/^\d+$/.test(login)) throw new Error('GOOGLE_CUSTOMER_CONTEXT_INVALID');
    const response = await fetchImpl(`https://googleads.googleapis.com/${apiVersion}/customers/${customer}/googleAds:searchStream`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${required(accessToken, 'accessToken')}`,
        'developer-token': token,
        'login-customer-id': login,
        'content-type': 'application/json',
      },
      body: JSON.stringify({query: required(query, 'query')}),
      signal: AbortSignal.timeout(20000),
      redirect: 'error',
    });
    const payload = await response.json().catch(() => { throw failure('GOOGLE_PREFLIGHT_RESPONSE_INVALID', 503, response, null); });
    if (response.status === 401) throw failure('GOOGLE_ACCESS_TOKEN_INVALID', 409, response, payload);
    if (!response.ok) throw failure('GOOGLE_PREFLIGHT_PROVIDER_FAILED', 503, response, payload);
    const chunks = Array.isArray(payload) ? payload : [];
    return Object.freeze({results: chunks.flatMap(chunk => Array.isArray(chunk?.results) ? chunk.results : [])});
  };
}

module.exports = Object.freeze({createGoogleAdsSearchClient});


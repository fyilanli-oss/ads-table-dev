'use strict';

const ACCOUNT_FIELDS = Object.freeze(['id', 'name', 'account_status', 'currency', 'timezone_name']);
const AD_INSIGHT_FIELDS = Object.freeze([
  'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
  'account_currency', 'date_start', 'date_stop', 'impressions', 'reach', 'clicks',
  'ctr', 'cpc', 'spend', 'actions', 'action_values', 'cost_per_action_type',
  'conversion_rate_ranking'
]);

function required(value, name) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${name} is required`);
  return value.trim();
}

function metaError(message, safeCode) { const error = new Error(message); error.safeCode = safeCode; return error; }

function createMetaClient({ accessToken, graphVersion = 'v23.0', transport = globalThis.fetch, baseUrl = 'https://graph.facebook.com' } = {}) {
  const token = required(accessToken, 'accessToken');
  if (typeof transport !== 'function') throw new TypeError('transport is required');
  async function get(pathname, params = {}) {
    const url = new URL(`${baseUrl}/${graphVersion}/${pathname.replace(/^\//, '')}`);
    for (const [key, value] of Object.entries(params)) if (value !== null && value !== undefined && value !== '') url.searchParams.set(key, String(value));
    let response;
    try { response = await transport(url, { headers: { authorization: `Bearer ${token}`, accept: 'application/json' } }); }
    catch { throw metaError('Meta API transport failed', 'META_TRANSPORT_FAILED'); }
    let body;
    try { body = await response.json(); } catch { throw new Error('Meta API response was not JSON'); }
    if (!response.ok) {
      const code = response.status === 429 ? 'META_RATE_LIMITED' : response.status >= 500 ? 'META_SERVICE_UNAVAILABLE' : response.status === 401 || response.status === 403 ? 'META_AUTH_REJECTED' : 'META_REQUEST_REJECTED';
      throw metaError(`Meta API request failed (${response.status})`, code);
    }
    return body;
  }
  return Object.freeze({
    listAccounts: () => get('me/adaccounts', { fields: ACCOUNT_FIELDS.join(','), limit: 100 }),
    fetchAdInsights: ({ accountId, since, until, limit = 100 }) => get(`${required(accountId, 'accountId')}/insights`, {
      level: 'ad', time_range: JSON.stringify({ since: required(since, 'since'), until: required(until, 'until') }),
      time_increment: 1, fields: AD_INSIGHT_FIELDS.join(','), limit
    })
  });
}

module.exports = Object.freeze({ ACCOUNT_FIELDS, AD_INSIGHT_FIELDS, createMetaClient, metaError });

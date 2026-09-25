'use strict';

const { businessDateFromTimestamp, validateTimeZone } = require('../../../funnel-core/time-service');
const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');

const API_BASE = 'https://a.klaviyo.com';
const REVISION = '2026-07-15';
const STATISTICS = Object.freeze([
  'delivered', 'clicks_unique', 'opens_unique', 'conversions', 'conversion_value', 'text_message_spend'
]);
const MAX_RATE_LIMIT_RETRIES = 2;
const MAX_RATE_LIMIT_WAIT_MS = 10000;
const RATE_LIMIT_JITTER_MS = 250;

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function finite(value, field) {
  if (value === null || value === undefined) return null;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) throw new Error(`${field} must be a non-negative number`);
  return number;
}

function zonedInstant(date, timeZone, end = false) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('providerDate must be YYYY-MM-DD');
  validateTimeZone(timeZone);
  const [year, month, day] = date.split('-').map(Number);
  const desired = Date.UTC(year, month - 1, day + (end ? 1 : 0), 0, 0, 0);
  let instant = desired;
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'
  });
  for (let index = 0; index < 3; index += 1) {
    const parts = Object.fromEntries(formatter.formatToParts(new Date(instant))
      .filter(part => part.type !== 'literal').map(part => [part.type, Number(part.value)]));
    const represented = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
    instant += desired - represented;
  }
  if (end) instant -= 1000;
  return new Date(instant).toISOString();
}

function monthStart(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('providerDate must be YYYY-MM-DD');
  return `${date.slice(0, 7)}-01`;
}

function resultKey(branch, groupings = {}) {
  const root = branch === 'campaign' ? groupings.campaign_id : groupings.flow_id;
  const message = branch === 'campaign' ? groupings.campaign_message_id : groupings.flow_message_id;
  return `${branch}:${required(root, `${branch}.root_id`)}:${required(message, `${branch}.message_id`)}:${required(groupings.send_channel, `${branch}.send_channel`)}`;
}

function reportRows(payload, branch) {
  const rows = payload?.data?.attributes?.results;
  if (!Array.isArray(rows)) throw new Error('KLAVIYO_REPORT_RESPONSE_INVALID');
  return rows.map(item => {
    if (!item || typeof item !== 'object') throw new Error('KLAVIYO_REPORT_RESPONSE_INVALID');
    const groupings = item.groupings || {};
    const statistics = item.statistics || {};
    const channel = required(groupings.send_channel, 'send_channel').toLowerCase();
    if (!['email', 'sms'].includes(channel)) throw new Error('KLAVIYO_REPORT_CHANNEL_UNSUPPORTED');
    const rootId = branch === 'campaign' ? groupings.campaign_id : groupings.flow_id;
    const messageId = branch === 'campaign' ? groupings.campaign_message_id : groupings.flow_message_id;
    const rootName = branch === 'flow' ? (groupings.flow_name || rootId) : rootId;
    const messageName = branch === 'campaign'
      ? (groupings.campaign_message_name || messageId)
      : (groupings.flow_message_name || messageId);
    return Object.freeze({
      key: resultKey(branch, groupings), branch, channel,
      root: { id: required(rootId, 'root.id'), name: required(rootName, 'root.name') },
      message: { id: required(messageId, 'message.id'), name: required(messageName, 'message.name') },
      statistics,
    });
  });
}

function metricPagePath(value) {
  const url = new URL(value, API_BASE);
  if (url.origin !== API_BASE || !['/api/metrics', '/api/metrics/'].includes(url.pathname)) {
    throw new Error('KLAVIYO_METRIC_PAGINATION_INVALID');
  }
  return `${url.pathname}${url.search}`;
}

function metricCandidate(item) {
  const name = required(item?.attributes?.name, 'metric.name');
  if (name.trim().toLowerCase() !== 'placed order') return null;
  const integration = item?.attributes?.integration;
  return Object.freeze({
    id: required(item?.id, 'metric.id'),
    name,
    integration_name: required(integration?.name, 'metric.integration.name'),
    integration_category: typeof integration?.category === 'string' && integration.category.trim()
      ? integration.category.trim() : null,
  });
}

function createKlaviyoProviderClient({
  fetchImpl = fetch,
  conversionMetricId = null,
  now = () => new Date(),
  sleepImpl = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)),
  random = Math.random,
} = {}) {
  const defaultMetricId = typeof conversionMetricId === 'string' && conversionMetricId.trim()
    ? conversionMetricId.trim() : null;

  async function request(accessToken, path, options = {}) {
    for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt += 1) {
      const response = await fetchImpl(`${API_BASE}${path}`, {
        ...options,
        redirect: 'error', signal: AbortSignal.timeout(20000),
        headers: {
          Authorization: `Bearer ${required(accessToken, 'accessToken')}`,
          accept: 'application/vnd.api+json', revision: REVISION,
          ...(options.body ? { 'content-type': 'application/vnd.api+json' } : {}),
          ...(options.headers || {}),
        },
      });
      if (response.status === 401 || response.status === 403) throw new Error('KLAVIYO_REAUTHORIZE');
      if (response.ok) {
        try { return await response.json(); }
        catch { throw new Error('KLAVIYO_PROVIDER_RESPONSE_INVALID'); }
      }

      console.warn('[klaviyo] provider request failed', {
        method: options.method || 'GET',
        path: new URL(path, API_BASE).pathname,
        status: response.status,
      });
      if (response.status !== 429 || attempt === MAX_RATE_LIMIT_RETRIES) {
        throw new Error('KLAVIYO_PROVIDER_UNAVAILABLE');
      }

      const retryAfter = response.headers?.get?.('retry-after');
      if (typeof retryAfter !== 'string' || !/^\d+$/.test(retryAfter.trim())) {
        throw new Error('KLAVIYO_PROVIDER_UNAVAILABLE');
      }
      const providerDelay = Number(retryAfter.trim()) * 1000;
      const delay = (providerDelay * (2 ** attempt)) + Math.floor(random() * RATE_LIMIT_JITTER_MS);
      if (!Number.isSafeInteger(delay) || delay < providerDelay || delay > MAX_RATE_LIMIT_WAIT_MS) {
        throw new Error('KLAVIYO_PROVIDER_UNAVAILABLE');
      }
      await sleepImpl(delay);
    }
    throw new Error('KLAVIYO_PROVIDER_UNAVAILABLE');
  }

  async function fetchAccount({ accessToken, accountId } = {}) {
    const payload = await request(accessToken, '/api/accounts/?fields[account]=timezone,preferred_currency');
    if (!Array.isArray(payload?.data) || payload.data.length !== 1) throw new Error('KLAVIYO_ACCOUNT_RESPONSE_INVALID');
    const item = payload.data[0];
    if (required(item?.id, 'provider_account.id') !== required(accountId, 'accountId')) throw new Error('KLAVIYO_PROVIDER_ACCOUNT_MISMATCH');
    return Object.freeze({
      id: item.id,
      currency: normalizeCurrencyCode(item?.attributes?.preferred_currency, 'provider_account.currency'),
      timezone: validateTimeZone(item?.attributes?.timezone),
    });
  }

  async function fetchPlacedOrderMetricCandidates({ accessToken } = {}) {
    let path = '/api/metrics/?fields[metric]=name,integration';
    const candidates = new Map();
    for (let page = 0; page < 100 && path; page += 1) {
      const payload = await request(accessToken, path);
      if (!Array.isArray(payload?.data)) throw new Error('KLAVIYO_METRIC_RESPONSE_INVALID');
      for (const item of payload.data) {
        const candidate = metricCandidate(item);
        if (candidate) candidates.set(candidate.id, candidate);
      }
      const next = payload?.links?.next;
      path = next ? metricPagePath(next) : null;
      if (page === 99 && path) throw new Error('KLAVIYO_METRIC_PAGINATION_LIMIT');
    }
    return Object.freeze([...candidates.values()].sort((left, right) => left.id.localeCompare(right.id)));
  }

  async function report(accessToken, branch, timeframe, conversionMetricIdInput) {
    const metricId = required(conversionMetricIdInput || defaultMetricId, 'Klaviyo conversion metric id');
    const type = `${branch}-values-report`;
    const groupBy = branch === 'campaign'
      ? ['campaign_message_id', 'campaign_message_name', 'campaign_id', 'send_channel']
      : ['flow_message_id', 'flow_message_name', 'flow_id', 'flow_name', 'send_channel'];
    const payload = await request(accessToken, `/api/${type}s/`, {
      method: 'POST',
      body: JSON.stringify({ data: { type, attributes: {
        timeframe, conversion_metric_id: metricId, statistics: STATISTICS, group_by: groupBy,
      } } }),
    });
    return reportRows(payload, branch);
  }

  async function fetchMessageFacts({ accessToken, account, providerDate, conversionMetricId: conversionMetricIdInput } = {}) {
    validateTimeZone(account?.timezone);
    const dailyWindow = { start: zonedInstant(providerDate, account.timezone), end: zonedInstant(providerDate, account.timezone, true) };
    const monthlyWindow = { start: zonedInstant(monthStart(providerDate), account.timezone), end: dailyWindow.end };
    const dailyCampaign = await report(accessToken, 'campaign', dailyWindow, conversionMetricIdInput);
    const dailyFlow = await report(accessToken, 'flow', dailyWindow, conversionMetricIdInput);
    const monthlyCampaign = await report(accessToken, 'campaign', monthlyWindow, conversionMetricIdInput);
    const monthlyFlow = await report(accessToken, 'flow', monthlyWindow, conversionMetricIdInput);
    const monthly = new Map([...monthlyCampaign, ...monthlyFlow].map(row => [row.key, row]));
    const periodClosed = providerDate.slice(0, 7) < businessDateFromTimestamp(now(), account.timezone).slice(0, 7);
    const rows = [...dailyCampaign, ...dailyFlow].map(row => {
      const month = monthly.get(row.key);
      const delivered = finite(row.statistics.delivered, 'statistics.delivered');
      const monthlyDelivered = finite(month?.statistics?.delivered, 'monthly.statistics.delivered');
      return Object.freeze({
        branch: row.branch, channel: row.channel, root: row.root, message: row.message,
        metrics: {
          delivered,
          unique_clicks: finite(row.statistics.clicks_unique, 'statistics.clicks_unique'),
          unique_opens: finite(row.statistics.opens_unique, 'statistics.opens_unique'),
          provider_spend: row.channel === 'sms' && normalizeCurrencyCode(account.currency, 'account.currency') === 'USD'
            ? finite(row.statistics.text_message_spend, 'statistics.text_message_spend') : null,
          add_to_cart: null, add_to_cart_value: null, checkout: null, checkout_value: null,
          purchase: finite(row.statistics.conversions, 'statistics.conversions'),
          purchase_value: finite(row.statistics.conversion_value, 'statistics.conversion_value'),
        },
        metric_support: { add_to_cart: 'unknown', add_to_cart_value: 'unknown', checkout: 'unknown', checkout_value: 'unknown' },
        spend_allocation: { dailySentCount: delivered, monthlySentCount: monthlyDelivered, periodClosed },
      });
    });
    return Object.freeze({ rows, verified_empty: rows.length === 0 });
  }

  return Object.freeze({ fetchAccount, fetchPlacedOrderMetricCandidates, fetchMessageFacts });
}

module.exports = Object.freeze({ API_BASE, REVISION, STATISTICS, zonedInstant, reportRows, metricPagePath, metricCandidate, createKlaviyoProviderClient });
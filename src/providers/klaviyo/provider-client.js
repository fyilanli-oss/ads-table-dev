'use strict';

const { businessDateFromTimestamp, validateTimeZone } = require('../../../funnel-core/time-service');
const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');

const API_BASE = 'https://a.klaviyo.com';
const REVISION = '2026-07-15';
const STATISTICS = Object.freeze([
  'recipients', 'delivered', 'clicks_unique', 'opens_unique', 'conversions', 'conversion_value', 'text_message_spend'
]);
const JOURNEY_METRIC_NAMES = Object.freeze({
  addToCart: Object.freeze(['added to cart']),
  checkout: Object.freeze(['checkout started', 'started checkout']),
  purchase: Object.freeze(['placed order']),
});
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

function diagnosticReport(rows, baseKeys = null) {
  const byBranch = branch => {
    const selected = rows.filter(row => row.branch === branch);
    const total = field => selected.reduce((sum, row) => {
      const value = finite(row.statistics?.[field], `diagnostic.${branch}.${field}`);
      return value === null ? sum : sum + value;
    }, 0);
    return Object.freeze({
      row_count: selected.length,
      conversion_count: total('conversions'),
      conversion_value: total('conversion_value'),
      matched_key_count: baseKeys ? selected.filter(row => baseKeys.has(row.key)).length : selected.length,
      unmatched_key_count: baseKeys ? selected.filter(row => !baseKeys.has(row.key)).length : 0,
    });
  };
  return Object.freeze({ campaign: byBranch('campaign'), flow: byBranch('flow') });
}

function metricPagePath(value) {
  const url = new URL(value, API_BASE);
  if (url.origin !== API_BASE || !['/api/metrics', '/api/metrics/'].includes(url.pathname)) {
    throw new Error('KLAVIYO_METRIC_PAGINATION_INVALID');
  }
  return `${url.pathname}${url.search}`;
}

function journeyMetricCandidate(item) {
  const name = required(item?.attributes?.name, 'metric.name');
  const normalizedName = name.trim().toLowerCase();
  const kind = Object.entries(JOURNEY_METRIC_NAMES)
    .find(([, names]) => names.includes(normalizedName))?.[0];
  if (!kind) return null;
  const integration = item?.attributes?.integration;
  return Object.freeze({
    kind,
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
  reportSpacingMs = 0,
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
      if (response.status === 401) throw codedError('KLAVIYO_ACCESS_TOKEN_INVALID', 401);
      if (response.status === 403) throw codedError('KLAVIYO_ACCESS_FORBIDDEN', 403);
      if (response.ok) {
        try { return await response.json(); }
        catch { throw new Error('KLAVIYO_PROVIDER_RESPONSE_INVALID'); }
      }

      console.warn('[klaviyo] provider request failed', {
        method: options.method || 'GET',
        path: new URL(path, API_BASE).pathname,
        status: response.status,
      });
      if (response.status !== 429) {
        throw new Error('KLAVIYO_PROVIDER_UNAVAILABLE');
      }
      if (attempt === MAX_RATE_LIMIT_RETRIES) throw codedError('KLAVIYO_PROVIDER_RATE_LIMITED', 503);

      const retryAfter = response.headers?.get?.('retry-after');
      if (typeof retryAfter !== 'string' || !/^\d+$/.test(retryAfter.trim())) {
        throw codedError('KLAVIYO_PROVIDER_RATE_LIMITED', 503);
      }
      const providerDelay = Number(retryAfter.trim()) * 1000;
      const delay = (providerDelay * (2 ** attempt)) + Math.floor(random() * RATE_LIMIT_JITTER_MS);
      if (!Number.isSafeInteger(delay) || delay < providerDelay || delay > MAX_RATE_LIMIT_WAIT_MS) {
        throw codedError('KLAVIYO_PROVIDER_RATE_LIMITED', 503);
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

  if (!Number.isInteger(reportSpacingMs) || reportSpacingMs < 0) {
    throw new TypeError('reportSpacingMs must be a non-negative integer');
  }

  async function fetchJourneyMetricCandidates({ accessToken } = {}) {
    let path = '/api/metrics/?fields[metric]=name,integration';
    const candidates = Object.fromEntries(Object.keys(JOURNEY_METRIC_NAMES).map(kind => [kind, new Map()]));
    for (let page = 0; page < 100 && path; page += 1) {
      const payload = await request(accessToken, path);
      if (!Array.isArray(payload?.data)) throw new Error('KLAVIYO_METRIC_RESPONSE_INVALID');
      for (const item of payload.data) {
        const candidate = journeyMetricCandidate(item);
        if (candidate) candidates[candidate.kind].set(candidate.id, candidate);
      }
      const next = payload?.links?.next;
      path = next ? metricPagePath(next) : null;
      if (page === 99 && path) throw new Error('KLAVIYO_METRIC_PAGINATION_LIMIT');
    }
    return Object.freeze(Object.fromEntries(Object.entries(candidates).map(([kind, items]) => [kind,
      Object.freeze([...items.values()]
        .sort((left, right) => left.id.localeCompare(right.id))
        .map(({ kind: ignored, ...candidate }) => Object.freeze(candidate)))
    ])));
  }

  async function fetchPlacedOrderMetricCandidates({ accessToken } = {}) {
    return (await fetchJourneyMetricCandidates({ accessToken })).purchase;
  }

  async function report(accessToken, branch, timeframe, conversionMetricIdInput, statistics = STATISTICS) {
    const metricId = required(conversionMetricIdInput || defaultMetricId, 'Klaviyo conversion metric id');
    const type = `${branch}-values-report`;
    const groupBy = branch === 'campaign'
      ? ['campaign_message_id', 'campaign_message_name', 'campaign_id', 'send_channel']
      : ['flow_message_id', 'flow_message_name', 'flow_id', 'flow_name', 'send_channel'];
    const payload = await request(accessToken, `/api/${type}s/`, {
      method: 'POST',
      body: JSON.stringify({ data: { type, attributes: {
        timeframe, conversion_metric_id: metricId, statistics, group_by: groupBy,
      } } }),
    });
    return reportRows(payload, branch);
  }

  async function fetchMessageFacts({ accessToken, account, providerDate, conversionMetricId: conversionMetricIdInput, journeyMetricIds = null, includeDiagnostics = false } = {}) {
    validateTimeZone(account?.timezone);
    const metricIds = Object.freeze({
      addToCart: typeof journeyMetricIds?.addToCart === 'string' && journeyMetricIds.addToCart.trim() ? journeyMetricIds.addToCart.trim() : null,
      checkout: typeof journeyMetricIds?.checkout === 'string' && journeyMetricIds.checkout.trim() ? journeyMetricIds.checkout.trim() : null,
      purchase: required(journeyMetricIds?.purchase || conversionMetricIdInput || defaultMetricId, 'Klaviyo purchase metric id'),
    });
    const dailyWindow = { start: zonedInstant(providerDate, account.timezone), end: zonedInstant(providerDate, account.timezone, true) };
    const monthlyWindow = { start: zonedInstant(monthStart(providerDate), account.timezone), end: dailyWindow.end };
    let reportCount = 0;
    const scheduledReport = async (branch, timeframe, metricId, statistics = STATISTICS) => {
      if (reportCount > 0 && reportSpacingMs > 0) await sleepImpl(reportSpacingMs);
      reportCount += 1;
      return report(accessToken, branch, timeframe, metricId, statistics);
    };
    const dailyCampaign = await scheduledReport('campaign', dailyWindow, metricIds.purchase);
    const dailyFlow = await scheduledReport('flow', dailyWindow, metricIds.purchase);
    const monthlyCampaign = await scheduledReport('campaign', monthlyWindow, metricIds.purchase);
    const monthlyFlow = await scheduledReport('flow', monthlyWindow, metricIds.purchase);
    const stageRows = {};
    for (const stage of ['addToCart', 'checkout']) {
      if (!metricIds[stage]) continue;
      const campaign = await scheduledReport('campaign', dailyWindow, metricIds[stage], ['conversions', 'conversion_value']);
      const flow = await scheduledReport('flow', dailyWindow, metricIds[stage], ['conversions', 'conversion_value']);
      stageRows[stage] = new Map([...campaign, ...flow].map(row => [row.key, row]));
    }
    const baseRows = [...dailyCampaign, ...dailyFlow];
    const baseKeys = new Set(baseRows.map(row => row.key));
    const journeyDrift = Object.values(stageRows)
      .some(rows => [...rows.keys()].some(key => !baseKeys.has(key)));
    if (journeyDrift && !includeDiagnostics) {
      throw new Error('KLAVIYO_JOURNEY_METRIC_KEY_DRIFT');
    }
    const diagnostics = includeDiagnostics ? Object.freeze({
      provider_date: providerDate,
      purchase: diagnosticReport(baseRows),
      add_to_cart: diagnosticReport(stageRows.addToCart ? [...stageRows.addToCart.values()] : [], baseKeys),
      checkout: diagnosticReport(stageRows.checkout ? [...stageRows.checkout.values()] : [], baseKeys),
      journey_key_drift: journeyDrift,
      dataset_v2_write: false,
    }) : null;
    const monthlyEmailRows = [...monthlyCampaign, ...monthlyFlow].filter(row => row.channel === 'email');
    const monthlyEmailRecipients = monthlyEmailRows.length === 0 ? 0 : monthlyEmailRows.reduce((total, row) => {
      const recipients = finite(row.statistics.recipients, 'monthly.statistics.recipients');
      return total === null || recipients === null ? null : total + recipients;
    }, 0);
    const periodClosed = providerDate.slice(0, 7) < businessDateFromTimestamp(now(), account.timezone).slice(0, 7);
    const rows = baseRows.map(row => {
      const delivered = finite(row.statistics.delivered, 'statistics.delivered');
      const recipients = finite(row.statistics.recipients, 'statistics.recipients');
      const addToCart = stageRows.addToCart?.get(row.key);
      const checkout = stageRows.checkout?.get(row.key);
      return Object.freeze({
        branch: row.branch, channel: row.channel, root: row.root, message: row.message,
        metrics: {
          delivered,
          unique_clicks: finite(row.statistics.clicks_unique, 'statistics.clicks_unique'),
          unique_opens: finite(row.statistics.opens_unique, 'statistics.opens_unique'),
          provider_spend: row.channel === 'sms' && normalizeCurrencyCode(account.currency, 'account.currency') === 'USD'
            ? finite(row.statistics.text_message_spend, 'statistics.text_message_spend') : null,
          add_to_cart: addToCart ? finite(addToCart.statistics.conversions, 'add_to_cart.conversions') : null,
          add_to_cart_value: addToCart ? finite(addToCart.statistics.conversion_value, 'add_to_cart.conversion_value') : null,
          checkout: checkout ? finite(checkout.statistics.conversions, 'checkout.conversions') : null,
          checkout_value: checkout ? finite(checkout.statistics.conversion_value, 'checkout.conversion_value') : null,
          purchase: finite(row.statistics.conversions, 'statistics.conversions'),
          purchase_value: finite(row.statistics.conversion_value, 'statistics.conversion_value'),
        },
        metric_support: {
          add_to_cart: addToCart ? 'supported' : 'unknown',
          add_to_cart_value: addToCart ? 'supported' : 'unknown',
          checkout: checkout ? 'supported' : 'unknown',
          checkout_value: checkout ? 'supported' : 'unknown',
        },
        spend_allocation: {
          dailySentCount: recipients,
          monthlySentCount: row.channel === 'email' ? monthlyEmailRecipients : null,
          periodClosed,
        },
      });
    });
    return Object.freeze({
      rows,
      verified_empty: rows.length === 0,
      ...(diagnostics ? { diagnostics } : {}),
    });
  }

  return Object.freeze({ fetchAccount, fetchJourneyMetricCandidates, fetchPlacedOrderMetricCandidates, fetchMessageFacts });
}

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

module.exports = Object.freeze({ API_BASE, REVISION, STATISTICS, JOURNEY_METRIC_NAMES, zonedInstant, reportRows, diagnosticReport, metricPagePath, journeyMetricCandidate, createKlaviyoProviderClient });

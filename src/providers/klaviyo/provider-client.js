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
const MAX_DIAGNOSTIC_EVENT_PAGES = 20;
const EVENT_CATEGORY_BY_NAME = Object.freeze({
  'Received Email': 'received_email',
  'Opened Email': 'opened_email',
  'Clicked Email': 'clicked_email',
  'Bounced Email': 'bounced_email',
  'Dropped Email': 'dropped_email',
  'Marked Email as Spam': 'marked_email_as_spam',
  'Unsubscribed': 'unsubscribed',
  'Consented to Receive SMS': 'consented_to_receive_sms',
  'Received SMS': 'received_sms',
  'Sent SMS': 'sent_sms',
  'Clicked SMS': 'clicked_sms',
  'Failed to Deliver SMS': 'failed_to_deliver_sms',
  'Received Automated Response SMS': 'received_automated_response_sms',
  'Failed to Deliver Automated Response SMS': 'failed_to_deliver_automated_response_sms',
  'Active on Site': 'active_on_site',
  'Viewed Product': 'viewed_product',
  'Added to Cart': 'added_to_cart',
  'Started Checkout': 'started_checkout',
  'Placed Order': 'placed_order',
  'Ordered Product': 'ordered_product',
  'Fulfilled Order': 'fulfilled_order',
  'Cancelled Order': 'cancelled_order',
  'Refunded Order': 'refunded_order',
});

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

function campaignPagePath(value) {
  const url = new URL(value, API_BASE);
  if (url.origin !== API_BASE || !['/api/campaigns', '/api/campaigns/'].includes(url.pathname)) {
    throw new Error('KLAVIYO_CAMPAIGN_PAGINATION_INVALID');
  }
  return `${url.pathname}${url.search}`;
}

function flowPagePath(value) {
  const url = new URL(value, API_BASE);
  if (url.origin !== API_BASE || !['/api/flows', '/api/flows/'].includes(url.pathname)) {
    throw new Error('KLAVIYO_FLOW_PAGINATION_INVALID');
  }
  return `${url.pathname}${url.search}`;
}

function eventPagePath(value) {
  const url = new URL(value, API_BASE);
  if (url.origin !== API_BASE || !['/api/events', '/api/events/'].includes(url.pathname)) {
    throw new Error('KLAVIYO_EVENT_PAGINATION_INVALID');
  }
  return `${url.pathname}${url.search}`;
}

function eventBusinessDate(value, timeZone) {
  const instant = new Date(required(value, 'event.datetime'));
  if (Number.isNaN(instant.getTime())) throw new Error('KLAVIYO_EVENT_DATETIME_INVALID');
  return businessDateFromTimestamp(instant, validateTimeZone(timeZone));
}

function campaignBusinessDate(item, timeZone) {
  const scheduledAt = item?.attributes?.scheduled_at;
  if (scheduledAt === null || scheduledAt === undefined || scheduledAt === '') return null;
  const instant = new Date(scheduledAt);
  if (Number.isNaN(instant.getTime())) throw new Error('KLAVIYO_CAMPAIGN_SCHEDULE_INVALID');
  return businessDateFromTimestamp(instant, validateTimeZone(timeZone));
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

  async function fetchCampaignInventory({ accessToken, timeZone } = {}) {
    validateTimeZone(timeZone);
    const dates = new Set();
    let totalCampaignCount = 0;
    let sentCampaignCount = 0;
    let sentWithScheduledAtCount = 0;
    let sentWithoutScheduledAtCount = 0;
    for (const channel of ['email', 'sms']) {
      const params = new URLSearchParams({
        filter: `equals(messages.channel,'${channel}')`,
        'fields[campaign]': 'status,scheduled_at',
        'page[size]': '100',
      });
      let path = `/api/campaigns/?${params.toString()}`;
      for (let page = 0; page < 100 && path; page += 1) {
        const payload = await request(accessToken, path);
        if (!Array.isArray(payload?.data)) throw new Error('KLAVIYO_CAMPAIGN_RESPONSE_INVALID');
        for (const item of payload.data) {
          totalCampaignCount += 1;
          const status = required(item?.attributes?.status, 'campaign.status');
          if (status !== 'Sent') continue;
          sentCampaignCount += 1;
          const date = campaignBusinessDate(item, timeZone);
          if (date) {
            sentWithScheduledAtCount += 1;
            dates.add(date);
          } else {
            sentWithoutScheduledAtCount += 1;
          }
        }
        const next = payload?.links?.next;
        path = next ? campaignPagePath(next) : null;
        if (page === 99 && path) throw new Error('KLAVIYO_CAMPAIGN_PAGINATION_LIMIT');
      }
    }
    return Object.freeze({
      total_campaign_count: totalCampaignCount,
      sent_campaign_count: sentCampaignCount,
      sent_with_scheduled_at_count: sentWithScheduledAtCount,
      sent_without_scheduled_at_count: sentWithoutScheduledAtCount,
      sent_dates: Object.freeze([...dates].sort((left, right) => right.localeCompare(left))),
    });
  }

  async function fetchSentCampaignDates({ accessToken, timeZone } = {}) {
    validateTimeZone(timeZone);
    const dates = new Set();
    for (const channel of ['email', 'sms']) {
      const params = new URLSearchParams({
        filter: `and(equals(messages.channel,'${channel}'),equals(status,'Sent'))`,
        'fields[campaign]': 'scheduled_at',
        'page[size]': '100',
        sort: '-scheduled_at',
      });
      let path = `/api/campaigns/?${params.toString()}`;
      for (let page = 0; page < 100 && path; page += 1) {
        const payload = await request(accessToken, path);
        if (!Array.isArray(payload?.data)) throw new Error('KLAVIYO_CAMPAIGN_RESPONSE_INVALID');
        for (const item of payload.data) {
          const date = campaignBusinessDate(item, timeZone);
          if (date) dates.add(date);
        }
        const next = payload?.links?.next;
        path = next ? campaignPagePath(next) : null;
        if (page === 99 && path) throw new Error('KLAVIYO_CAMPAIGN_PAGINATION_LIMIT');
      }
    }
    return Object.freeze([...dates].sort((left, right) => right.localeCompare(left)));
  }

  async function fetchFlowEventInventory({ accessToken, timeZone } = {}) {
    validateTimeZone(timeZone);
    const flowStatusCounts = { live: 0, manual: 0, draft: 0, other: 0 };
    let flowCount = 0;
    let flowPath = '/api/flows/?fields[flow]=status&page[size]=50';
    for (let page = 0; page < 100 && flowPath; page += 1) {
      const payload = await request(accessToken, flowPath);
      if (!Array.isArray(payload?.data)) throw new Error('KLAVIYO_FLOW_RESPONSE_INVALID');
      for (const item of payload.data) {
        flowCount += 1;
        const status = required(item?.attributes?.status, 'flow.status').toLowerCase();
        if (Object.prototype.hasOwnProperty.call(flowStatusCounts, status) && status !== 'other') {
          flowStatusCounts[status] += 1;
        } else {
          flowStatusCounts.other += 1;
        }
      }
      const next = payload?.links?.next;
      flowPath = next ? flowPagePath(next) : null;
      if (page === 99 && flowPath) throw new Error('KLAVIYO_FLOW_PAGINATION_LIMIT');
    }

    const eventCounts = Object.fromEntries(Object.values(EVENT_CATEGORY_BY_NAME).map(key => [key, 0]));
    eventCounts.other = 0;
    let eventCount = 0;
    let attributedEventCount = 0;
    let earliestEventDate = null;
    let latestEventDate = null;
    let eventScanTruncated = false;
    let eventPath = '/api/events/?fields[event]=datetime&fields[metric]=name,integration&fields[attribution]=id&include=metric,attributions&page[size]=200&sort=datetime';
    for (let page = 0; page < MAX_DIAGNOSTIC_EVENT_PAGES && eventPath; page += 1) {
      const payload = await request(accessToken, eventPath);
      if (!Array.isArray(payload?.data) || !Array.isArray(payload?.included)) {
        throw new Error('KLAVIYO_EVENT_RESPONSE_INVALID');
      }
      const metrics = new Map(payload.included
        .filter(item => item?.type === 'metric')
        .map(item => [required(item?.id, 'metric.id'), required(item?.attributes?.name, 'metric.name')]));
      for (const item of payload.data) {
        eventCount += 1;
        const date = eventBusinessDate(item?.attributes?.datetime, timeZone);
        earliestEventDate = !earliestEventDate || date < earliestEventDate ? date : earliestEventDate;
        latestEventDate = !latestEventDate || date > latestEventDate ? date : latestEventDate;
        const metricId = required(item?.relationships?.metric?.data?.id, 'event.metric.id');
        const category = EVENT_CATEGORY_BY_NAME[metrics.get(metricId)] || 'other';
        eventCounts[category] += 1;
        const attributions = item?.relationships?.attributions?.data;
        if (Array.isArray(attributions) && attributions.length > 0) attributedEventCount += 1;
      }
      const next = payload?.links?.next;
      eventPath = next ? eventPagePath(next) : null;
      if (page === MAX_DIAGNOSTIC_EVENT_PAGES - 1 && eventPath) eventScanTruncated = true;
    }
    return Object.freeze({
      flow_count: flowCount,
      flow_status_counts: Object.freeze({ ...flowStatusCounts }),
      scanned_event_count: eventCount,
      event_counts: Object.freeze({ ...eventCounts }),
      attributed_event_count: attributedEventCount,
      earliest_event_date: earliestEventDate,
      latest_event_date: latestEventDate,
      event_scan_truncated: eventScanTruncated,
    });
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

  return Object.freeze({ fetchAccount, fetchPlacedOrderMetricCandidates, fetchCampaignInventory, fetchSentCampaignDates, fetchFlowEventInventory, fetchMessageFacts });
}

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

module.exports = Object.freeze({ API_BASE, REVISION, STATISTICS, EVENT_CATEGORY_BY_NAME, zonedInstant, reportRows, metricPagePath, campaignPagePath, flowPagePath, eventPagePath, campaignBusinessDate, eventBusinessDate, metricCandidate, createKlaviyoProviderClient });

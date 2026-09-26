'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { SPECS } = require('../src/shopify/embedded-provider-strategies');
const { createKlaviyoProviderClient, zonedInstant } = require('../src/providers/klaviyo/provider-client');

function response(payload, status = 200, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: name => headers[String(name).toLowerCase()] ?? null },
    json: async () => payload,
  };
}

test('Klaviyo OAuth requests the documented Flow reporting scope', () => {
  assert.match(SPECS.klaviyo.scope, /(?:^| )flows:read(?: |$)/);
});

test('Klaviyo reporting window follows the provider account timezone', () => {
  assert.equal(zonedInstant('2026-09-24', 'America/New_York'), '2026-09-24T04:00:00.000Z');
  assert.equal(zonedInstant('2026-09-24', 'America/New_York', true), '2026-09-25T03:59:59.000Z');
});

test('Klaviyo provider client verifies account and maps documented campaign/flow report fields', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (url.includes('/api/accounts/')) return response({ data: [{ id: 'account-1', attributes: { preferred_currency: 'USD', timezone: 'America/New_York' } }] });
    const body = JSON.parse(options.body);
    const branch = body.data.type.startsWith('campaign') ? 'campaign' : 'flow';
    const monthly = body.data.attributes.timeframe.start.startsWith('2026-09-01');
    const groupings = branch === 'campaign'
      ? { campaign_id: 'campaign-1', campaign_message_id: 'message-1', campaign_message_name: 'Message', send_channel: 'email' }
      : { flow_id: 'flow-1', flow_name: 'Flow', flow_message_id: 'message-2', flow_message_name: 'Flow message', send_channel: 'sms' };
    return response({ data: { attributes: { results: [{ groupings, statistics: {
      delivered: monthly ? 1000 : 100, clicks_unique: 10, opens_unique: 20,
      conversions: 2, conversion_value: 50, text_message_spend: branch === 'flow' ? 4 : null,
    } }] } } });
  };
  const client = createKlaviyoProviderClient({ fetchImpl, conversionMetricId: 'metric-1', now: () => new Date('2026-09-24T12:00:00Z') });
  const account = await client.fetchAccount({ accessToken: 'secret', accountId: 'account-1' });
  const result = await client.fetchMessageFacts({ accessToken: 'secret', account, providerDate: '2026-09-24' });
  assert.equal(result.verified_empty, false);
  assert.equal(result.rows.length, 2);
  assert.equal(result.rows[0].spend_allocation.monthlySentCount, 1000);
  assert.equal(result.rows[1].metrics.provider_spend, 4);
  assert.equal(calls.filter(call => call.url.includes('values-reports')).length, 4);
  assert.ok(calls.every(call => call.options.headers.Authorization === 'Bearer secret'));
});

test('Klaviyo provider client proves empty only after both documented report branches succeed', async () => {
  const fetchImpl = async (url) => url.includes('/api/accounts/')
    ? response({ data: [{ id: 'account-1', attributes: { preferred_currency: 'USD', timezone: 'UTC' } }] })
    : response({ data: { attributes: { results: [] } } });
  const client = createKlaviyoProviderClient({ fetchImpl, conversionMetricId: 'metric-1' });
  const account = await client.fetchAccount({ accessToken: 'secret', accountId: 'account-1' });
  const result = await client.fetchMessageFacts({ accessToken: 'secret', account, providerDate: '2026-09-24' });
  assert.deepEqual(result, { rows: [], verified_empty: true });
});

test('Klaviyo USD-only SMS spend is not mislabeled as a non-USD account currency', async () => {
  const fetchImpl = async (url, options) => {
    if (url.includes('/api/accounts/')) return response({ data: [{ id: 'account-1', attributes: { preferred_currency: 'EUR', timezone: 'UTC' } }] });
    const body = JSON.parse(options.body);
    const branch = body.data.type.startsWith('campaign') ? 'campaign' : 'flow';
    const groupings = branch === 'campaign'
      ? { campaign_id: 'campaign-1', campaign_message_id: 'message-1', send_channel: 'sms' }
      : { flow_id: 'flow-1', flow_message_id: 'message-2', send_channel: 'sms' };
    return response({ data: { attributes: { results: [{ groupings, statistics: {
      delivered: 1, clicks_unique: 0, opens_unique: 0, conversions: 0, conversion_value: 0, text_message_spend: 9,
    } }] } } });
  };
  const client = createKlaviyoProviderClient({ fetchImpl, conversionMetricId: 'metric-1' });
  const account = await client.fetchAccount({ accessToken: 'secret', accountId: 'account-1' });
  const result = await client.fetchMessageFacts({ accessToken: 'secret', account, providerDate: '2026-09-24' });
  assert.ok(result.rows.every(row => row.metrics.provider_spend === null));
});

test('Klaviyo provider client fails closed on auth, response or account drift without returning provider bodies', async () => {
  const unauthorized = createKlaviyoProviderClient({ fetchImpl: async () => response({ secret: 'raw' }, 401), conversionMetricId: 'metric-1' });
  await assert.rejects(unauthorized.fetchAccount({ accessToken: 'secret', accountId: 'account-1' }), error => error.code === 'KLAVIYO_ACCESS_TOKEN_INVALID' && error.status === 401);
  const forbidden = createKlaviyoProviderClient({ fetchImpl: async () => response({ secret: 'raw' }, 403), conversionMetricId: 'metric-1' });
  await assert.rejects(forbidden.fetchAccount({ accessToken: 'secret', accountId: 'account-1' }), error => error.code === 'KLAVIYO_ACCESS_FORBIDDEN' && error.status === 403);
  const drift = createKlaviyoProviderClient({ fetchImpl: async () => response({ data: [{ id: 'other', attributes: { preferred_currency: 'USD', timezone: 'UTC' } }] }), conversionMetricId: 'metric-1' });
  await assert.rejects(drift.fetchAccount({ accessToken: 'secret', accountId: 'account-1' }), /KLAVIYO_PROVIDER_ACCOUNT_MISMATCH/);
});

test('Klaviyo metric discovery paginates and returns only exact provider-reported Placed Order candidates', async () => {
  const calls = [];
  const fetchImpl = async url => {
    calls.push(url);
    if (calls.length === 1) return response({
      data: [
        { id: 'metric-other', attributes: { name: 'Ordered Product', integration: { name: 'Shopify', category: 'Ecommerce' } } },
        { id: 'metric-b', attributes: { name: 'Placed Order', integration: { name: 'WooCommerce', category: 'Ecommerce' } } },
      ],
      links: { next: 'https://a.klaviyo.com/api/metrics/?page[cursor]=next' },
    });
    return response({
      data: [{ id: 'metric-a', attributes: { name: 'Placed Order', integration: { name: 'Shopify', category: 'Ecommerce' } } }],
      links: { next: null },
    });
  };
  const client = createKlaviyoProviderClient({ fetchImpl });
  assert.deepEqual(await client.fetchPlacedOrderMetricCandidates({ accessToken: 'secret' }), [
    { id: 'metric-a', name: 'Placed Order', integration_name: 'Shopify', integration_category: 'Ecommerce' },
    { id: 'metric-b', name: 'Placed Order', integration_name: 'WooCommerce', integration_category: 'Ecommerce' },
  ]);
  assert.equal(calls.length, 2);
  assert.equal(calls[0], 'https://a.klaviyo.com/api/metrics/?fields[metric]=name,integration');
});

test('Klaviyo metric discovery rejects pagination outside the provider origin', async () => {
  const client = createKlaviyoProviderClient({ fetchImpl: async () => response({ data: [], links: { next: 'https://attacker.invalid/steal' } }) });
  await assert.rejects(client.fetchPlacedOrderMetricCandidates({ accessToken: 'secret' }), /KLAVIYO_METRIC_PAGINATION_INVALID/);
});
test('Klaviyo reporting serializes campaign and flow requests to avoid burst rate limits', async () => {
  let active = 0;
  let maxActive = 0;
  const fetchImpl = async () => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    await new Promise(resolve => setImmediate(resolve));
    active -= 1;
    return response({ data: { attributes: { results: [] } } });
  };
  const client = createKlaviyoProviderClient({ fetchImpl, conversionMetricId: 'metric-1' });
  const result = await client.fetchMessageFacts({
    accessToken: 'secret',
    account: { id: 'account-1', currency: 'USD', timezone: 'UTC' },
    providerDate: '2026-09-24',
  });
  assert.equal(maxActive, 1);
  assert.deepEqual(result, { rows: [], verified_empty: true });
});

test('Klaviyo provider client respects Retry-After before retrying a 429 response', async () => {
  let calls = 0;
  const waits = [];
  const fetchImpl = async () => {
    calls += 1;
    if (calls === 1) return response({}, 429, { 'retry-after': '1' });
    return response({ data: { attributes: { results: [] } } });
  };
  const client = createKlaviyoProviderClient({
    fetchImpl,
    conversionMetricId: 'metric-1',
    sleepImpl: async milliseconds => { waits.push(milliseconds); },
    random: () => 0,
  });
  const result = await client.fetchMessageFacts({
    accessToken: 'secret',
    account: { id: 'account-1', currency: 'USD', timezone: 'UTC' },
    providerDate: '2026-09-24',
  });
  assert.equal(calls, 5);
  assert.deepEqual(waits, [1000]);
  assert.deepEqual(result, { rows: [], verified_empty: true });
});

test('Klaviyo provider client returns a safe rate-limit error after the bounded retry is exhausted', async () => {
  let calls = 0;
  const client = createKlaviyoProviderClient({
    fetchImpl: async () => {
      calls += 1;
      return response({ provider_secret: 'must-not-leak' }, 429, { 'retry-after': '0' });
    },
    conversionMetricId: 'metric-1',
    sleepImpl: async () => {},
    random: () => 0,
  });
  await assert.rejects(client.fetchAccount({ accessToken: 'secret', accountId: 'account-1' }), error => {
    assert.equal(error.code, 'KLAVIYO_PROVIDER_RATE_LIMITED');
    assert.equal(error.status, 503);
    assert.doesNotMatch(`${error.code}:${error.message}`, /provider_secret|must-not-leak|account-1/i);
    return true;
  });
  assert.equal(calls, 3);
});


test('Klaviyo sent campaign inventory returns unique provider dates without campaign identifiers', async () => {
  const calls = [];
  const fetchImpl = async url => {
    calls.push(url);
    const decoded = decodeURIComponent(url);
    if (decoded.includes("'email'")) return response({
      data: [
        { id: 'campaign-secret-1', attributes: { scheduled_at: '2026-09-20T23:30:00Z' } },
        { id: 'campaign-secret-2', attributes: { scheduled_at: '2026-09-20T08:00:00Z' } },
      ],
      links: { next: null },
    });
    return response({
      data: [{ id: 'campaign-secret-3', attributes: { scheduled_at: '2026-09-19T10:00:00Z' } }],
      links: { next: null },
    });
  };
  const client = createKlaviyoProviderClient({ fetchImpl });
  const dates = await client.fetchSentCampaignDates({ accessToken: 'secret', timeZone: 'America/New_York' });
  assert.deepEqual(dates, ['2026-09-20', '2026-09-19']);
  assert.equal(JSON.stringify(dates).includes('campaign-secret'), false);
  assert.equal(calls.length, 2);
  assert.ok(calls.every(url => decodeURIComponent(url).includes("equals(status,'Sent')")));
});

test('Klaviyo sent campaign inventory rejects pagination outside the provider origin', async () => {
  const client = createKlaviyoProviderClient({
    fetchImpl: async () => response({
      data: [],
      links: { next: 'https://attacker.invalid/api/campaigns/?page[cursor]=steal' },
    }),
  });
  await assert.rejects(
    client.fetchSentCampaignDates({ accessToken: 'secret', timeZone: 'UTC' }),
    /KLAVIYO_CAMPAIGN_PAGINATION_INVALID/,
  );
});


test('Klaviyo campaign diagnostics distinguish total, Sent and missing scheduled_at without identifiers', async () => {
  const calls = [];
  const fetchImpl = async url => {
    calls.push(url);
    const decoded = decodeURIComponent(url);
    if (decoded.includes("'email'")) return response({
      data: [
        { id: 'draft-secret', attributes: { status: 'Draft', scheduled_at: null } },
        { id: 'sent-dated-secret', attributes: { status: 'Sent', scheduled_at: '2026-09-20T23:30:00Z' } },
        { id: 'sent-undated-secret', attributes: { status: 'Sent', scheduled_at: null } },
      ],
      links: { next: null },
    });
    return response({
      data: [{ id: 'sent-sms-secret', attributes: { status: 'Sent', scheduled_at: '2026-09-19T10:00:00Z' } }],
      links: { next: null },
    });
  };
  const client = createKlaviyoProviderClient({ fetchImpl });
  const result = await client.fetchCampaignInventory({ accessToken: 'secret', timeZone: 'America/New_York' });
  assert.deepEqual(result, {
    total_campaign_count: 4,
    sent_campaign_count: 3,
    sent_with_scheduled_at_count: 2,
    sent_without_scheduled_at_count: 1,
    sent_dates: ['2026-09-20', '2026-09-19'],
  });
  assert.equal(JSON.stringify(result).includes('secret'), false);
  assert.equal(calls.length, 2);
  assert.ok(calls.every(url => decodeURIComponent(url).includes("fields[campaign]=status,scheduled_at")));
  assert.ok(calls.every(url => !decodeURIComponent(url).includes("equals(status,'Sent')")));
});


test('Klaviyo Flow/Event diagnostics return only aggregate statuses, dates and allowlisted categories', async () => {
  const fetchImpl = async url => {
    const decoded = decodeURIComponent(url);
    if (decoded.includes('/api/flows/')) return response({
      data: [
        { id: 'flow-secret-1', attributes: { status: 'live' } },
        { id: 'flow-secret-2', attributes: { status: 'draft' } },
        { id: 'flow-secret-3', attributes: { status: 'manual' } },
      ],
      links: { next: null },
    });
    if (decoded.includes('/api/events/')) return response({
      data: [
        {
          id: 'event-secret-1',
          attributes: { datetime: '2024-01-01T10:00:00Z' },
          relationships: { metric: { data: { id: 'metric-1' } }, attributions: { data: [{ type: 'attribution', id: 'attribute-secret' }] } },
        },
        {
          id: 'event-secret-2',
          attributes: { datetime: '2024-01-03T12:00:00Z' },
          relationships: { metric: { data: { id: 'metric-2' } }, attributions: { data: [] } },
        },
        {
          id: 'event-secret-3',
          attributes: { datetime: '2024-01-02T12:00:00Z' },
          relationships: { metric: { data: { id: 'metric-3' } }, attributions: { data: [] } },
        },
      ],
      included: [
        { type: 'metric', id: 'metric-1', attributes: { name: 'Received Email', integration: { name: 'Klaviyo' } } },
        { type: 'metric', id: 'metric-2', attributes: { name: 'Placed Order', integration: { name: 'API' } } },
        { type: 'metric', id: 'metric-3', attributes: { name: 'Unlisted Private Metric', integration: { name: 'API' } } },
      ],
      links: { next: null },
    });
    throw new Error('unexpected URL');
  };
  const client = createKlaviyoProviderClient({ fetchImpl });
  const result = await client.fetchFlowEventInventory({ accessToken: 'secret', timeZone: 'UTC' });
  assert.equal(result.flow_count, 3);
  assert.deepEqual(result.flow_status_counts, { live: 1, manual: 1, draft: 1, other: 0 });
  assert.equal(result.scanned_event_count, 3);
  assert.equal(result.event_counts.received_email, 1);
  assert.equal(result.event_counts.placed_order, 1);
  assert.equal(result.event_counts.other, 1);
  assert.equal(result.attributed_event_count, 1);
  assert.equal(result.earliest_event_date, '2024-01-01');
  assert.equal(result.latest_event_date, '2024-01-03');
  assert.equal(result.event_scan_truncated, false);
  assert.doesNotMatch(JSON.stringify(result), /secret|Unlisted Private Metric|Klaviyo|API/);
});

test('Klaviyo Flow/Event diagnostics reject pagination outside the provider origin', async () => {
  const client = createKlaviyoProviderClient({
    fetchImpl: async url => url.includes('/api/flows/')
      ? response({ data: [], links: { next: 'https://attacker.invalid/api/flows/?page[cursor]=steal' } })
      : response({ data: [], included: [], links: { next: null } }),
  });
  await assert.rejects(
    client.fetchFlowEventInventory({ accessToken: 'secret', timeZone: 'UTC' }),
    /KLAVIYO_FLOW_PAGINATION_INVALID/,
  );
});


test('Klaviyo Flow/Event diagnostics accept an empty event page without an included collection', async () => {
  const client = createKlaviyoProviderClient({
    fetchImpl: async url => url.includes('/api/flows/')
      ? response({ data: [], links: { next: null } })
      : response({ data: [], links: { next: null } }),
  });
  const result = await client.fetchFlowEventInventory({ accessToken: 'secret', timeZone: 'UTC' });
  assert.equal(result.flow_count, 0);
  assert.equal(result.scanned_event_count, 0);
  assert.equal(result.earliest_event_date, null);
  assert.equal(result.latest_event_date, null);
  assert.equal(result.event_scan_truncated, false);
});

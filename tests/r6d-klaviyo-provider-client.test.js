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

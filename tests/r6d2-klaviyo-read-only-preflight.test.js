'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { closedProviderDate, createKlaviyoReadOnlyPreflight } = require('../src/providers/klaviyo/read-only-preflight');
const { registerShopifyKlaviyoAccountRoutes } = require('../src/routes/shopify-klaviyo-account-routes');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const connection = {
  provider: 'klaviyo', status: 'connected', accessToken: 'secret-token', sourceCurrency: 'USD', monthlyPlanCost: '25.00',
  selectedAccounts: [{ id: 'account-1', name: 'Account', currency: 'USD' }],
  conversionMetric: { id: 'metric-1', name: 'Placed Order', integrationName: 'Shopify' },
};

function preflight({ facts = { rows: [], verified_empty: true }, providerError = null } = {}) {
  return createKlaviyoReadOnlyPreflight({
    connectionStore: { resolveConnected: async input => { assert.deepEqual(input, { authority, provider: 'klaviyo' }); return connection; } },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 2 }) },
    providerClient: {
      fetchAccount: async ({ accessToken, accountId }) => {
        assert.equal(accessToken, 'secret-token');
        assert.equal(accountId, 'account-1');
        if (providerError) throw new Error(providerError);
        return { id: 'account-1', currency: 'USD', timezone: 'UTC' };
      },
      fetchMessageFacts: async ({ providerDate, conversionMetricId }) => {
        assert.equal(providerDate, '2026-09-23');
        assert.equal(conversionMetricId, 'metric-1');
        return facts;
      },
    },
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: '2026-09-23', fx_provider: 'test' }),
    now: () => new Date('2026-09-25T12:00:00Z'),
  });
}

test('R6-D2 chooses a provider date closed across supported timezones', () => {
  assert.equal(closedProviderDate(new Date('2026-09-25T00:01:00Z')), '2026-09-23');
});

test('R6-D2 preflight fails closed when the canonical metric binding is absent', async () => {
  const missing = createKlaviyoReadOnlyPreflight({
    connectionStore: { resolveConnected: async () => ({ ...connection, conversionMetric: null }) },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 2 }) },
    providerClient: { fetchAccount: async () => ({ id: 'account-1', currency: 'USD', timezone: 'UTC' }), fetchMessageFacts: async () => ({ rows: [], verified_empty: true }) },
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: '2026-09-23', fx_provider: 'test' }),
    now: () => new Date('2026-09-25T12:00:00Z'),
  });
  await assert.rejects(missing.execute(authority), error => error.code === 'KLAVIYO_PREFLIGHT_METRIC_REQUIRED' && error.status === 409);
});

test('R6-D2 read-only preflight returns aggregate evidence and never returns provider rows or tokens', async () => {
  const result = await preflight().execute(authority);
  assert.deepEqual(result, {
    status: 'PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT',
    provider_result_status: 'empty', selected_account_count: 1, row_count: 0, empty_provider_result: true,
    account_api_verified: true, campaign_reporting_verified: true, flow_reporting_verified: true, time_fx_verified: true,
    dataset_v2_write: false, production_activation: false, provider_date: '2026-09-23', currency_version: 2,
  });
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes('secret-token'), false);
  assert.equal(JSON.stringify(result).includes('account-1'), false);
});

test('R6-D2 read-only preflight redacts provider failures', async () => {
  await assert.rejects(preflight({ providerError: 'provider-secret-body' }).execute(authority), error => {
    assert.equal(error.code, 'KLAVIYO_PREFLIGHT_FAILED');
    assert.equal(error.status, 503);
    assert.equal(error.message.includes('provider-secret-body'), false);
    return true;
  });
});

test('R6-D2 route requires Shopify authority and ignores caller tenant/date fields', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let receivedAuthority;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: {},
    preflight: { execute: async input => { receivedAuthority = input; return { status: 'PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT' }; } },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/klaviyo/runtime/preflight']({ get: () => 'Bearer session', body: { workspace_id: 'attacker', provider_date: '2099-01-01' } }, res);
  assert.deepEqual(receivedAuthority, authority);
  assert.equal(res.code, 200);
});

test('R6-D2 metric discovery and selection routes remain Shopify-session-bound', async () => {
  const routes = {};
  const app = { get: (path, handler) => { routes[`GET ${path}`] = handler; }, post: (path, handler) => { routes[`POST ${path}`] = handler; } };
  const received = [];
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async () => authority,
    selection: {},
    metricBinding: {
      discover: async input => { received.push(['discover', input]); return { candidate_count: 1 }; },
      select: async (input, body) => { received.push(['select', input, body]); return { status: 'KLAVIYO_CONVERSION_METRIC_BOUND' }; },
    },
  });
  const res = () => ({ set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } });
  await routes['GET /api/shopify/providers/klaviyo/runtime/metrics']({ get: () => 'Bearer session' }, res());
  await routes['POST /api/shopify/providers/klaviyo/runtime/metrics/select']({ get: () => 'Bearer session', body: { metric_id: 'metric-1', workspace_id: 'attacker' } }, res());
  assert.deepEqual(received, [
    ['discover', authority],
    ['select', authority, { metric_id: 'metric-1', workspace_id: 'attacker' }],
  ]);
});

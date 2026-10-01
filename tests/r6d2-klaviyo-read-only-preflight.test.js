'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { JOURNEY_DIAGNOSTIC_DATE, closedProviderDate, resolveProviderDate, createKlaviyoReadOnlyPreflight } = require('../src/providers/klaviyo/read-only-preflight');
const { registerShopifyKlaviyoAccountRoutes } = require('../src/routes/shopify-klaviyo-account-routes');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const connection = {
  provider: 'klaviyo', status: 'connected', accessToken: 'secret-token', sourceCurrency: 'USD', monthlyPlanCost: '25.00',
  selectedAccounts: [{ id: 'account-1', name: 'Account', currency: 'USD' }],
  conversionMetric: { id: 'metric-1', name: 'Placed Order', integrationName: 'Shopify' },
  journeyMetrics: {
    addToCart: { id: 'metric-add', name: 'Added to Cart', integrationName: 'Shopify' },
    checkout: { id: 'metric-checkout', name: 'Checkout Started', integrationName: 'Shopify' },
    purchase: { id: 'metric-1', name: 'Placed Order', integrationName: 'Shopify' },
  },
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
      fetchMessageFacts: async ({ providerDate, conversionMetricId, journeyMetricIds }) => {
        assert.equal(providerDate, '2026-09-23');
        assert.equal(conversionMetricId, 'metric-1');
        assert.deepEqual(journeyMetricIds, { addToCart: 'metric-add', checkout: 'metric-checkout', purchase: 'metric-1' });
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

test('R6-D2 accepts only valid closed provider dates inside the bounded acceptance window', () => {
  const now = new Date('2026-09-25T12:00:00Z');
  assert.equal(resolveProviderDate(null, now), '2026-09-23');
  assert.equal(resolveProviderDate('2026-09-22', now), '2026-09-22');
  assert.throws(() => resolveProviderDate('2026-09-24', now), error => error.code === 'KLAVIYO_PROVIDER_DATE_NOT_CLOSED');
  assert.throws(() => resolveProviderDate('2026-02-30', now), error => error.code === 'KLAVIYO_PROVIDER_DATE_INVALID');
  assert.throws(() => resolveProviderDate('2026-08-01', now), error => error.code === 'KLAVIYO_PROVIDER_DATE_OUT_OF_RANGE');
});

test('R6-D2 preflight fails closed when the canonical metric binding is absent', async () => {
  const missing = createKlaviyoReadOnlyPreflight({
    connectionStore: { resolveConnected: async () => ({ ...connection, conversionMetric: null, journeyMetrics: { ...connection.journeyMetrics, purchase: null } }) },
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
    provider_result_status: 'empty', selected_account_count: 1, row_count: 0, campaign_row_count: 0, flow_row_count: 0, empty_provider_result: true,
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

test('R6-D5 journey diagnostic is fixed to the closed date and returns aggregate counts only', async () => {
  const diagnosticReport = {
    provider_date: JOURNEY_DIAGNOSTIC_DATE,
    purchase: {
      campaign: { row_count: 1, conversion_count: 1, conversion_value: 10, matched_key_count: 1, unmatched_key_count: 0 },
      flow: { row_count: 0, conversion_count: 0, conversion_value: 0, matched_key_count: 0, unmatched_key_count: 0 },
    },
    add_to_cart: {
      campaign: { row_count: 2, conversion_count: 2, conversion_value: 20, matched_key_count: 1, unmatched_key_count: 1 },
      flow: { row_count: 1, conversion_count: 1, conversion_value: 10, matched_key_count: 0, unmatched_key_count: 1 },
    },
    checkout: {
      campaign: { row_count: 1, conversion_count: 1, conversion_value: 10, matched_key_count: 1, unmatched_key_count: 0 },
      flow: { row_count: 0, conversion_count: 0, conversion_value: 0, matched_key_count: 0, unmatched_key_count: 0 },
    },
    journey_key_drift: true,
    dataset_v2_write: false,
  };
  const diagnostic = createKlaviyoReadOnlyPreflight({
    connectionStore: { resolveConnected: async () => connection },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 2 }) },
    providerClient: {
      fetchAccount: async () => ({ id: 'account-1', currency: 'USD', timezone: 'UTC' }),
      fetchMessageFacts: async input => {
        assert.equal(input.providerDate, JOURNEY_DIAGNOSTIC_DATE);
        assert.equal(input.includeDiagnostics, true);
        return { rows: [], verified_empty: true, diagnostics: diagnosticReport };
      },
    },
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: JOURNEY_DIAGNOSTIC_DATE, fx_provider: 'test' }),
    now: () => new Date('2026-10-01T12:00:00Z'),
  });
  const result = await diagnostic.executeDiagnostic(authority);
  assert.deepEqual(result, {
    status: 'PASS_R6_D5_KLAVIYO_JOURNEY_DIAGNOSTIC', provider_date: '2026-09-28', provider_result_status: 'empty',
    selected_account_count: 1, row_count: 0, campaign_row_count: 0, flow_row_count: 0,
    journey_diagnostics: diagnosticReport, dataset_v2_write: false, production_activation: false,
  });
  assert.doesNotMatch(JSON.stringify(result), /secret-token|account-1|metric-add|metric-checkout|metric-1/);
});

test('R6-D2 route requires Shopify authority and forwards only the validated provider date candidate', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let receivedAuthority;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: {},
    preflight: { execute: async (input, providerDate) => { receivedAuthority = { input, providerDate }; return { status: 'PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT' }; } },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/klaviyo/runtime/preflight']({ get: () => 'Bearer session', body: { workspace_id: 'attacker', provider_date: '2099-01-01' } }, res);
  assert.deepEqual(receivedAuthority, { input: authority, providerDate: '2099-01-01' });
  assert.equal(res.code, 200);
});

test('R6-D5 journey diagnostic route is Shopify-session-bound and accepts no tenant or date input', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let receivedAuthority;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: {},
    preflight: {
      execute: async () => ({}),
      executeDiagnostic: async input => { receivedAuthority = input; return { status: 'PASS_R6_D5_KLAVIYO_JOURNEY_DIAGNOSTIC' }; },
    },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/klaviyo/runtime/journey-diagnostic']({
    get: () => 'Bearer session', body: { workspace_id: 'attacker', provider_date: '2099-01-01' },
  }, res);
  assert.equal(receivedAuthority, authority);
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

test('R6-D2 C6 controlled Dataset acceptance route is session-bound and forwards confirmation plus provider date', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let received;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: {},
    datasetAcceptance: { execute: async (input, confirmation, providerDate) => {
      received = { input, confirmation, providerDate };
      return { status: 'PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE', attempted: 0, persisted: 0 };
    } },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/klaviyo/runtime/acceptance']({
    get: () => 'Bearer session',
    body: { confirmation: 'RUN_R6_D2_C6_KLAVIYO_WRITE', workspace_id: 'attacker', provider_date: '2099-01-01' },
  }, res);
  assert.deepEqual(received, { input: authority, confirmation: 'RUN_R6_D2_C6_KLAVIYO_WRITE', providerDate: '2099-01-01' });
  assert.equal(res.code, 200);
});




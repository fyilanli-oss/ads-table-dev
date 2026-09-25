'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  CONFIRMATION,
  createKlaviyoControlledDatasetAcceptance,
} = require('../src/providers/klaviyo/controlled-dataset-acceptance');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: WORKSPACE,
  source: 'shopify_verified_session',
});
const connection = Object.freeze({
  provider: 'klaviyo', status: 'connected', accessToken: 'secret', sourceCurrency: 'USD', monthlyPlanCost: '50.00',
  selectedAccounts: [{ id: 'account-1', name: 'Account', currency: 'USD' }],
  conversionMetric: { id: 'metric-1', name: 'Placed Order', integrationName: 'Shopify' },
});
const fact = Object.freeze({
  branch: 'campaign', channel: 'email',
  root: { id: 'campaign-1', name: 'Campaign' },
  message: { id: 'message-1', name: 'Message' },
  metrics: {
    delivered: 100, unique_clicks: 10, unique_opens: 30,
    add_to_cart: null, add_to_cart_value: null, checkout: null, checkout_value: null,
    purchase: 2, purchase_value: 50,
  },
  metric_support: { add_to_cart: 'unknown', add_to_cart_value: 'unknown', checkout: 'unknown', checkout_value: 'unknown' },
  spend_allocation: { dailySentCount: 100, monthlySentCount: 1000, periodClosed: true },
});

function acceptance({ result = { rows: [fact], verified_empty: false }, existingRows = [] } = {}) {
  const calls = [];
  const service = createKlaviyoControlledDatasetAcceptance({
    connectionStore: { resolveConnected: async input => { calls.push(['connection', input]); return connection; } },
    settingsStore: { resolveReportingCurrency: async input => { calls.push(['currency', input]); return { reportingCurrency: 'TRY', currencyVersion: 2 }; } },
    providerClient: {
      fetchAccount: async input => { calls.push(['account', input]); return { id: 'account-1', currency: 'USD', timezone: 'UTC' }; },
      fetchMessageFacts: async input => { calls.push(['facts', input]); return result; },
    },
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: '2026-09-23', fx_provider: 'test_fx' }),
    repository: {
      readCanonicalRawFacts: async input => { calls.push(['read', input]); return existingRows; },
      upsertCanonicalRawFacts: async rows => { calls.push(['write', rows]); return rows; },
    },
    now: () => new Date('2026-09-25T12:00:00Z'),
  });
  return { calls, service };
}

test('C6 controlled acceptance requires exact action-time confirmation before provider or Dataset access', async () => {
  const { calls, service } = acceptance();
  await assert.rejects(service.execute(authority, 'wrong'), error =>
    error.code === 'KLAVIYO_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED' && error.status === 409);
  assert.deepEqual(calls, []);
});

test('C6 controlled acceptance persists only verified workspace rows and returns aggregate evidence', async () => {
  const { calls, service } = acceptance();
  const result = await service.execute(authority, CONFIRMATION);
  assert.deepEqual(result, {
    status: 'PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE',
    attempted: 1, persisted: 1, empty_provider_result: false, provider_result_status: 'non_empty',
    selected_account_count: 1, provider_date: '2026-09-23', production_activation: false, currency_version: 2,
  });
  const write = calls.find(([name]) => name === 'write')[1];
  assert.equal(write.length, 1);
  assert.equal(write[0].identity.workspace_id, WORKSPACE);
  assert.equal(write[0].identity.platform, 'klaviyo');
  assert.equal(write[0].currency.source_currency, 'USD');
  assert.equal(write[0].currency.target_currency, 'TRY');
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes('account-1'), false);
  assert.equal(JSON.stringify(result).includes('secret'), false);
});

test('C6 controlled acceptance preserves verified-empty without synthetic rows', async () => {
  const { calls, service } = acceptance({ result: { rows: [], verified_empty: true } });
  const result = await service.execute(authority, CONFIRMATION);
  assert.equal(result.attempted, 0);
  assert.equal(result.persisted, 0);
  assert.equal(result.empty_provider_result, true);
  assert.deepEqual(calls.find(([name]) => name === 'write')[1], []);
});

test('C6 controlled acceptance blocks an already persisted workspace/account/date before provider contact', async () => {
  const { calls, service } = acceptance({ existingRows: [{ id: 'existing-row' }] });
  await assert.rejects(service.execute(authority, CONFIRMATION), error =>
    error.code === 'KLAVIYO_DATASET_ACCEPTANCE_ALREADY_EXECUTED' && error.status === 409);
  assert.equal(calls.filter(([name]) => name === 'read').length, 1);
  assert.equal(calls.some(([name]) => name === 'account'), false);
  assert.equal(calls.some(([name]) => name === 'facts'), false);
  assert.equal(calls.some(([name]) => name === 'write'), false);
});

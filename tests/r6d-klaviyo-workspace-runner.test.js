'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { validateWorkspaceCanonicalRow } = require('../funnel-core/workspace-canonical-contract');
const { createKlaviyoWorkspaceRunner } = require('../src/providers/klaviyo/workspace-runner');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const fact = {
  branch: 'campaign', channel: 'email',
  root: { id: 'campaign-1', name: 'Campaign' },
  message: { id: 'message-1', name: 'Message' },
  metrics: { delivered: 100, unique_clicks: 10, unique_opens: 30, add_to_cart: null, add_to_cart_value: null, checkout: null, checkout_value: null, purchase: null, purchase_value: null },
  metric_support: { add_to_cart: 'unknown', add_to_cart_value: 'unknown', checkout: 'unknown', checkout_value: 'unknown', purchase: 'unknown', purchase_value: 'unknown' },
  spend_allocation: { dailySentCount: 100, monthlySentCount: 1000, periodClosed: true },
};

function context(patch = {}) {
  return {
    authority: { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' },
    connection: {
      provider: 'klaviyo', status: 'connected', accessToken: 'secret', sourceCurrency: 'USD', monthlyPlanCost: '50.00',
      selectedAccounts: [{ id: 'account-1', name: 'Account', currency: 'USD' }],
      conversionMetric: { id: 'metric-1', name: 'Placed Order', integrationName: 'Shopify' },
    },
    reportingCurrency: 'TRY', currencyVersion: 1,
    request: { provider_date: '2026-09-24' },
    ...patch,
  };
}

function runner({ account = { id: 'account-1', currency: 'USD', timezone: 'America/New_York' }, result = { rows: [fact], verified_empty: false } } = {}) {
  return createKlaviyoWorkspaceRunner({
    providerClient: { fetchAccount: async () => account, fetchMessageFacts: async () => result },
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: '2026-09-24', fx_provider: 'test_fx' }),
  });
}

test('Klaviyo workspace runner maps only verified provider facts into workspace canonical rows', async () => {
  const result = await runner()(context());
  assert.equal(result.provider_result_status, 'non_empty');
  assert.deepEqual(result.checked_account_ids, ['account-1']);
  assert.equal(result.rows.length, 1);
  const row = result.rows[0];
  validateWorkspaceCanonicalRow(row);
  assert.equal(row.identity.workspace_id, WORKSPACE);
  assert.equal(row.identity.user_id, null);
  assert.equal(row.currency.source_currency, 'USD');
  assert.equal(row.currency.target_currency, 'TRY');
  assert.equal(row.raw_metrics.spend_value, 200);
  assert.equal(Object.hasOwn(result, 'accessToken'), false);
});

test('Klaviyo workspace runner accepts only provider-proven empty results and writes no fake row', async () => {
  const empty = await runner({ result: { rows: [], verified_empty: true } })(context());
  assert.deepEqual(empty.rows, []);
  assert.equal(empty.provider_result_status, 'empty');
  await assert.rejects(runner({ result: { rows: [], verified_empty: false } })(context()), /KLAVIYO_EMPTY_RESULT_NOT_VERIFIED/);
});

test('Klaviyo workspace runner rejects account, currency and canonical connection drift', async () => {
  await assert.rejects(runner({ account: { id: 'other', currency: 'USD', timezone: 'UTC' } })(context()), /KLAVIYO_PROVIDER_ACCOUNT_MISMATCH/);
  await assert.rejects(runner({ account: { id: 'account-1', currency: 'EUR', timezone: 'UTC' } })(context()), /KLAVIYO_PROVIDER_CURRENCY_MISMATCH/);
  await assert.rejects(runner()(context({ connection: { ...context().connection, selectedAccounts: [] } })), /KLAVIYO_SINGLE_SELECTED_ACCOUNT_REQUIRED/);
});

test('Klaviyo workspace runner uses canonical monthly plan cost, not caller or provider cost', async () => {
  const poisoned = { ...fact, spend_allocation: { ...fact.spend_allocation, monthlyPlanCost: 9999 } };
  const result = await runner({ result: { rows: [poisoned], verified_empty: false } })(context());
  assert.equal(result.rows[0].raw_metrics.spend_value, 200);
});

test('Klaviyo workspace runner requires a canonical account-scoped conversion metric', async () => {
  await assert.rejects(
    runner()(context({ connection: { ...context().connection, conversionMetric: null } })),
    /connection\.conversionMetric\.id is required/
  );
});

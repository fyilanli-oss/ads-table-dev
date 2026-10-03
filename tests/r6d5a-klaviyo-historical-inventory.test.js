'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createKlaviyoHistoricalInventory } = require('../src/providers/klaviyo/historical-inventory');
const { registerShopifyKlaviyoAccountRoutes } = require('../src/routes/shopify-klaviyo-account-routes');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const connection = {
  provider: 'klaviyo',
  status: 'connected',
  accessToken: 'secret-token',
  sourceCurrency: 'USD',
  monthlyPlanCost: '25.00',
  selectedAccounts: [{ id: 'account-1', name: 'Account', currency: 'USD' }],
  conversionMetric: { id: 'metric-1', name: 'Placed Order', integrationName: 'Klaviyo' },
  journeyMetrics: {
    addToCart: { id: 'metric-add', name: 'Added to Cart', integrationName: 'Klaviyo' },
    checkout: { id: 'metric-checkout', name: 'Checkout Started', integrationName: 'Klaviyo' },
    purchase: { id: 'metric-1', name: 'Placed Order', integrationName: 'Klaviyo' },
  },
};

function inventory({ dates, counts = { total: 3, sent: 3, dated: 3, undated: 0 }, onFx = () => {} }) {
  const providerDates = [];
  return {
    providerDates,
    service: createKlaviyoHistoricalInventory({
      connectionStore: {
        resolveConnected: async input => {
          assert.deepEqual(input, { authority, provider: 'klaviyo' });
          return connection;
        },
      },
      settingsStore: {
        resolveReportingCurrency: async input => {
          assert.deepEqual(input, authority);
          return { reportingCurrency: 'TRY', currencyVersion: 3 };
        },
      },
      providerClient: {
        fetchAccount: async ({ accessToken, accountId }) => {
          assert.equal(accessToken, 'secret-token');
          assert.equal(accountId, 'account-1');
          return { id: 'account-1', currency: 'USD', timezone: 'UTC' };
        },
        fetchCampaignInventory: async ({ accessToken, timeZone }) => {
          assert.equal(accessToken, 'secret-token');
          assert.equal(timeZone, 'UTC');
          return {
            total_campaign_count: counts.total,
            sent_campaign_count: counts.sent,
            sent_with_scheduled_at_count: counts.dated,
            sent_without_scheduled_at_count: counts.undated,
            sent_dates: dates,
          };
        },
        fetchMessageFacts: async ({ accessToken, providerDate, conversionMetricId, journeyMetricIds }) => {
          assert.equal(accessToken, 'secret-token');
          assert.equal(conversionMetricId, 'metric-1');
          assert.deepEqual(journeyMetricIds, { addToCart: 'metric-add', checkout: 'metric-checkout', purchase: 'metric-1' });
          providerDates.push(providerDate);
          return { rows: [], verified_empty: true };
        },
      },
      resolveFxRate: async (source, target, options) => {
        onFx();
        assert.equal(source, 'USD');
        assert.equal(target, 'TRY');
        assert.deepEqual(options, { rateDate: '2026-09-24' });
        return { fx_rate: 40, fx_rate_date: '2026-09-24', fx_provider: 'test' };
      },
      now: () => new Date('2026-09-25T12:00:00Z'),
    }),
  };
}

test('R6-D5-A checks the most recent sent-campaign date through today and marks open attribution provisional', async () => {
  const setup = inventory({ dates: ['2026-09-24', '2026-09-20', '2026-09-19'] });
  const result = await setup.service.execute(authority);
  assert.deepEqual(setup.providerDates, ['2026-09-24']);
  assert.deepEqual(result, {
    status: 'PASS_R6_D5_A_KLAVIYO_HISTORICAL_INVENTORY',
    provider_result_status: 'empty',
    selected_account_count: 1,
    total_campaign_count: 3,
    sent_campaign_count: 3,
    sent_with_scheduled_at_count: 3,
    sent_without_scheduled_at_count: 0,
    available_sent_date_count: 3,
    checked_date_count: 1,
    row_count: 0,
    empty_provider_result: true,
    account_api_verified: true,
    campaign_inventory_verified: true,
    campaign_reporting_verified: true,
    flow_reporting_verified: true,
    time_fx_verified: true,
    dataset_v2_write: false,
    production_activation: false,
    provider_date: '2026-09-24',
    provider_date_finality: 'provisional',
    attribution_window_days: 5,
    reconciliation_required: true,
    currency_version: 3,
  });
  assert.equal(JSON.stringify(result).includes('secret-token'), false);
  assert.equal(JSON.stringify(result).includes('account-1'), false);
});

test('R6-D5-A reports a verified inventory with no eligible date without resolving FX or reports', async () => {
  let fxCalls = 0;
  const setup = inventory({ dates: [], counts: { total: 4, sent: 2, dated: 0, undated: 2 }, onFx: () => { fxCalls += 1; } });
  const result = await setup.service.execute(authority);
  assert.equal(result.checked_date_count, 0);
  assert.equal(result.provider_date, null);
  assert.equal(result.provider_date_finality, null);
  assert.equal(result.attribution_window_days, 5);
  assert.equal(result.reconciliation_required, false);
  assert.equal(result.total_campaign_count, 4);
  assert.equal(result.sent_campaign_count, 2);
  assert.equal(result.sent_with_scheduled_at_count, 0);
  assert.equal(result.sent_without_scheduled_at_count, 2);
  assert.equal(result.dataset_v2_write, false);
  assert.equal(fxCalls, 0);
  assert.deepEqual(setup.providerDates, []);
});

test('R6-D5-A route is Shopify-session-bound and ignores caller tenant/date fields', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let received;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => {
      assert.equal(session_token, 'session');
      return authority;
    },
    selection: {},
    historicalInventory: {
      execute: async input => {
        received = input;
        return { status: 'PASS_R6_D5_A_KLAVIYO_HISTORICAL_INVENTORY' };
      },
    },
  });
  const res = {
    set() {},
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return body; },
  };
  await routes['/api/shopify/providers/klaviyo/runtime/historical-inventory']({
    get: () => 'Bearer session',
    body: { workspace_id: 'attacker', provider_date: '2099-01-01' },
  }, res);
  assert.deepEqual(received, authority);
  assert.equal(res.code, 200);
});

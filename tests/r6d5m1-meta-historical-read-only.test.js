'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  createMetaHistoricalReadOnlyInventory,
  providerMetricEvidence,
  subtractDays,
} = require('../src/providers/meta/historical-read-only-inventory');
const { registerShopifyAdAccountRoutes } = require('../src/routes/shopify-ad-account-routes');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e4-meta/e4-t1-provider-fixture.json'), 'utf8'));
const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d5m1-meta-historical-read-only-v1.json'), 'utf8'));
const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const now = () => new Date('2026-10-03T12:00:00.000Z');
const connection = {
  provider: 'meta',
  status: 'connected',
  accessToken: 'secret-token',
  accessTokenExpiresAt: '2026-11-01T00:00:00.000Z',
  grantedScopes: ['ads_read'],
  selectedAccounts: [{ id: fixture.account.id, name: fixture.account.name, currency: fixture.account.currency }],
};

function providerRow({ conversions = true } = {}) {
  return {
    ...fixture.insight,
    date_start: '2026-09-20',
    date_stop: '2026-09-20',
    actions: conversions ? fixture.insight.actions : [{ action_type: 'link_click', value: '60' }],
    action_values: conversions ? fixture.insight.action_values : [],
  };
}

function transport({ rows = [providerRow()] } = {}) {
  return async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/me/adaccounts')) {
      return { ok: true, status: 200, json: async () => ({ data: [fixture.account] }) };
    }
    const range = JSON.parse(target.searchParams.get('time_range'));
    const data = range.since === range.until
      ? rows.filter(row => row.date_start === range.since)
      : rows;
    return { ok: true, status: 200, json: async () => ({ data }) };
  };
}

function createInventory(selectedTransport) {
  return createMetaHistoricalReadOnlyInventory({
    connectionStore: { resolveConnected: async input => {
      assert.deepEqual(input, { authority, provider: 'meta' });
      return connection;
    } },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 7 }) },
    transport: selectedTransport,
    resolveFxRate: async (_source, _target, { rateDate }) => ({
      fx_rate: 40,
      fx_rate_date: rateDate,
      fx_provider: 'approved-test',
    }),
    now,
  });
}

test('R6-D5-M1 contract freezes a bounded read-only and no-zero conversion boundary', () => {
  assert.equal(contract.status, 'LIVE_PROVIDER_ACTION_EVIDENCE_PASS_VISUAL_ACCEPTANCE_PENDING');
  assert.equal(contract.read_only_inventory.bounded_lookback_days, 31);
  assert.equal(contract.read_only_inventory.hierarchy, 'campaign_to_adset_to_ad');
  assert.equal(contract.read_only_inventory.ad_click_source, 'actions.link_click');
  assert.equal(contract.read_only_inventory.conversion_missing_semantics, 'unknown_not_zero');
  assert.equal(contract.authority.account_and_date_from_browser, false);
  assert.ok(contract.forbidden.includes('dataset_v2_write'));
  assert.ok(contract.forbidden.includes('schedule_or_backfill'));
});

test('R6-D5-M1 finds the latest provider date and returns only aggregate canonical evidence', async () => {
  const result = await createInventory(transport()).execute(authority);
  assert.equal(result.status, 'PASS_R6_D5_M1_META_HISTORICAL_READ_ONLY');
  assert.equal(result.provider_result_status, 'non_empty');
  assert.equal(result.selected_account_count, 1);
  assert.equal(result.lookback_days, 31);
  assert.equal(result.provider_date_start, '2026-09-20');
  assert.equal(result.provider_date_end, '2026-09-20');
  assert.equal(result.row_count, 1);
  assert.equal(result.campaign_count, 1);
  assert.equal(result.adset_count, 1);
  assert.equal(result.ad_count, 1);
  assert.equal(result.impression_total, 1000);
  assert.equal(result.ad_click_total, 60);
  assert.equal(result.spend_total, 5020);
  assert.equal(result.reporting_currency, 'TRY');
  assert.equal(result.conversion_support.purchase.supported_rows, 1);
  assert.deepEqual(
    result.provider_action_evidence[0].actions.entries.find(entry => entry.action_type === 'purchase'),
    { action_type: 'purchase', value: '4', entry_count: 1 },
  );
  assert.deepEqual(
    result.provider_action_evidence[0].action_values.entries.find(entry => entry.action_type === 'purchase'),
    { action_type: 'purchase', value: '480.00', entry_count: 1 },
  );
  assert.equal(result.dataset_v2_write, false);
  assert.equal(result.production_activation, false);
  assert.equal(JSON.stringify(result).includes('secret-token'), false);
  assert.equal(JSON.stringify(result).includes(fixture.account.id), false);
  assert.equal(JSON.stringify(result).includes(fixture.insight.ad_id), false);
});

test('R6-D5-M1 preserves missing pixel conversion facts as unknown rather than zero', async () => {
  const result = await createInventory(transport({ rows: [providerRow({ conversions: false })] })).execute(authority);
  assert.equal(result.provider_result_status, 'non_empty');
  assert.equal(result.ad_click_total, 60);
  assert.equal(result.conversion_support.add_to_cart.supported_rows, 0);
  assert.equal(result.conversion_support.add_to_cart.unknown_rows, 1);
  assert.equal(result.conversion_support.checkout.unknown_rows, 1);
  assert.equal(result.conversion_support.purchase.unknown_rows, 1);
  assert.deepEqual(result.provider_action_evidence[0].actions.entries, [
    { action_type: 'link_click', value: '60', entry_count: 1 },
  ]);
  assert.deepEqual(result.provider_action_evidence[0].action_values.entries, []);
});

test('provider action evidence exposes only sanitized types, values and counts', () => {
  const result = providerMetricEvidence([
    { actions: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '1' }] },
    { actions: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '1' }, { action_type: '<unsafe>', value: '2' }] },
  ], 'actions');
  assert.deepEqual(result.entries, [
    { action_type: 'offsite_conversion.fb_pixel_purchase', value: '1', entry_count: 2 },
  ]);
  assert.equal(result.malformed_entry_count, 1);
  assert.equal(JSON.stringify(result).includes('campaign_id'), false);
});

test('R6-D5-M1 returns verified empty without FX or Dataset writes when the bounded window has no rows', async () => {
  let fxCalls = 0;
  const inventory = createMetaHistoricalReadOnlyInventory({
    connectionStore: { resolveConnected: async () => connection },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 7 }) },
    transport: transport({ rows: [] }),
    resolveFxRate: async () => { fxCalls += 1; throw new Error('FX should not run'); },
    now,
  });
  const result = await inventory.execute(authority);
  assert.equal(result.provider_result_status, 'empty');
  assert.equal(result.row_count, 0);
  assert.equal(result.time_fx_verified, false);
  assert.equal(result.dataset_v2_write, false);
  assert.equal(fxCalls, 0);
});

test('R6-D5-M1 uses a closed 31-day range', () => {
  assert.equal(subtractDays('2026-10-02', 30), '2026-09-02');
});

test('Meta historical route is Shopify-session-bound and ignores caller account and date', async () => {
  const routes = {};
  const app = { get() {}, post: (route, handler) => { routes[route] = handler; } };
  let received;
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => {
      assert.equal(session_token, 'session');
      return authority;
    },
    selection: { status() {}, list() {}, complete() {}, reporting() {} },
    metaHistoricalInventory: {
      execute: async input => {
        received = input;
        return { status: 'PASS_R6_D5_M1_META_HISTORICAL_READ_ONLY' };
      },
    },
  });
  const res = {
    set() {},
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return body; },
  };
  await routes['/api/shopify/providers/meta/runtime/historical-inventory']({
    get: () => 'Bearer session',
    body: { workspace_id: 'attacker', account_id: 'attacker', provider_date: '2099-01-01' },
  }, res);
  assert.deepEqual(received, authority);
  assert.equal(res.code, 200);
});

test('Meta historical UI uses only approved Shopify controls and remains operator-gated', () => {
  const html = renderEmbeddedPlatforms({
    clientId: 'client',
    providerOAuthEnabled: true,
    providerAvailability: { meta: true },
  });
  assert.match(html, /id="r6d3-meta-historical-step"/);
  assert.match(html, /<s-button id="r6d3-meta-historical-run" variant="secondary">/);
  assert.match(html, /<s-paragraph id="r6d3-meta-historical-message" aria-live="polite">/);
  assert.match(html, /\/api\/shopify\/providers\/meta\/runtime\/historical-inventory/);
  assert.match(html, /Provider response evidence:/);
  assert.match(html, /action_values \[/);
  assert.match(html, /params\.get\("acceptance"\) === "r6d3-meta"/);
  assert.doesNotMatch(html, /<button[^>]*r6d3-meta-historical/);
  assert.doesNotMatch(html, /style=["'][^"']*r6d3-meta-historical/);
});

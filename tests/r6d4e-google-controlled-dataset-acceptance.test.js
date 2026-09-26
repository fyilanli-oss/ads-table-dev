'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  CONFIRMATION,
  acceptanceDateWindow,
  createGoogleControlledDatasetAcceptance,
} = require('../src/providers/google/controlled-dataset-acceptance');
const {registerShopifyAdAccountRoutes} = require('../src/routes/shopify-ad-account-routes');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const standardFixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e5-google/e5-t3-standard-ad-fixture.json'), 'utf8'));
const pmaxFixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e5-google/e5-t4-pmax-asset-group-fixture.json'), 'utf8'));
const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d4e-google-controlled-dataset-acceptance-v1.json'), 'utf8'));
const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = Object.freeze({authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session'});
const accounts = Object.freeze([
  {id: '1111111111', name: 'One', currency: 'USD', login_customer_id: '9999999999'},
  {id: '2222222222', name: 'Two', currency: 'USD', login_customer_id: '9999999999'},
  {id: '3333333333', name: 'Three', currency: 'USD', login_customer_id: '8888888888'},
]);
const connection = Object.freeze({
  provider: 'google_ads', status: 'connected', version: 7, accessToken: 'secret-access', refreshToken: 'secret-refresh',
  accessTokenExpiresAt: '2026-10-01T00:00:00.000Z', selectedAccounts: accounts,
});
const now = () => new Date('2026-08-31T12:00:00.000Z');

function acceptance({withRows = true, existingRows = [], failAt = null} = {}) {
  const calls = [];
  const fail = stage => { if (failAt === stage) throw new Error('secret--must-not-leak'); };
  const search = async input => {
    calls.push(['provider', input]);
    fail('provider');
    if (input.query.includes('customer.currency_code')) return {results: [{customer: {id: input.customerId, currencyCode: 'USD', timeZone: 'America/Los_Angeles'}}]};
    if (input.query.includes('FROM ad_group_ad') && input.query.includes('metrics.impressions')) return {results: withRows ? [standardFixture.row] : []};
    if (input.query.includes('FROM asset_group') && input.query.includes('metrics.impressions')) return {results: withRows ? [pmaxFixture.row] : []};
    if (input.query.includes('metrics.conversions')) return {results: []};
    throw new Error('unexpected Google query');
  };
  const service = createGoogleControlledDatasetAcceptance({
    connectionStore: {resolveConnected: async input => { calls.push(['connection', input]); fail('connection'); return connection; }},
    settingsStore: {resolveReportingCurrency: async input => { calls.push(['currency', input]); fail('currency'); return {reportingCurrency: 'TRY', currencyVersion: 7}; }},
    tokenLifecycle: {run: async ({connection: current, operation}) => { calls.push(['token']); fail('token'); return {connection: current, value: await operation(current)}; }},
    search,
    resolveFxRate: async (_source, _target, input) => { calls.push(['fx', input]); fail('fx'); return {fx_rate: 40, fx_rate_date: input.rateDate, fx_provider: 'approved-test'}; },
    repository: {
      readCanonicalRawFacts: async input => { calls.push(['read', input]); fail('guard'); return existingRows; },
      upsertCanonicalRawFacts: async rows => { calls.push(['write', rows]); fail('write'); return rows; },
    },
    now,
  });
  return {calls, service};
}

test('R6-D4-E uses a conservative three-day guard window for account timezones', () => {
  assert.deepEqual(acceptanceDateWindow(now), {from: '2026-08-29', to: '2026-08-31'});
});

test('R6-D4-E contract reuses completed Google runtime and keeps production activation closed', () => {
  assert.equal(contract.status, 'PASS_REPOSITORY_IMPLEMENTATION_PRODUCTION_ACCEPTANCE_PENDING');
  assert.equal(contract.canonical_dataset_platform, 'google');
  assert.equal(contract.reuse.e5_standard_performance_max_conversion_time_fx, true);
  assert.equal(contract.reuse.r6d4d_workspace_runner, true);
  assert.equal(contract.reuse.legacy_server_refresh_reactivated, false);
  assert.equal(contract.controls.verified_empty_writes_synthetic_rows, false);
  assert.equal(contract.production_deployed, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.equal(contract.live_acceptance.status, 'PENDING');
});

test('R6-D4-E requires exact action-time confirmation before provider or Dataset access', async () => {
  const {calls, service} = acceptance();
  await assert.rejects(service.execute(authority, 'wrong'), error => error.code === 'GOOGLE_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED' && error.status === 409);
  assert.deepEqual(calls, []);
});

test('R6-D4-E persists only verified workspace Google rows across all selected accounts and branches', async () => {
  const {calls, service} = acceptance();
  const result = await service.execute(authority, CONFIRMATION);
  assert.equal(result.status, 'PASS_R6_D4_E_GOOGLE_DATASET_WRITE');
  assert.equal(result.selected_account_count, 3);
  assert.equal(result.standard_row_count, 3);
  assert.equal(result.pmax_row_count, 3);
  assert.equal(result.attempted, 6);
  assert.equal(result.persisted, 6);
  assert.equal(result.production_activation, false);
  const write = calls.find(([name]) => name === 'write')[1];
  assert.equal(write.length, 6);
  assert.deepEqual(new Set(write.map(row => row.identity.platform_account_id)), new Set(accounts.map(account => account.id)));
  for (const row of write) {
    assert.equal(row.identity.workspace_id, WORKSPACE);
    assert.equal(row.identity.user_id, null);
    assert.equal(row.identity.platform, 'google');
    assert.equal(row.currency.source_currency, 'USD');
    assert.equal(row.currency.target_currency, 'TRY');
    assert.equal(row.provenance.synthetic, false);
  }
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes('secret-access'), false);
});

test('R6-D4-E preserves provider-verified empty without synthetic rows', async () => {
  const {calls, service} = acceptance({withRows: false});
  const result = await service.execute(authority, CONFIRMATION);
  assert.equal(result.attempted, 0);
  assert.equal(result.persisted, 0);
  assert.equal(result.empty_provider_result, true);
  assert.equal(result.standard_row_count, 0);
  assert.equal(result.pmax_row_count, 0);
  assert.deepEqual(calls.find(([name]) => name === 'write')[1], []);
});

test('R6-D4-E guards every selected account with canonical Google platform before provider contact', async () => {
  const {calls, service} = acceptance({existingRows: [{id: 'existing'}]});
  await assert.rejects(service.execute(authority, CONFIRMATION), error => error.code === 'GOOGLE_DATASET_ACCEPTANCE_ALREADY_EXECUTED' && error.status === 409);
  assert.equal(calls.filter(([name]) => name === 'read').length, 1);
  assert.equal(calls.find(([name]) => name === 'read')[1].platform, 'google');
  assert.equal(calls.some(([name]) => name === 'provider'), false);
  assert.equal(calls.some(([name]) => name === 'write'), false);
});

test('R6-D4-E returns only allowlisted failure stages and redacts underlying details', async () => {
  for (const [failAt, stage] of [['connection', 'CONNECTION'], ['currency', 'CURRENCY'], ['guard', 'DATASET_GUARD'], ['token', 'TOKEN_LIFECYCLE'], ['provider', 'PROVIDER_FACTS'], ['fx', 'PROVIDER_FACTS'], ['write', 'DATASET_PERSISTENCE']]) {
    const {service} = acceptance({failAt});
    await assert.rejects(service.execute(authority, CONFIRMATION), error => {
      assert.equal(error.code, `GOOGLE_DATASET_ACCEPTANCE_FAILED_${stage}`);
      assert.equal(error.status, 503);
      assert.doesNotMatch(`${error.code}:${error.message}`, /secret-|secret-access|secret-refresh/i);
      return true;
    });
  }
});

test('Google controlled acceptance route is Shopify-session-bound and forwards only confirmation', async () => {
  const routes = {};
  const app = {get() {}, post: (route, handler) => { routes[route] = handler; }};
  let received;
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async ({session_token}) => { assert.equal(session_token, 'session'); return authority; },
    selection: {status() {}, list() {}, complete() {}},
    googleDatasetAcceptance: {execute: async (...input) => { received = input; return {status: 'PASS_R6_D4_E_GOOGLE_DATASET_WRITE', attempted: 0, persisted: 0}; }},
  });
  const res = {set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; }};
  await routes['/api/shopify/providers/google_ads/runtime/acceptance']({get: () => 'Bearer session', body: {confirmation: CONFIRMATION, workspace_id: 'attacker', provider_date: '2099-01-01'}}, res);
  assert.deepEqual(received, [authority, CONFIRMATION]);
  assert.equal(res.code, 200);
});

test('Google controlled Dataset acceptance stays inside the explicit hidden operator surface', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true, providerAvailability: {google_ads: true}});
  assert.match(html, /id="r6d4-google-acceptance" hidden/);
  assert.match(html, /id="r6d4-google-dataset-run"/);
  assert.match(html, /RUN_R6_D4_E_GOOGLE_WRITE/);
  assert.match(html, /\/api\/shopify\/providers\/google_ads\/runtime\/acceptance/);
});


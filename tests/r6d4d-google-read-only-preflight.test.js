'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {createGoogleAdsSearchClient} = require('../src/providers/google/search-client');
const {createGoogleWorkspaceRunner, previousDate} = require('../src/providers/google/workspace-runner');
const {createGoogleReadOnlyPreflight} = require('../src/providers/google/read-only-preflight');
const {mapGoogleStandardAd} = require('../src/providers/google/standard-mapper');
const {mapGooglePmaxAssetGroup} = require('../src/providers/google/pmax-mapper');
const {registerShopifyAdAccountRoutes} = require('../src/routes/shopify-ad-account-routes');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = {authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session'};
const standardFixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e5-google/e5-t3-standard-ad-fixture.json'), 'utf8'));
const pmaxFixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e5-google/e5-t4-pmax-asset-group-fixture.json'), 'utf8'));
const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d4d-google-read-only-preflight-v1.json'), 'utf8'));
const liveEvidence = JSON.parse(fs.readFileSync(path.join(__dirname, '../docs/security/evidence/R6D4D_GOOGLE_READ_ONLY_LIVE_ACCEPTANCE_2026-09-26.json'), 'utf8'));
const accounts = [
  {id: '1111111111', name: 'One', currency: 'USD', login_customer_id: '9999999999'},
  {id: '2222222222', name: 'Two', currency: 'USD', login_customer_id: '9999999999'},
  {id: '3333333333', name: 'Three', currency: 'USD', login_customer_id: '8888888888'},
];
const connection = {
  provider: 'google_ads', status: 'connected', version: 7, accessToken: 'secret-access', refreshToken: 'secret-refresh',
  accessTokenExpiresAt: '2026-10-01T00:00:00.000Z', selectedAccounts: accounts,
};
const now = () => new Date('2026-08-31T12:00:00.000Z');

function providerSearch(calls, {withRows = true} = {}) {
  return async input => {
    calls.push(input);
    if (input.query.includes('customer.currency_code')) return {results: [{customer: {id: input.customerId, currencyCode: 'USD', timeZone: 'America/Los_Angeles'}}]};
    if (input.query.includes('FROM ad_group_ad') && input.query.includes('metrics.impressions')) {
      return {results: withRows ? [standardFixture.row] : []};
    }
    if (input.query.includes('FROM asset_group') && input.query.includes('metrics.impressions')) {
      return {results: withRows ? [pmaxFixture.row] : []};
    }
    if (input.query.includes('metrics.conversions')) return {results: []};
    throw new Error('unexpected Google query');
  };
}

test('R6-D4-D contract reuses E5 and keeps Dataset V2 closed', () => {
  assert.equal(contract.status, 'PASS_PRODUCTION_READ_ONLY_ACCEPTANCE');
  assert.equal(contract.reuse.e5_standard_query_and_mapper, true);
  assert.equal(contract.reuse.e5_performance_max_query_and_mapper, true);
  assert.equal(contract.reuse.e5_time_fx, true);
  assert.equal(contract.reuse.new_metric_contract, false);
  assert.equal(contract.acceptance.normal_data_sources_ui_changed, false);
  assert.equal(contract.provider_contact, true);
  assert.equal(contract.dataset_v2_write, false);
  assert.equal(contract.live_acceptance.status, 'PASS');
  assert.equal(contract.live_acceptance.selected_account_count, 3);
  assert.equal(contract.live_acceptance.verified_row_count, 0);
  assert.equal(contract.live_acceptance.provider_result_status, 'empty');
  assert.equal(contract.live_acceptance.standard_and_performance_max_verified, true);
  assert.equal(contract.live_acceptance.time_fx_verified, true);
  assert.equal(contract.live_acceptance.supabase_postcheck, 'PASS');
  assert.equal(liveEvidence.result, 'PASS_PRODUCTION_READ_ONLY_ACCEPTANCE');
  assert.equal(liveEvidence.merchant_acceptance.dataset_v2_writes, 0);
  assert.equal(liveEvidence.supabase_postcheck.google_dataset_rows, 0);
  assert.equal(liveEvidence.supabase_postcheck.google_synthetic_rows, 0);
  assert.equal(liveEvidence.supabase_postcheck.browser_grant_count, 0);
  assert.equal(liveEvidence.safeguards.production_activation, false);
});

test('completed E5 Standard and PMax mappers support workspace authority without legacy user ownership', () => {
  const customer = standardFixture.customer;
  const standard = mapGoogleStandardAd(standardFixture.row, {workspaceId: WORKSPACE, customer});
  const pmax = mapGooglePmaxAssetGroup(pmaxFixture.row, {workspaceId: WORKSPACE, customer: pmaxFixture.customer});
  for (const mapped of [standard, pmax]) {
    assert.equal(mapped.row.identity.workspace_id, WORKSPACE);
    assert.equal(mapped.row.identity.user_id, null);
    assert.equal(mapped.row.provenance.synthetic, false);
  }
});

test('Google workspace runner covers all three selected accounts and both completed E5 branches without persistence', async () => {
  const calls = [];
  const fxDates = [];
  const runner = createGoogleWorkspaceRunner({
    search: providerSearch(calls),
    resolveFxRate: async (source, target, {rateDate}) => {
      fxDates.push({source, target, rateDate});
      return {fx_rate: 40, fx_rate_date: rateDate, fx_provider: 'approved-test'};
    },
    now,
  });
  const result = await runner({authority, connection, reportingCurrency: 'TRY'});
  assert.deepEqual(result.checked_account_ids, accounts.map(account => account.id));
  assert.equal(result.standard_row_count, 3);
  assert.equal(result.pmax_row_count, 3);
  assert.equal(result.rows.length, 6);
  assert.equal(result.provider_result_status, 'non_empty');
  assert.equal(calls.length, 15);
  assert.deepEqual(new Set(calls.map(call => call.customerId)), new Set(accounts.map(account => account.id)));
  assert.ok(calls.every(call => call.loginCustomerId === accounts.find(account => account.id === call.customerId).login_customer_id));
  assert.ok(result.rows.every(row => row.identity.workspace_id === WORKSPACE && row.identity.user_id === null));
  assert.ok(result.rows.every(row => row.currency.source_currency === 'USD' && row.currency.target_currency === 'TRY'));
  assert.deepEqual(fxDates, accounts.map(() => ({source: 'USD', target: 'TRY', rateDate: '2026-08-30'})));
});

test('Google read-only preflight returns only aggregate evidence and accepts provider-verified empty results', async () => {
  const calls = [];
  let lifecycleCalls = 0;
  const preflight = createGoogleReadOnlyPreflight({
    connectionStore: {resolveConnected: async input => {assert.deepEqual(input, {authority, provider: 'google_ads'}); return connection;}},
    settingsStore: {resolveReportingCurrency: async () => ({reportingCurrency: 'TRY', currencyVersion: 4})},
    tokenLifecycle: {run: async ({connection: current, operation}) => {lifecycleCalls += 1; return {value: await operation(current), connection: current};}},
    search: providerSearch(calls, {withRows: false}),
    resolveFxRate: async (_source, _target, {rateDate}) => ({fx_rate: 40, fx_rate_date: rateDate, fx_provider: 'approved-test'}),
    now,
  });
  const result = await preflight.execute(authority);
  assert.deepEqual(result, {
    status: 'PASS_R6_D4_D_GOOGLE_READ_ONLY_PREFLIGHT', provider_result_status: 'empty', selected_account_count: 3,
    row_count: 0, standard_row_count: 0, pmax_row_count: 0, empty_provider_result: true,
    customer_metadata_verified: true, standard_and_pmax_verified: true, time_fx_verified: true,
    provider_date_strategy: 'previous_closed_business_date_per_customer_timezone', dataset_v2_write: false,
    production_activation: false, currency_version: 4,
  });
  assert.equal(lifecycleCalls, 1);
  assert.equal(calls.length, 15);
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes('secret-access'), false);
  assert.ok(accounts.every(account => !JSON.stringify(result).includes(account.id)));
});

test('Google preflight route is Shopify-session-bound and ignores caller workspace and date', async () => {
  const routes = {};
  const app = {get() {}, post: (route, handler) => {routes[route] = handler;}};
  let received;
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async ({session_token}) => {assert.equal(session_token, 'session'); return authority;},
    selection: {status() {}, list() {}, complete() {}},
    googlePreflight: {execute: async input => {received = input; return {status: 'PASS_R6_D4_D_GOOGLE_READ_ONLY_PREFLIGHT'};}},
  });
  const res = {set() {}, status(code) {this.code = code; return this;}, json(body) {this.body = body; return body;}};
  await routes['/api/shopify/providers/google_ads/runtime/preflight']({
    get: () => 'Bearer session', body: {workspace_id: 'attacker', provider_date: '2099-01-01'},
  }, res);
  assert.deepEqual(received, authority);
  assert.equal(res.code, 200);
});

test('Google search uses the selected manager context and safely classifies unauthorized responses', async () => {
  const requests = [];
  const client = createGoogleAdsSearchClient({
    developerToken: 'developer-secret', apiVersion: 'v25',
    fetchImpl: async (url, options) => {
      requests.push({url, options});
      return {ok: false, status: 401, headers: {get: name => name === 'request-id' ? 'safe-request-id' : null}, json: async () => ({error: {status: 'UNAUTHENTICATED'}})};
    },
  });
  await assert.rejects(
    client({accessToken: 'access-secret', customerId: '1111111111', loginCustomerId: '9999999999', query: 'SELECT customer.id FROM customer'}),
    error => error.code === 'GOOGLE_ACCESS_TOKEN_INVALID' && error.upstreamStatus === 401 && error.upstreamCode === 'UNAUTHENTICATED'
  );
  assert.equal(requests[0].options.headers['login-customer-id'], '9999999999');
  assert.equal(requests[0].options.headers['developer-token'], 'developer-secret');
  assert.match(requests[0].options.headers.authorization, /^Bearer /);
});

test('Google acceptance surface is hidden unless the explicit operator parameter is used', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true, providerAvailability: {google_ads: true}});
  assert.match(html, /id="r6d4-google-acceptance" hidden/);
  assert.match(html, /params\.get\("acceptance"\) === "r6d4-google"/);
  assert.match(html, /\/api\/shopify\/providers\/google_ads\/runtime\/preflight/);
  assert.match(html, /Dataset V2 writes: 0/);
});

test('previous-date helper selects the closed day before the provider business date', () => {
  assert.equal(previousDate('2026-03-01'), '2026-02-28');
});


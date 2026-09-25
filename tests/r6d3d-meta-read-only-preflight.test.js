'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createMetaWorkspaceRunner, previousClosedBusinessDate } = require('../src/providers/meta/workspace-runner');
const { createMetaReadOnlyPreflight } = require('../src/providers/meta/read-only-preflight');
const { registerShopifyAdAccountRoutes } = require('../src/routes/shopify-ad-account-routes');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e4-meta/e4-t1-provider-fixture.json'), 'utf8'));
const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const now = () => new Date('2026-09-25T12:00:00.000Z');
const connection = {
  provider: 'meta', status: 'connected', accessToken: 'secret-token', accessTokenExpiresAt: '2026-11-01T00:00:00.000Z',
  grantedScopes: ['ads_read'], sourceCurrency: 'USD',
  selectedAccounts: [{ id: fixture.account.id, name: fixture.account.name, currency: fixture.account.currency }],
};
const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d3d-meta-read-only-preflight-v1.json'), 'utf8'));

function transport({ insights = [fixture.insight] } = {}) {
  return async url => ({
    ok: true,
    status: 200,
    json: async () => String(url).includes('/me/adaccounts') ? { data: [fixture.account] } : { data: insights },
  });
}

test('R6-D3-D derives the previous closed date from each Meta account timezone', () => {
  assert.equal(previousClosedBusinessDate(new Date('2026-09-25T02:00:00Z'), 'America/New_York'), '2026-09-23');
  assert.equal(previousClosedBusinessDate(new Date('2026-09-25T22:00:00Z'), 'Asia/Tokyo'), '2026-09-25');
});

test('R6-D3-D contract reuses E4 and keeps all production data movement closed', () => {
  assert.equal(contract.status, 'PASS_REPOSITORY_ONLY_PRODUCTION_ACCEPTANCE_PENDING');
  assert.equal(contract.reuse.e4_client, true);
  assert.equal(contract.reuse.e4_mapper, true);
  assert.equal(contract.reuse.new_metric_contract, false);
  assert.equal(contract.acceptance.normal_data_sources_ui_changed, false);
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.equal(contract.next_gate, 'R6-D3-D_META_PRODUCTION_READ_ONLY_ACCEPTANCE');
});

test('Meta workspace runner reuses E4 mapping with workspace authority and no persistence', async () => {
  const fxDates = [];
  const runner = createMetaWorkspaceRunner({
    transport: transport(),
    resolveFxRate: async (_source, _target, { rateDate }) => {
      fxDates.push(rateDate);
      return { fx_rate: 40, fx_rate_date: rateDate, fx_provider: 'approved-test' };
    },
    now,
  });
  const result = await runner({ authority, connection, reportingCurrency: 'TRY', request: {} });
  assert.equal(result.provider_result_status, 'non_empty');
  assert.deepEqual(result.checked_account_ids, [fixture.account.id]);
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].identity.workspace_id, WORKSPACE);
  assert.equal(result.rows[0].identity.user_id, null);
  assert.equal(result.rows[0].identity.platform_account_id, fixture.account.id);
  assert.equal(result.rows[0].currency.source_currency, 'USD');
  assert.equal(result.rows[0].currency.target_currency, 'TRY');
  assert.equal(result.rows[0].time.source_timezone, fixture.account.timezone_name);
  assert.deepEqual(fxDates, ['2026-09-24']);
});

test('Meta read-only preflight returns redacted aggregate evidence and writes nothing', async () => {
  const preflight = createMetaReadOnlyPreflight({
    connectionStore: { resolveConnected: async input => { assert.deepEqual(input, { authority, provider: 'meta' }); return connection; } },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 4 }) },
    transport: transport({ insights: [] }),
    resolveFxRate: async (_source, _target, { rateDate }) => ({ fx_rate: 40, fx_rate_date: rateDate, fx_provider: 'approved-test' }),
    now,
  });
  const result = await preflight.execute(authority);
  assert.deepEqual(result, {
    status: 'PASS_R6_D3_D_META_READ_ONLY_PREFLIGHT', provider_result_status: 'empty', selected_account_count: 1,
    row_count: 0, empty_provider_result: true, account_api_verified: true, insights_verified: true, time_fx_verified: true,
    provider_date_strategy: 'previous_closed_business_date_per_account_timezone', dataset_v2_write: false,
    production_activation: false, currency_version: 4,
  });
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes('secret-token'), false);
  assert.equal(JSON.stringify(result).includes(fixture.account.id), false);
});

test('Meta preflight fails closed for expired grants and redacts provider failures', async () => {
  const make = (activeConnection, selectedTransport = transport()) => createMetaReadOnlyPreflight({
    connectionStore: { resolveConnected: async () => activeConnection },
    settingsStore: { resolveReportingCurrency: async () => ({ reportingCurrency: 'TRY', currencyVersion: 4 }) },
    transport: selectedTransport,
    resolveFxRate: async () => ({ fx_rate: 40, fx_rate_date: '2026-09-23', fx_provider: 'approved-test' }),
    now,
  });
  await assert.rejects(make({ ...connection, accessTokenExpiresAt: '2026-09-01T00:00:00Z' }).execute(authority), error => error.code === 'META_PREFLIGHT_REAUTHORIZE' && error.status === 409);
  await assert.rejects(make(connection, async () => { throw new Error('provider-secret-body'); }).execute(authority), error => error.code === 'META_PREFLIGHT_FAILED' && !error.message.includes('provider-secret-body'));
});

test('Meta preflight route is Shopify-session-bound and ignores caller tenant fields', async () => {
  const routes = {};
  const app = { get() {}, post: (route, handler) => { routes[route] = handler; } };
  let received;
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: { status() {}, list() {}, complete() {} },
    metaPreflight: { execute: async input => { received = input; return { status: 'PASS_R6_D3_D_META_READ_ONLY_PREFLIGHT' }; } },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/meta/runtime/preflight']({ get: () => 'Bearer session', body: { workspace_id: 'attacker', provider_date: '2099-01-01' } }, res);
  assert.deepEqual(received, authority);
  assert.equal(res.code, 200);
});

test('Meta acceptance surface stays hidden outside its explicit operator parameter', () => {
  const html = renderEmbeddedPlatforms({ clientId: 'client', providerOAuthEnabled: true, providerAvailability: { meta: true } });
  assert.match(html, /id="r6d3-meta-acceptance" hidden/);
  assert.match(html, /params\.get\("acceptance"\) === "r6d3-meta"/);
  assert.match(html, /\/api\/shopify\/providers\/meta\/runtime\/preflight/);
  assert.match(html, /Dataset V2 writes: 0/);
});

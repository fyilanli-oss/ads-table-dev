'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  CONFIRMATION,
  acceptanceDateWindow,
  createMetaControlledDatasetAcceptance,
} = require('../src/providers/meta/controlled-dataset-acceptance');
const { registerShopifyAdAccountRoutes } = require('../src/routes/shopify-ad-account-routes');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e4-meta/e4-t1-provider-fixture.json'), 'utf8'));
const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = Object.freeze({ authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' });
const now = () => new Date('2026-09-25T12:00:00.000Z');
const connection = Object.freeze({
  provider: 'meta', status: 'connected', accessToken: 'secret-token', accessTokenExpiresAt: '2026-11-01T00:00:00.000Z',
  grantedScopes: ['ads_read'], selectedAccounts: [{ id: fixture.account.id, name: fixture.account.name, currency: fixture.account.currency }],
});
const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d3e-meta-controlled-dataset-acceptance-v1.json'), 'utf8'));

function acceptance({ insights = [fixture.insight], existingRows = [], failAt = null } = {}) {
  const calls = [];
  const fail = stage => { if (failAt === stage) throw new Error('secret--must-not-leak'); };
  const service = createMetaControlledDatasetAcceptance({
    connectionStore: { resolveConnected: async input => { calls.push(['connection', input]); fail('connection'); return connection; } },
    settingsStore: { resolveReportingCurrency: async input => { calls.push(['currency', input]); fail('currency'); return { reportingCurrency: 'TRY', currencyVersion: 7 }; } },
    transport: async url => {
      calls.push(['provider', String(url)]);
      fail('provider');
      return { ok: true, status: 200, json: async () => String(url).includes('/me/adaccounts') ? { data: [fixture.account] } : { data: insights } };
    },
    resolveFxRate: async (_source, _target, input) => { calls.push(['fx', input]); fail('fx'); return { fx_rate: 40, fx_rate_date: input.rateDate, fx_provider: 'approved-test' }; },
    repository: {
      readCanonicalRawFacts: async input => { calls.push(['read', input]); fail('guard'); return existingRows; },
      upsertCanonicalRawFacts: async rows => { calls.push(['write', rows]); fail('write'); return rows; },
    },
    now,
  });
  return { calls, service };
}

test('R6-D3-E uses a conservative three-day guard window for account timezones', () => {
  assert.deepEqual(acceptanceDateWindow(now), { from: '2026-09-23', to: '2026-09-25' });
});

test('R6-D3-E contract records the verified-empty production acceptance without inventing writes', () => {
  assert.equal(contract.status, 'PASS_PRODUCTION_VERIFIED_EMPTY');
  assert.equal(contract.reuse.e4_client_mapper_time_fx, true);
  assert.equal(contract.reuse.new_metric_contract, false);
  assert.equal(contract.controls.verified_empty_writes_synthetic_rows, false);
  assert.equal(contract.production_deployed, true);
  assert.equal(contract.provider_contact, true);
  assert.equal(contract.dataset_v2_write, false);
  assert.equal(contract.live_acceptance.attempted, 0);
  assert.equal(contract.live_acceptance.persisted, 0);
  assert.equal(contract.live_acceptance.synthetic_rows_written, 0);
  assert.equal(contract.live_acceptance.non_empty_physical_upsert_observed, false);
  assert.equal(contract.live_acceptance.supabase_postcheck, 'PASS');
  assert.equal(contract.next_gate, 'R6-D4_GOOGLE_ADS_ANALYST_BRIEF');
});

test('R6-D3-E requires exact action-time confirmation before provider or Dataset access', async () => {
  const { calls, service } = acceptance();
  await assert.rejects(service.execute(authority, 'wrong'), error => error.code === 'META_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED' && error.status === 409);
  assert.deepEqual(calls, []);
});

test('R6-D3-E persists only verified workspace Meta rows and returns aggregate evidence', async () => {
  const { calls, service } = acceptance();
  const result = await service.execute(authority, CONFIRMATION);
  assert.deepEqual(result, {
    status: 'PASS_R6_D3_E_META_DATASET_WRITE', attempted: 1, persisted: 1,
    empty_provider_result: false, provider_result_status: 'non_empty', selected_account_count: 1,
    provider_date_strategy: 'previous_closed_business_date_per_account_timezone', production_activation: false, currency_version: 7,
  });
  const write = calls.find(([name]) => name === 'write')[1];
  assert.equal(write.length, 1);
  assert.equal(write[0].identity.workspace_id, WORKSPACE);
  assert.equal(write[0].identity.user_id, null);
  assert.equal(write[0].identity.platform, 'meta');
  assert.equal(write[0].currency.source_currency, 'USD');
  assert.equal(write[0].currency.target_currency, 'TRY');
  assert.equal(Object.hasOwn(result, 'rows'), false);
  assert.equal(JSON.stringify(result).includes(fixture.account.id), false);
  assert.equal(JSON.stringify(result).includes('secret-token'), false);
});

test('R6-D3-E preserves verified-empty without synthetic rows', async () => {
  const { calls, service } = acceptance({ insights: [] });
  const result = await service.execute(authority, CONFIRMATION);
  assert.equal(result.attempted, 0);
  assert.equal(result.persisted, 0);
  assert.equal(result.empty_provider_result, true);
  assert.deepEqual(calls.find(([name]) => name === 'write')[1], []);
});

test('R6-D3-E blocks an existing workspace/account acceptance before provider contact', async () => {
  const { calls, service } = acceptance({ existingRows: [{ id: 'existing' }] });
  await assert.rejects(service.execute(authority, CONFIRMATION), error => error.code === 'META_DATASET_ACCEPTANCE_ALREADY_EXECUTED' && error.status === 409);
  assert.equal(calls.filter(([name]) => name === 'read').length, 1);
  assert.equal(calls.some(([name]) => name === 'provider'), false);
  assert.equal(calls.some(([name]) => name === 'write'), false);
});

test('R6-D3-E returns only allowlisted failure stages and redacts underlying details', async () => {
  for (const [failAt, stage] of [['connection', 'CONNECTION'], ['currency', 'CURRENCY'], ['guard', 'DATASET_GUARD'], ['provider', 'PROVIDER_FACTS'], ['fx', 'PROVIDER_FACTS'], ['write', 'DATASET_PERSISTENCE']]) {
    const { service } = acceptance({ failAt });
    await assert.rejects(service.execute(authority, CONFIRMATION), error => {
      assert.equal(error.code, `META_DATASET_ACCEPTANCE_FAILED_${stage}`);
      assert.equal(error.status, 503);
      assert.doesNotMatch(`${error.code}:${error.message}`, /secret-|token/i);
      return true;
    });
  }
});

test('Meta controlled acceptance route is Shopify-session-bound and forwards only confirmation', async () => {
  const routes = {};
  const app = { get() {}, post: (route, handler) => { routes[route] = handler; } };
  let received;
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => { assert.equal(session_token, 'session'); return authority; },
    selection: { status() {}, list() {}, complete() {} },
    metaDatasetAcceptance: { execute: async (...input) => { received = input; return { status: 'PASS_R6_D3_E_META_DATASET_WRITE', attempted: 0, persisted: 0 }; } },
  });
  const res = { set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; } };
  await routes['/api/shopify/providers/meta/runtime/acceptance']({ get: () => 'Bearer session', body: { confirmation: CONFIRMATION, workspace_id: 'attacker', provider_date: '2099-01-01' } }, res);
  assert.deepEqual(received, [authority, CONFIRMATION]);
  assert.equal(res.code, 200);
});

test('Meta controlled Dataset acceptance stays inside the explicit hidden operator surface', () => {
  const html = renderEmbeddedPlatforms({ clientId: 'client', providerOAuthEnabled: true, providerAvailability: { meta: true } });
  assert.match(html, /id="r6d3-meta-acceptance" hidden/);
  assert.match(html, /id="r6d3-meta-dataset-run"/);
  assert.match(html, /RUN_R6_D3_E_META_WRITE/);
  assert.match(html, /\/api\/shopify\/providers\/meta\/runtime\/acceptance/);
});

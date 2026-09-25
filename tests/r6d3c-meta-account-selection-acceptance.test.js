'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {createAdAccountSelection} = require('../src/shopify/ad-account-selection');
const {registerShopifyAdAccountRoutes} = require('../src/routes/shopify-ad-account-routes');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const contract = JSON.parse(read('contracts/r6d3c-meta-account-selection-acceptance-v1.json'));
const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session',
});

function response() {
  return {
    statusCode: 0,
    body: null,
    headers: {},
    set(name, value) { this.headers[name] = value; return this; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
  };
}

function routeFixture(selection, authenticateEmbedded = async () => authority) {
  const routes = {};
  registerShopifyAdAccountRoutes({
    get(route, handler) { routes[`GET ${route}`] = handler; },
    post(route, handler) { routes[`POST ${route}`] = handler; },
  }, {authenticateEmbedded, selection});
  return routes;
}

test('R6-D3-C reuses the canonical R7-A2 runtime without activating data movement', () => {
  assert.equal(contract.reuse_decision.new_account_selection_runtime_required, false);
  assert.equal(contract.reuse_decision.legacy_server_dashboard_runtime_reactivated, false);
  assert.equal(contract.authority.browser_fields_are_authority, false);
  assert.equal(contract.authority.server_refetch_on_save, true);
  assert.deepEqual(contract.cardinality, {
    minimum: 1,
    maximum: 3,
    duplicates_allowed: false,
    database_constraint: 'workspace_provider_connected_account_count',
  });
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.deepEqual(contract.production_acceptance_preparation, {
    status: 'PREPARED_LOCALLY_NOT_EXECUTED',
    runbook: 'docs/R6D3C_META_PRODUCTION_MERCHANT_ACCEPTANCE_RUNBOOK.md',
    read_only_preflight: 'docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_PREFLIGHT.sql',
    read_only_postcheck: 'docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_POSTCHECK.sql',
    requires_real_merchant_session: true,
    requires_explicit_production_approval: true,
    screenshots_required: false,
    final_pass_requires_legacy_meta_count_unchanged: true,
  });
});

test('Meta list and save stay Shopify-session-bound and save only re-fetched provider accounts', async () => {
  const writes = [];
  let discoveries = 0;
  const providerAccounts = [
    {id: 'act_1', name: 'Provider One', currency: 'USD'},
    {id: 'act_2', name: 'Provider Two', currency: 'EUR'},
  ];
  const selection = createAdAccountSelection({
    store: {
      readStatus: async () => ({status: 'pending_account_selection', selected_accounts: []}),
      readPendingProvider: async () => ({status: 'pending_account_selection', accessToken: 'server-token', connection_version: 7}),
      completeAccountSelection: async input => writes.push(input),
    },
    discoverByProvider: {
      meta: async token => {
        assert.equal(token, 'server-token');
        discoveries += 1;
        return providerAccounts;
      },
      google_ads: async () => [],
    },
  });
  const routes = routeFixture(selection);

  const list = response();
  await routes['GET /api/shopify/providers/meta/accounts']({
    get: name => name === 'authorization' ? 'Bearer verified-session' : '',
  }, list);
  assert.equal(list.statusCode, 200);
  assert.deepEqual(list.body.accounts, providerAccounts);

  const save = response();
  await routes['POST /api/shopify/providers/meta/accounts/select']({
    body: {account_ids: ['act_2'], name: 'Browser Fake', currency: 'TRY', workspace_id: 'attacker'},
    get: name => name === 'authorization' ? 'Bearer verified-session' : '',
  }, save);
  assert.equal(save.statusCode, 200);
  assert.deepEqual(save.body, {status: 'connected', accounts: [providerAccounts[1]]});
  assert.equal(discoveries, 2);
  assert.deepEqual(writes, [{authority, provider: 'meta', version: 7, accounts: [providerAccounts[1]]}]);
});

test('Meta account endpoints reject missing Shopify authority and invalid cardinality before persistence', async () => {
  let writes = 0;
  const available = [1, 2, 3, 4].map(number => ({id: `act_${number}`, name: `Account ${number}`, currency: 'USD'}));
  const selection = createAdAccountSelection({
    store: {
      readPendingProvider: async () => ({status: 'pending_account_selection', accessToken: 'server-token', connection_version: 2}),
      completeAccountSelection: async () => { writes += 1; },
    },
    discoverByProvider: {meta: async () => available, google_ads: async () => []},
  });
  const unauthorizedRoutes = routeFixture(selection, async () => { throw new Error('invalid'); });
  const unauthorized = response();
  await unauthorizedRoutes['GET /api/shopify/providers/meta/accounts']({get: () => ''}, unauthorized);
  assert.equal(unauthorized.statusCode, 401);
  assert.deepEqual(unauthorized.body, {code: 'SHOPIFY_SESSION_REQUIRED'});

  const routes = routeFixture(selection);
  for (const account_ids of [[], ['act_1', 'act_2', 'act_3', 'act_4'], ['act_1', 'act_1'], ['missing']]) {
    const rejected = response();
    await routes['POST /api/shopify/providers/meta/accounts/select']({
      body: {account_ids},
      get: () => 'Bearer verified-session',
    }, rejected);
    assert.equal(rejected.statusCode, 400);
  }
  assert.equal(writes, 0);
});

test('Shopify-native modal and Supabase constraints preserve pending then 1-3 connected accounts', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true, providerAvailability: {meta: true}});
  assert.match(html, /id="meta-account-modal" heading="Select Meta account"/);
  assert.match(html, /id="meta-choice"[^>]+multiple/);
  assert.match(html, /Array\.isArray\(choices\.values\)/);
  assert.match(html, /selectedIds\.length < 1 \|\| selectedIds\.length > 3/);
  assert.match(html, /modal\.showOverlay\(\)/);
  assert.match(html, /result\.status === 'pending_account_selection'.*loadAccounts\(\)/);

  const migration = read('supabase/migrations/20260924120453_add_workspace_provider_selected_accounts.sql');
  assert.match(migration, /status <> 'pending_account_selection' or jsonb_array_length\(selected_accounts\) = 0/);
  assert.match(migration, /provider in \('meta', 'google_ads'\).*jsonb_array_length\(selected_accounts\) between 1 and 3/s);

  const store = read('src/providers/workspace-provider-connection-store.js');
  assert.match(store, /\.eq\('connection_version', version\)\.eq\('status', 'pending_account_selection'\)/);
  assert.match(store, /status: 'connected'[\s\S]*selected_accounts: verifiedAccounts/);
});

test('production acceptance runbook requires a real modal, persistence and unchanged data movement', () => {
  const runbook = read('docs/R6D3C_META_PRODUCTION_MERCHANT_ACCEPTANCE_RUNBOOK.md');
  assert.match(runbook, /hesap seçim modalının kendiliğinden açıldığı/);
  assert.match(runbook, /en az 1, en fazla 3/);
  assert.match(runbook, /bir kez yenilenir/);
  assert.match(runbook, /Dataset V2 Meta satırı, aktif Meta schedule ve açık Meta job sayısı `0`/);
  assert.match(runbook, /Legacy Meta kayıt sayısı değişir/);
  assert.match(runbook, /Ekran görüntüsü zorunlu değildir/);
  assert.match(runbook, /not deployed, not executed, production merchant acceptance pending/);
});

test('production preflight is read-only and blocks an already-started canonical Meta flow', () => {
  const sql = read('docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_PREFLIGHT.sql');
  assert.doesNotMatch(sql, /\b(insert|update|delete|alter|drop|truncate|create)\b/i);
  assert.match(sql, /canonical_meta_count = 0/);
  assert.match(sql, /dataset_v2_meta_count = 0/);
  assert.match(sql, /active_legacy_meta_schedule_count = 0/);
  assert.match(sql, /open_legacy_meta_job_count = 0/);
  assert.match(sql, /browser_grants\.grant_count = 0/);
  assert.match(sql, /legacy_meta_connection_baseline/);
});

test('production postcheck proves a secure 1-3 account connection without activating data movement', () => {
  const sql = read('docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_POSTCHECK.sql');
  assert.doesNotMatch(sql, /\b(insert|update|delete|alter|drop|truncate|create)\b/i);
  assert.match(sql, /jsonb_array_length\(selected_accounts\) not between 1 and 3/);
  assert.match(sql, /active_account_id is distinct from selected_accounts->0->>'id'/);
  assert.match(sql, /refresh_token_envelope is not null/);
  assert.match(sql, /'ads_read' = any\(granted_scopes\)/);
  assert.match(sql, /invalid_selected_account_count = 0/);
  assert.match(sql, /dataset_v2_meta_count = 0/);
  assert.match(sql, /legacy_meta_connection_after/);
});

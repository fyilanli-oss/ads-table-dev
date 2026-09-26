'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {CONFIRMATION, createGoogleDisconnect} = require('../src/shopify/google-disconnect');
const {createCanonicalWorkspaceProviderConnectionStore} = require('../src/providers/workspace-provider-connection-store');
const {registerShopifyAdAccountRoutes} = require('../src/routes/shopify-ad-account-routes');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session',
});

test('Google Ads disconnect requires exact action-time confirmation before reading state', async () => {
  let reads = 0;
  const service = createGoogleDisconnect({store: {
    resolveConnected: async () => { reads += 1; },
    disconnectGoogle: async () => {},
  }});
  await assert.rejects(service.execute(authority, 'yes'), error =>
    error.code === 'GOOGLE_DISCONNECT_CONFIRMATION_REQUIRED' && error.status === 409);
  assert.equal(reads, 0);
});

test('Google Ads disconnect revokes provider grant before local cleanup', async () => {
  const calls = [], transport = [];
  const service = createGoogleDisconnect({
    store: {
      resolveConnected: async input => { calls.push(['resolve', input]); return {version: 11, accessToken: 'access-secret', refreshToken: 'refresh-secret'}; },
      disconnectGoogle: async input => calls.push(['disconnect', input]),
    },
    fetchImpl: async (url, options) => { transport.push([url, options]); return {ok: true, status: 200, text: async () => ''}; },
  });
  const outcome = await service.execute(authority, CONFIRMATION);
  assert.equal(outcome.provider_grant_revoked, true);
  assert.equal(new URLSearchParams(transport[0][1].body).get('token'), 'refresh-secret');
  assert.deepEqual(calls, [['resolve', {authority, provider: 'google_ads'}], ['disconnect', {authority, version: 11}]]);
});

test('failed Google revoke preserves local connection', async () => {
  let writes = 0;
  const service = createGoogleDisconnect({
    store: {resolveConnected: async () => ({version: 11, refreshToken: 'secret'}), disconnectGoogle: async () => { writes += 1; }},
    fetchImpl: async () => ({ok: false, status: 500, text: async () => ''}),
  });
  await assert.rejects(service.execute(authority, CONFIRMATION), error => error.code === 'GOOGLE_REVOKE_FAILED');
  assert.equal(writes, 0);
});

test('already disconnected Google Ads is idempotent', async () => {
  let writes = 0;
  const service = createGoogleDisconnect({store: {
    resolveConnected: async () => null,
    disconnectGoogle: async () => { writes += 1; },
  }});
  const result = await service.execute(authority, CONFIRMATION);
  assert.equal(result.status, 'not_connected');
  assert.equal(result.provider_grant_revoked, false);
  assert.equal(writes, 0);
});

test('canonical Google Ads cleanup is workspace-scoped, provider-scoped and optimistic', async () => {
  const calls = [];
  const query = {
    update(row) { calls.push(['update', row]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    select(columns) { calls.push(['select', columns]); return query; },
    async maybeSingle() { return {data: {status: 'disconnected', connection_version: 13}, error: null}; },
  };
  const store = createCanonicalWorkspaceProviderConnectionStore({
    client: {from(table) { calls.push(['from', table]); return query; }},
    vault: {encrypt() {}, decrypt() {}},
    now: () => new Date('2026-09-26T12:00:00.000Z'),
  });
  await store.disconnectGoogle({authority, version: 12});
  const update = calls.find(([name]) => name === 'update')[1];
  assert.equal(update.status, 'disconnected');
  assert.equal(update.active_account_id, null);
  assert.equal(update.source_currency, null);
  assert.equal(update.access_token_envelope, null);
  assert.equal(update.refresh_token_envelope, null);
  assert.equal(update.access_token_expires_at, null);
  assert.deepEqual(update.granted_scopes, []);
  assert.deepEqual(update.selected_accounts, []);
  assert.equal(update.account_verified_at, null);
  assert.equal(update.connection_version, 13);
  assert.equal(Object.hasOwn(update, 'connected_at'), false);
  assert.deepEqual(calls.filter(([name]) => name === 'eq'), [
    ['eq', 'workspace_id', authority.workspace_id],
    ['eq', 'provider', 'google_ads'],
    ['eq', 'connection_version', 12],
    ['eq', 'status', 'connected'],
  ]);
});

test('session-bound Google Ads disconnect route delegates exact confirmation', async () => {
  const routes = new Map();
  const app = {
    get(route, handler) { routes.set(`GET ${route}`, handler); },
    post(route, handler) { routes.set(`POST ${route}`, handler); },
  };
  const calls = [];
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async input => { calls.push(['auth', input]); return authority; },
    selection: {status() {}, list() {}, complete() {}},
    googleDisconnect: {execute: async (resolved, confirmation) => {
      calls.push(['disconnect', resolved, confirmation]);
      return {status: 'not_connected', provider_grant_revoked: false};
    }},
  });
  const handler = routes.get('POST /api/shopify/providers/google_ads/accounts/disconnect');
  const response = {
    statusCode: null, body: null,
    set() {}, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; },
  };
  await handler({get: () => 'Bearer session-token', body: {confirmation: CONFIRMATION}}, response);
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.provider_grant_revoked, false);
  assert.deepEqual(calls, [
    ['auth', {session_token: 'session-token'}],
    ['disconnect', authority, CONFIRMATION],
  ]);
});

test('embedded UI exposes Google Ads warning modal, Cancel and provider revoke action', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  const ui = fs.readFileSync(path.join(__dirname, '../src/shopify/ad-account-ui.js'), 'utf8');
  const runtime = fs.readFileSync(path.join(__dirname, '../src/shopify/runtime.js'), 'utf8');
  assert.match(html, /id="google_ads-disconnect-modal" heading="Disconnect Google Ads\?"/);
  assert.match(html, /id="google_ads-disconnect-confirm"/);
  assert.match(html, /commandFor="google_ads-disconnect-modal" command="--hide">Cancel/);
  assert.match(ui, /DISCONNECT_GOOGLE_ADS/);
  assert.match(runtime, /createGoogleDisconnect\(\{store: connectionStore, fetchImpl\}\)/);
});

test('contract forbids global Google revoke and preserves unrelated state', () => {
  const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/r6d4f-google-independent-disconnect-v1.json'), 'utf8'));
  assert.equal(contract.disconnect_policy.provider_global_revoke_allowed, false);
  assert.equal(contract.disconnect_policy.provider_api_contact_allowed, false);
  assert.equal(contract.supabase.migration_required, false);
  assert.deepEqual(contract.must_preserve, [
    'workspace_reporting_currency',
    'meta_connection',
    'klaviyo_connection',
    'historical_dataset_v2_rows',
    'legacy_google_history',
    'parked_google_sheets_state',
    'parked_ga4_state',
  ]);
});

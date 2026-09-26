'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {CONFIRMATION, createMetaDisconnect} = require('../src/shopify/meta-disconnect');
const {createCanonicalWorkspaceProviderConnectionStore} = require('../src/providers/workspace-provider-connection-store');
const {registerShopifyAdAccountRoutes} = require('../src/routes/shopify-ad-account-routes');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session',
});

test('Meta disconnect requires action-time confirmation before reading the connection', async () => {
  let reads = 0;
  const service = createMetaDisconnect({store: {
    resolveConnected: async () => { reads += 1; },
    disconnectMeta: async () => {},
  }});
  await assert.rejects(service.execute(authority, 'yes'), error =>
    error.code === 'META_DISCONNECT_CONFIRMATION_REQUIRED' && error.status === 409);
  assert.equal(reads, 0);
});

test('Meta user grant is revoked before canonical credentials are cleared', async () => {
  const calls = [];
  const service = createMetaDisconnect({
    graphVersion: 'v20.0',
    fetchImpl: async (url, options) => {
      calls.push(['revoke', url, options]);
      return {ok: true, status: 200, json: async () => ({success: true})};
    },
    store: {
      resolveConnected: async input => {
        calls.push(['resolve', input]);
        return {status: 'connected', version: 7, accessToken: 'meta-user-secret'};
      },
      disconnectMeta: async input => calls.push(['disconnect', input]),
    },
  });

  assert.deepEqual(await service.execute(authority, CONFIRMATION), {
    status: 'not_connected', historical_data_preserved: true,
  });
  assert.deepEqual(calls[0], ['resolve', {authority, provider: 'meta'}]);
  assert.equal(calls[1][1].origin + calls[1][1].pathname, 'https://graph.facebook.com/v20.0/me/permissions');
  assert.equal(calls[1][1].searchParams.get('access_token'), 'meta-user-secret');
  assert.equal(calls[1][2].method, 'DELETE');
  assert.equal(calls[1][2].redirect, 'error');
  assert.deepEqual(calls[2], ['disconnect', {authority, version: 7}]);
});

test('provider revoke failure leaves the canonical Meta connection active', async () => {
  let writes = 0;
  const service = createMetaDisconnect({
    fetchImpl: async () => ({ok: false, status: 503, json: async () => ({error: {message: 'secret provider detail'}})}),
    store: {
      resolveConnected: async () => ({status: 'connected', version: 2, accessToken: 'meta-user-secret'}),
      disconnectMeta: async () => { writes += 1; },
    },
  });
  await assert.rejects(service.execute(authority, CONFIRMATION), error => error.code === 'META_REVOKE_FAILED');
  assert.equal(writes, 0);
});

test('already disconnected Meta is idempotent and does not call the provider', async () => {
  let providerCalls = 0;
  const service = createMetaDisconnect({
    fetchImpl: async () => { providerCalls += 1; },
    store: {resolveConnected: async () => null, disconnectMeta: async () => {}},
  });
  assert.deepEqual(await service.execute(authority, CONFIRMATION), {
    status: 'not_connected', historical_data_preserved: true,
  });
  assert.equal(providerCalls, 0);
});

test('canonical Meta cleanup is workspace-scoped, optimistic and credential-complete', async () => {
  const calls = [];
  const query = {
    update(row) { calls.push(['update', row]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    select(columns) { calls.push(['select', columns]); return query; },
    async maybeSingle() { return {data: {status: 'disconnected', connection_version: 9}, error: null}; },
  };
  const store = createCanonicalWorkspaceProviderConnectionStore({
    client: {from(table) { calls.push(['from', table]); return query; }},
    vault: {encrypt() {}, decrypt() {}},
    now: () => new Date('2026-09-26T09:00:00.000Z'),
  });
  await store.disconnectMeta({authority, version: 8});
  const update = calls.find(([name]) => name === 'update')[1];
  assert.equal(update.status, 'disconnected');
  assert.equal(update.access_token_envelope, null);
  assert.equal(update.refresh_token_envelope, null);
  assert.deepEqual(update.granted_scopes, []);
  assert.deepEqual(update.selected_accounts, []);
  assert.equal(update.account_verified_at, null);
  assert.equal(update.connection_version, 9);
  assert.equal(Object.hasOwn(update, 'connected_at'), false);
  assert.deepEqual(calls.filter(([name]) => name === 'eq'), [
    ['eq', 'workspace_id', authority.workspace_id],
    ['eq', 'provider', 'meta'],
    ['eq', 'connection_version', 8],
    ['eq', 'status', 'connected'],
  ]);
});

test('session-bound Meta disconnect route delegates only the exact confirmation', async () => {
  const routes = new Map();
  const app = {
    get(path, handler) { routes.set(`GET ${path}`, handler); },
    post(path, handler) { routes.set(`POST ${path}`, handler); },
  };
  const calls = [];
  registerShopifyAdAccountRoutes(app, {
    authenticateEmbedded: async input => { calls.push(['auth', input]); return authority; },
    selection: {status() {}, list() {}, complete() {}},
    metaDisconnect: {execute: async (resolved, confirmation) => {
      calls.push(['disconnect', resolved, confirmation]);
      return {status: 'not_connected', historical_data_preserved: true};
    }},
  });
  const handler = routes.get('POST /api/shopify/providers/meta/accounts/disconnect');
  assert.equal(typeof handler, 'function');
  const response = {
    statusCode: null, body: null,
    set() {}, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; },
  };
  await handler({get: () => 'Bearer session-token', body: {confirmation: CONFIRMATION}}, response);
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, {status: 'not_connected', historical_data_preserved: true});
  assert.deepEqual(calls, [
    ['auth', {session_token: 'session-token'}],
    ['disconnect', authority, CONFIRMATION],
  ]);
});

test('embedded UI exposes Meta warning modal without enabling Google disconnect', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  const ui = fs.readFileSync(path.join(__dirname, '../src/shopify/ad-account-ui.js'), 'utf8');
  const runtime = fs.readFileSync(path.join(__dirname, '../src/shopify/runtime.js'), 'utf8');
  assert.match(html, /id="meta-disconnect-modal" heading="Disconnect Meta\?"/);
  assert.match(html, /id="meta-disconnect-confirm"/);
  assert.doesNotMatch(html, /id="google_ads-disconnect-modal"/);
  assert.match(ui, /confirmation: 'DISCONNECT_META'/);
  assert.match(ui, /connection remains active/);
  assert.match(runtime, /createMetaDisconnect/);
});

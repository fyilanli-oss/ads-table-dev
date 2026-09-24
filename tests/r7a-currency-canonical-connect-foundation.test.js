'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createWorkspaceSettingsStore, SUPPORTED_REPORTING_CURRENCIES } = require('../src/providers/workspace-settings-store');
const { createCanonicalWorkspaceProviderConnectionStore } = require('../src/providers/workspace-provider-connection-store');
const { registerShopifyWorkspaceSettingsRoutes } = require('../src/routes/shopify-workspace-settings-routes');
const { PROVIDERS } = require('../src/routes/shopify-provider-oauth-routes');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

const root = path.resolve(__dirname, '..');
const authority = Object.freeze({authority: 'server_resolved_workspace', workspace_id: '11111111-1111-4111-8111-111111111111', source: 'shopify_verified_session'});

function query(result, calls) {
  const value = {
    insert(row) { calls.push(['insert', row]); return value; },
    update(row) { calls.push(['update', row]); return value; },
    select(columns) { calls.push(['select', columns]); return value; },
    eq(field, expected) { calls.push(['eq', field, expected]); return value; },
    in(field, expected) { calls.push(['in', field, expected]); return value; },
    maybeSingle: async () => result,
  };
  return value;
}

test('R7-A contract follows the Execution Plan and preserves the R6-D/R7-B gates', () => {
  const contract = JSON.parse(fs.readFileSync(path.join(root, 'contracts/r7a-currency-canonical-connect-foundation-v1.json'), 'utf8'));
  assert.deepEqual(contract.active_providers, ['meta', 'google_ads', 'klaviyo']);
  assert.deepEqual(contract.parked_providers, ['tiktok', 'pinterest']);
  assert.equal(contract.reporting_currency.source, 'merchant_selected');
  assert.equal(contract.reporting_currency.shopify_currency_allowed, false);
  assert.equal(contract.connection_authority.table, 'workspace_provider_connections');
  assert.equal(contract.connection_authority.legacy_shopify_table_write_allowed, false);
  assert.equal(contract.next_gate, 'R7-A2_verified_account_selection_for_meta_google_and_repository_acceptance');
});

test('workspace currency is explicitly selected once and never inferred from Shopify', async () => {
  const calls = [];
  const store = createWorkspaceSettingsStore({client: {from(table) { calls.push(['from', table]); return query({data: {reporting_currency: 'TRY', reporting_currency_source: 'merchant_selected', reporting_currency_version: 1}, error: null}, calls); }}});
  const result = await store.selectReportingCurrency({authority, currency: 'try'});
  assert.deepEqual(result, {status: 'configured', reporting_currency: 'TRY', reporting_currency_version: 1});
  const inserted = calls.find(([name]) => name === 'insert')[1];
  assert.deepEqual(inserted, {workspace_id: authority.workspace_id, reporting_currency: 'TRY', reporting_currency_source: 'merchant_selected'});
  assert.equal(JSON.stringify(inserted).includes('shop'), false);
  assert.equal(SUPPORTED_REPORTING_CURRENCIES.includes('TRY'), true);
});

test('workspace settings routes derive workspace only from the verified Shopify session', async () => {
  const routes = {};
  const app = {get(pathname, handler) { routes[`GET ${pathname}`] = handler; }, post(pathname, handler) { routes[`POST ${pathname}`] = handler; }};
  let received;
  registerShopifyWorkspaceSettingsRoutes(app, {
    authenticateEmbedded: async ({session_token}) => { assert.equal(session_token, 'session'); return authority; },
    settings: {
      readStatus: async input => ({status: 'currency_required', authority: input}),
      selectReportingCurrency: async input => (received = input, {status: 'configured', reporting_currency: input.currency, reporting_currency_version: 1}),
    },
  });
  const res = {set() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return body; }};
  await routes['POST /api/shopify/workspace/reporting-currency']({get: () => 'Bearer session', body: {currency: 'TRY', workspace_id: 'attacker'}}, res);
  assert.deepEqual(received, {authority, currency: 'TRY'});
  assert.equal(res.code, 200);
});

test('embedded OAuth writes a pending row only to the canonical workspace connection table', async () => {
  const calls = [];
  const vault = {encrypt(value) { return value ? {version: 1, keyId: 'k', iv: 'i', tag: 't', ciphertext: value} : null; }, decrypt() { return null; }};
  const client = {from(table) { calls.push(['from', table]); return query({data: {workspace_id: authority.workspace_id, provider: 'klaviyo', status: 'pending_account_selection'}, error: null}, calls); }};
  const store = createCanonicalWorkspaceProviderConnectionStore({client, vault, now: () => new Date('2026-09-23T17:00:00.000Z')});
  await store.writeFromOAuthTransaction({transaction: {surface: 'shopify_embedded', user_id: null, return_target: '/shopify/app/platforms', workspace_id: authority.workspace_id, shop_id: 'shop-a', provider: 'klaviyo'}, accessToken: 'access'});
  assert.equal(calls[0][1], 'workspace_provider_connections');
  assert.equal(calls.some(call => call.includes('shopify_workspace_provider_connections')), false);
});

test('Data Sources is currency-first, modal-first, and excludes parked providers from OAuth', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  assert.match(html, /id="currency-setup"/);
  assert.match(html, /id="provider-sections" hidden/);
  assert.match(html, /commandFor="meta-connect-modal" command="--show"/);
  assert.match(html, /id="klaviyo-account-modal" heading="Finish Klaviyo setup"/);
  assert.match(html, /data-provider="meta"/);
  assert.doesNotMatch(html, /data-provider="tiktok"|data-provider="pinterest"/);
  assert.deepEqual(PROVIDERS, ['meta', 'google_ads', 'klaviyo']);
  assert.doesNotMatch(html, /<iframe/i);
});

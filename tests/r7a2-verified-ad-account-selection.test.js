'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createMetaAccountDiscovery, createGoogleAdsAccountDiscovery, createAdAccountSelection} = require('../src/shopify/ad-account-selection');
const {selectedAccounts} = require('../src/providers/workspace-provider-connection-store');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const authority = {authority: 'server_resolved_workspace', workspace_id: '11111111-1111-4111-8111-111111111111', source: 'shopify_verified_session'};
const response = (status, body) => ({status, ok: status >= 200 && status < 300, json: async () => body});

test('R7-A v2 contract completes repository scope while preserving live gates', () => {
  const contract = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../contracts/r7a-currency-canonical-connect-foundation-v2.json'), 'utf8'));
  assert.equal(contract.status, 'R7_A_KLAVIYO_MERCHANT_ACCEPTANCE_PASS_R6_D_NEXT');
  assert.equal(contract.verified_account_selection.browser_account_fields_are_authority, false);
  assert.match(contract.verified_account_selection.meta, /1-3 accounts/);
  assert.match(contract.verified_account_selection.google_ads, /1-3 accounts/);
  assert.match(contract.verified_account_selection.klaviyo, /Exactly 1 account/);
  assert.equal(contract.ui.app_home_model, 'shopify_admin_embedded_developer_hosted_iframe');
  assert.equal(contract.ui.nested_iframe, false);
  assert.equal(contract.production_state.live_provider_contact_performed, true);
  assert.equal(contract.production_state.canonical_connection_created, true);
  assert.deepEqual(contract.blocked_until_acceptance, ['R7-B', 'R3-C']);
});

test('additive migration stores verified selections with provider-specific cardinality', () => {
  const sql = fs.readFileSync(path.resolve(__dirname, '../supabase/migrations/20260924120453_add_workspace_provider_selected_accounts.sql'), 'utf8');
  assert.match(sql, /add column selected_accounts jsonb not null default '\[\]'::jsonb/);
  assert.match(sql, /provider in \('meta', 'google_ads'\).*jsonb_array_length\(selected_accounts\) between 1 and 3/s);
  assert.match(sql, /provider = 'klaviyo'.*jsonb_array_length\(selected_accounts\) = 1/s);
  assert.doesNotMatch(sql, /grant .*anon|grant .*authenticated/i);
});

test('Meta account discovery returns only provider-supplied identity and currency', async () => {
  const discover = createMetaAccountDiscovery({fetchImpl: async (url, options) => {
    assert.match(url, /\/me\/adaccounts\?fields=id,name,currency,account_status/);
    assert.equal(options.headers.Authorization, 'Bearer access');
    return response(200, {data: [{id: 'act_1', name: 'Verified Meta', currency: 'USD', account_status: 1}]});
  }});
  assert.deepEqual(await discover('access'), [{id: 'act_1', name: 'Verified Meta', currency: 'USD'}]);
});

test('Google Ads discovery uses accessible customers and verified customer_client currency', async () => {
  const calls = [];
  const discover = createGoogleAdsAccountDiscovery({developerToken: 'developer', apiVersion: 'v25', fetchImpl: async (url, options) => {
    calls.push({url, options});
    if (url.endsWith('customers:listAccessibleCustomers')) return response(200, {resourceNames: ['customers/123']});
    return response(200, [{results: [{customerClient: {id: '456', descriptiveName: 'Verified Google', currencyCode: 'EUR', timeZone: 'Europe/Berlin', manager: false, level: 1}}]}]);
  }});
  assert.deepEqual(await discover('access'), [{id: '456', name: 'Verified Google', currency: 'EUR'}]);
  assert.equal(calls.every(call => call.options.headers['developer-token'] === 'developer'), true);
  assert.equal(calls[1].options.method, 'POST');
});

test('selection re-fetches provider accounts and ignores caller name/currency/tenant claims', async () => {
  const writes = [];
  const store = {
    readStatus: async () => ({status: 'pending_account_selection'}),
    readPendingProvider: async () => ({status: 'pending_account_selection', accessToken: 'access', connection_version: 3}),
    completeAccountSelection: async input => writes.push(input),
  };
  const selection = createAdAccountSelection({store, discoverByProvider: {meta: async () => [{id: 'act_1', name: 'Verified', currency: 'USD'}], google_ads: async () => []}});
  await assert.rejects(selection.complete(authority, 'meta', {account_id: 'other'}), error => error.code === 'INVALID_ACCOUNT');
  await assert.rejects(selection.complete(authority, 'meta', {account_ids: ['other']}), error => error.code === 'INVALID_ACCOUNT');
  const result = await selection.complete(authority, 'meta', {account_ids: ['act_1'], name: 'Fake', currency: 'TRY', workspace_id: 'attacker'});
  assert.deepEqual(result, {status: 'connected', accounts: [{id: 'act_1', name: 'Verified', currency: 'USD'}]});
  assert.deepEqual(writes[0], {authority, provider: 'meta', version: 3, accounts: [{id: 'act_1', name: 'Verified', currency: 'USD'}]});
});

test('Meta and Google require between one and three distinct verified ad accounts', async () => {
  const available = [1,2,3,4].map(number => ({id: `act_${number}`, name: `Account ${number}`, currency: 'USD'}));
  const writes = [];
  const selection = createAdAccountSelection({
    store: {
      readPendingProvider: async () => ({status: 'pending_account_selection', accessToken: 'access', connection_version: 4}),
      completeAccountSelection: async input => writes.push(input),
    },
    discoverByProvider: {meta: async () => available, google_ads: async () => available},
  });
  await assert.rejects(selection.complete(authority, 'meta', {account_ids: []}), error => error.code === 'ACCOUNT_SELECTION_LIMIT');
  await assert.rejects(selection.complete(authority, 'meta', {account_ids: available.map(item => item.id)}), error => error.code === 'ACCOUNT_SELECTION_LIMIT');
  await assert.rejects(selection.complete(authority, 'meta', {account_ids: ['act_1', 'act_2', 'act_2']}), error => error.code === 'INVALID_ACCOUNT');
  const result = await selection.complete(authority, 'meta', {account_ids: ['act_1', 'act_2', 'act_3']});
  assert.equal(result.accounts.length, 3);
  assert.equal(writes[0].accounts.length, 3);
});

test('canonical store allows 1-3 ad accounts and exactly one Klaviyo account', () => {
  const accounts = [1,2,3].map(number => ({id: `account-${number}`, name: `Account ${number}`, currency: number === 2 ? 'EUR' : 'USD'}));
  assert.equal(selectedAccounts('meta', accounts).length, 3);
  assert.equal(selectedAccounts('google_ads', accounts.slice(0, 2)).length, 2);
  assert.equal(selectedAccounts('klaviyo', accounts.slice(0, 1)).length, 1);
  assert.throws(() => selectedAccounts('klaviyo', accounts.slice(0, 2)), /INVALID_ACCOUNT_SELECTION_COUNT/);
  assert.throws(() => selectedAccounts('meta', [accounts[0], accounts[0]]), /INVALID_ACCOUNT_SELECTION_COUNT/);
});

test('Meta and Google account selection are Shopify-native modals and parked providers remain excluded', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  assert.match(html, /id="meta-account-modal" heading="Select Meta account"/);
  assert.match(html, /id="google_ads-account-modal" heading="Select Google Ads account"/);
  assert.match(html, /Select between 1 and 3 accounts returned by Meta/);
  assert.match(html, /id="meta-choice"[^>]+multiple/);
  assert.doesNotMatch(html, /Open setup|Close setup/);
  assert.doesNotMatch(html, /id="meta-disconnect-modal"|id="google_ads-disconnect-modal"/);
  assert.match(html, /id="klaviyo-disconnect-modal"/);
  assert.doesNotMatch(html, /data-provider="tiktok"|data-provider="pinterest"/);
});

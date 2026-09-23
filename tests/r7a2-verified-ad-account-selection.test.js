'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createMetaAccountDiscovery, createGoogleAdsAccountDiscovery, createAdAccountSelection} = require('../src/shopify/ad-account-selection');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const authority = {authority: 'server_resolved_workspace', workspace_id: '11111111-1111-4111-8111-111111111111', source: 'shopify_verified_session'};
const response = (status, body) => ({status, ok: status >= 200 && status < 300, json: async () => body});

test('R7-A v2 contract completes repository scope while preserving live gates', () => {
  const contract = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../contracts/r7a-currency-canonical-connect-foundation-v2.json'), 'utf8'));
  assert.equal(contract.status, 'R7_A_REPOSITORY_COMPLETE_LIVE_ACCEPTANCE_PENDING');
  assert.equal(contract.verified_account_selection.browser_account_fields_are_authority, false);
  assert.equal(contract.production_state.live_provider_contact_performed, false);
  assert.deepEqual(contract.blocked_until_acceptance, ['R6-D', 'R7-B', 'R3-C']);
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
  const result = await selection.complete(authority, 'meta', {account_id: 'act_1', name: 'Fake', currency: 'TRY', workspace_id: 'attacker'});
  assert.deepEqual(result, {status: 'connected', active_account_id: 'act_1', account_name: 'Verified', currency: 'USD'});
  assert.deepEqual(writes[0], {authority, provider: 'meta', version: 3, account: {id: 'act_1', name: 'Verified', currency: 'USD'}});
});

test('Meta and Google account selection are Shopify-native modals and parked providers remain excluded', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  assert.match(html, /id="meta-account-modal" heading="Select Meta account"/);
  assert.match(html, /id="google_ads-account-modal" heading="Select Google Ads account"/);
  assert.match(html, /Only an account returned by Meta can be connected/);
  assert.doesNotMatch(html, /data-provider="tiktok"|data-provider="pinterest"/);
});

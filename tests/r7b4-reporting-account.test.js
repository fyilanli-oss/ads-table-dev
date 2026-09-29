'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {createAdAccountSelection} = require('../src/shopify/ad-account-selection');
const {createCanonicalWorkspaceProviderConnectionStore} = require('../src/providers/workspace-provider-connection-store');
const {renderEmbeddedPlatforms} = require('../src/shopify/embedded-app-home');

const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session',
});
const accounts = Object.freeze([
  {id: 'account-1', name: 'Account One', currency: 'USD'},
  {id: 'account-2', name: 'Account Two', currency: 'EUR'},
]);

test('R7-B4 migration separates reporting preference from connected accounts', () => {
  const sql = fs.readFileSync(path.resolve(__dirname, '../supabase/migrations/20260929110000_r7b4_add_reporting_account.sql'), 'utf8');
  assert.match(sql, /add column reporting_account_id text/);
  assert.match(sql, /add column reporting_account_name text/);
  assert.match(sql, /add column reporting_account_selected_at timestamptz/);
  assert.match(sql, /provider in \('meta', 'google_ads'\)/);
  assert.match(sql, /selected_accounts @> jsonb_build_array/);
  assert.match(sql, /provider = 'klaviyo'.*reporting_account_id is null/s);
  assert.doesNotMatch(sql, /grant .*anon|grant .*authenticated/i);
  assert.doesNotMatch(sql, /delete from|truncate/i);
});

test('Reporting Account change revalidates provider access and preserves the connected set', async () => {
  const writes = [];
  const store = {
    resolveConnected: async () => ({
      provider: 'meta',
      status: 'connected',
      accessToken: 'access',
      selectedAccounts: accounts,
      version: 7,
    }),
    updateReportingAccount: async value => writes.push(value),
  };
  const selection = createAdAccountSelection({
    store,
    discoverByProvider: {meta: async token => {
      assert.equal(token, 'access');
      return accounts;
    }, google_ads: async () => []},
  });
  const result = await selection.reporting(authority, 'meta', {account_id: 'account-2', name: 'caller value ignored'});
  assert.deepEqual(result, {
    status: 'connected',
    reporting_account: {id: 'account-2', name: 'Account Two'},
    connected_account_count: 2,
  });
  assert.deepEqual(writes, [{
    authority,
    provider: 'meta',
    version: 7,
    account: accounts[1],
  }]);
  assert.deepEqual(store.resolveConnected && accounts.map(account => account.id), ['account-1', 'account-2']);
});

test('Reporting Account rejects unconnected and no-longer-accessible accounts without writing', async () => {
  let writes = 0;
  const store = {
    resolveConnected: async () => ({
      provider: 'meta',
      status: 'connected',
      accessToken: 'access',
      selectedAccounts: accounts,
      version: 4,
    }),
    updateReportingAccount: async () => { writes += 1; },
  };
  const selection = createAdAccountSelection({
    store,
    discoverByProvider: {meta: async () => [accounts[0]], google_ads: async () => []},
  });
  await assert.rejects(selection.reporting(authority, 'meta', {account_id: 'not-connected'}), error => error.code === 'REPORTING_ACCOUNT_NOT_SELECTED');
  await assert.rejects(selection.reporting(authority, 'meta', {account_id: 'account-2'}), error => error.code === 'REPORTING_ACCOUNT_ACCESS_NOT_VERIFIED');
  assert.equal(writes, 0);
});

test('canonical store changes only reporting fields under optimistic connection protection', async () => {
  const calls = [];
  const query = {
    update(row) { calls.push(['update', row]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    select(columns) { calls.push(['select', columns]); return query; },
    async maybeSingle() {
      return {data: {
        reporting_account_id: 'account-2',
        reporting_account_name: 'Account Two',
        reporting_account_selected_at: '2026-09-29T11:00:00.000Z',
        connection_version: 8,
      }, error: null};
    },
  };
  const client = {from(table) { calls.push(['from', table]); return query; }};
  const vault = {
    encrypt(value) { return {ciphertext: value}; },
    decrypt(value) { return value.ciphertext; },
  };
  const store = createCanonicalWorkspaceProviderConnectionStore({
    client,
    vault,
    now: () => new Date('2026-09-29T11:00:00.000Z'),
  });
  await store.updateReportingAccount({
    authority,
    provider: 'meta',
    version: 7,
    account: accounts[1],
  });
  const update = calls.find(([operation]) => operation === 'update')[1];
  assert.deepEqual(Object.keys(update).sort(), [
    'connection_version',
    'reporting_account_id',
    'reporting_account_name',
    'reporting_account_selected_at',
    'updated_at',
  ]);
  assert.equal(update.reporting_account_id, 'account-2');
  assert.equal(update.connection_version, 8);
  assert.deepEqual(calls.filter(([operation]) => operation === 'eq'), [
    ['eq', 'workspace_id', authority.workspace_id],
    ['eq', 'provider', 'meta'],
    ['eq', 'connection_version', 7],
    ['eq', 'status', 'connected'],
  ]);
});

test('Settings exposes Reporting Account only for Meta and Google Ads', () => {
  const html = renderEmbeddedPlatforms({clientId: 'client', providerOAuthEnabled: true});
  for (const provider of ['meta', 'google_ads']) {
    assert.match(html, new RegExp(`id="${provider}-reporting-action"`));
    assert.match(html, new RegExp(`id="${provider}-reporting-modal"`));
    assert.match(html, new RegExp(`id="${provider}-reporting-choice"`));
    assert.match(html, new RegExp(`id="${provider}-reporting-save"`));
  }
  assert.doesNotMatch(html, /id="klaviyo-reporting-action"/);
  assert.match(html, /does not reconnect .* remove connected accounts or delete historical data/);
  assert.match(html, /request\('\/reporting', \{account_id: accountId\}\)/);
});

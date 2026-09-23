'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { createWorkspaceSettingsStore } = require('../src/providers/workspace-settings-store');
const { createWorkspaceProviderRuntime } = require('../src/providers/workspace-provider-runtime');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: WORKSPACE,
  source: 'shopify_verified_session',
});

function settingsClient(data, error = null) {
  const filters = [];
  const query = {
    select() { return this; },
    eq(key, value) { filters.push([key, value]); return this; },
    async maybeSingle() { return { data, error }; },
  };
  return { filters, client: { from(table) { assert.equal(table, 'workspace_settings'); return query; } } };
}

test('workspace settings accepts only merchant-selected reporting currency', async () => {
  const fixture = settingsClient({
    reporting_currency: 'try',
    reporting_currency_source: 'merchant_selected',
    reporting_currency_version: 2,
  });
  const store = createWorkspaceSettingsStore({ client: fixture.client });
  assert.deepEqual(await store.resolveReportingCurrency(authority), {
    reportingCurrency: 'TRY', currencyVersion: 2, source: 'merchant_selected',
  });
  assert.deepEqual(fixture.filters, [['workspace_id', WORKSPACE]]);
});

test('workspace settings fails closed when currency is absent or inferred', async () => {
  const missing = createWorkspaceSettingsStore({ client: settingsClient(null).client });
  await assert.rejects(missing.resolveReportingCurrency(authority), /WORKSPACE_REPORTING_CURRENCY_REQUIRED/);
  const inferred = createWorkspaceSettingsStore({ client: settingsClient({
    reporting_currency: 'USD', reporting_currency_source: 'shopify', reporting_currency_version: 1,
  }).client });
  await assert.rejects(inferred.resolveReportingCurrency(authority), /WORKSPACE_REPORTING_CURRENCY_SOURCE_INVALID/);
});

function runtimeFixture(overrides = {}) {
  const calls = [];
  const connection = overrides.connection === undefined ? {
    provider: 'meta', status: 'connected', activeAccountId: 'act-1', sourceCurrency: 'USD',
    accessToken: 'secret', refreshToken: 'refresh', version: 1,
  } : overrides.connection;
  const runtime = createWorkspaceProviderRuntime({
    resolveAuthority: async input => { calls.push(['authority', input]); return authority; },
    connectionStore: { resolveConnected: async input => { calls.push(['connection', input]); return connection; } },
    settingsStore: { resolveReportingCurrency: async input => { calls.push(['currency', input]); return { reportingCurrency: 'TRY', currencyVersion: 3 }; } },
    runners: overrides.runners || { meta: async context => { calls.push(['runner', context]); return { rows: [{ identity: {} }] }; } },
    datasetRuntime: { write: async input => { calls.push(['write', input]); return input.rows; } },
  });
  return { calls, runtime };
}

test('workspace provider runtime resolves authority, connection and currency before Dataset V2 write', async () => {
  const { calls, runtime } = runtimeFixture();
  const result = await runtime.run({ authority_input: { session_token: 'opaque' }, provider: 'meta', request: { date: '2026-09-23' } });
  assert.deepEqual(result, {
    provider: 'meta', workspace_id: WORKSPACE, attempted: 1, persisted: 1,
    empty_provider_result: false, currency_version: 3,
  });
  assert.deepEqual(calls.map(([name]) => name), ['authority', 'connection', 'currency', 'runner', 'write']);
  assert.equal(calls[3][1].reportingCurrency, 'TRY');
  assert.equal(calls[4][1].authority_input.session_token, 'opaque');
  assert.equal(Object.hasOwn(result, 'accessToken'), false);
  assert.equal(Object.hasOwn(result, 'refreshToken'), false);
});

test('workspace provider runtime rejects missing connection, parked providers and caller tenant claims', async () => {
  const missing = runtimeFixture({ connection: null }).runtime;
  await assert.rejects(missing.run({ authority_input: { session_token: 'opaque' }, provider: 'meta' }), /CANONICAL_PROVIDER_CONNECTION_REQUIRED/);
  const active = runtimeFixture().runtime;
  await assert.rejects(active.run({ authority_input: { session_token: 'opaque' }, provider: 'tiktok' }), /PROVIDER_NOT_ACTIVE_IN_R6/);
  await assert.rejects(active.run({ authority_input: { workspace_id: WORKSPACE }, provider: 'meta' }), /not tenant authority/);
  await assert.rejects(active.run({ authority_input: { session_token: 'opaque' }, provider: 'meta', request: { workspace_id: WORKSPACE } }), /not tenant authority/);
});

test('workspace provider runtime cannot silently run an unregistered provider implementation', async () => {
  const runtime = runtimeFixture({ runners: {} }).runtime;
  await assert.rejects(runtime.run({ authority_input: { session_token: 'opaque' }, provider: 'meta' }), /PROVIDER_RUNTIME_NOT_READY/);
});


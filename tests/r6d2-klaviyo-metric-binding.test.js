'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createKlaviyoMetricBinding } = require('../src/providers/klaviyo/metric-binding');
const { createCanonicalWorkspaceProviderConnectionStore } = require('../src/providers/workspace-provider-connection-store');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const connection = { provider: 'klaviyo', status: 'connected', activeAccountId: 'account-1', accessToken: 'secret', version: 7 };
const candidates = [
  { id: 'metric-shopify', name: 'Placed Order', integration_name: 'Shopify', integration_category: 'Ecommerce' },
  { id: 'metric-woo', name: 'Placed Order', integration_name: 'WooCommerce', integration_category: 'Ecommerce' },
];

function fixture(items = candidates) {
  const writes = [];
  const binding = createKlaviyoMetricBinding({
    connectionStore: {
      resolveConnected: async () => connection,
      bindKlaviyoConversionMetric: async input => { writes.push(input); return { connection_version: 8 }; },
    },
    providerClient: { fetchPlacedOrderMetricCandidates: async ({ accessToken }) => { assert.equal(accessToken, 'secret'); return items; } },
  });
  return { binding, writes };
}

test('metric discovery is read-only and preserves provider integration provenance', async () => {
  const { binding, writes } = fixture();
  const result = await binding.discover(authority);
  assert.equal(result.candidate_count, 2);
  assert.equal(result.connection_write, false);
  assert.deepEqual(result.candidates, candidates);
  assert.deepEqual(writes, []);
});

test('metric selection revalidates the provider candidate and binds it to account and connection version', async () => {
  const { binding, writes } = fixture();
  const result = await binding.select(authority, { metric_id: 'metric-woo' });
  assert.equal(result.status, 'KLAVIYO_CONVERSION_METRIC_BOUND');
  assert.equal(result.dataset_v2_write, false);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].accountId, 'account-1');
  assert.equal(writes[0].version, 7);
  assert.deepEqual(writes[0].metric, candidates[1]);
});

test('metric selection never guesses on missing or unknown candidates', async () => {
  await assert.rejects(fixture([]).binding.discover(authority), error => error.code === 'KLAVIYO_CONVERSION_METRIC_NOT_FOUND');
  await assert.rejects(fixture().binding.select(authority, { metric_id: 'unknown' }), error => error.code === 'KLAVIYO_CONVERSION_METRIC_SELECTION_INVALID');
});

test('conversion metric migration is additive, workspace-scoped and commerce-channel independent', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'supabase', 'migrations', '20260925072801_add_workspace_provider_conversion_metric.sql'), 'utf8');
  assert.match(sql, /alter table public\.workspace_provider_connections/);
  assert.match(sql, /conversion_metric_id text/);
  assert.match(sql, /provider = 'klaviyo'/);
  assert.match(sql, /lower\(btrim\(conversion_metric_name\)\) = 'placed order'/);
  assert.doesNotMatch(sql, /shop_id|woocommerce_id|insert into|update public\.workspace_provider_connections/i);
});

test('metric binding production SQL gates are read-only before/after and rollback blocks populated bindings', () => {
  const sql = name => fs.readFileSync(path.join(__dirname, '..', 'docs', 'security', 'sql', name), 'utf8');
  const preflight = sql('R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT.sql');
  const postcheck = sql('R6D2C1_KLAVIYO_METRIC_BINDING_POSTCHECK.sql');
  const rollback = sql('R6D2C1_KLAVIYO_METRIC_BINDING_ROLLBACK.sql');
  assert.doesNotMatch(preflight, /\b(insert|update|delete|alter|drop|truncate)\b/i);
  assert.doesNotMatch(postcheck, /\b(insert|update|delete|alter|drop|truncate)\b/i);
  assert.match(preflight, /target_columns\.existing_count = 0/);
  assert.match(postcheck, /target_columns\.existing_count = 5/);
  assert.match(postcheck, /binding_state\.bound_metric_count = 0/);
  assert.match(rollback, /R6D2C1_ROLLBACK_BLOCKED_METRIC_BINDINGS_EXIST/);
  assert.match(rollback, /drop column if exists conversion_metric_id/);
});

test('canonical store binds a metric only to the same connected account and connection version', async () => {
  const calls = [];
  const query = {
    update(value) { calls.push(['update', value]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    select(value) { calls.push(['select', value]); return query; },
    async maybeSingle() { return { data: { connection_version: 8 }, error: null }; },
  };
  const store = createCanonicalWorkspaceProviderConnectionStore({
    client: { from(table) { calls.push(['from', table]); return query; } },
    vault: { encrypt() {}, decrypt() {} },
    now: () => new Date('2026-09-25T09:00:00.000Z'),
  });
  await store.bindKlaviyoConversionMetric({
    authority, version: 7, accountId: 'account-1', metric: candidates[0],
  });
  assert.deepEqual(calls.filter(call => call[0] === 'eq'), [
    ['eq', 'workspace_id', WORKSPACE], ['eq', 'provider', 'klaviyo'],
    ['eq', 'status', 'connected'], ['eq', 'active_account_id', 'account-1'],
    ['eq', 'connection_version', 7],
  ]);
  const update = calls.find(call => call[0] === 'update')[1];
  assert.equal(update.conversion_metric_id, 'metric-shopify');
  assert.equal(update.connection_version, 8);
});

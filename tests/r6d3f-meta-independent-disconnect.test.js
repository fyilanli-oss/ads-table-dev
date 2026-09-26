'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(path.join(root, 'contracts/r6d3f-meta-independent-disconnect-v1.json'), 'utf8'));
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

test('R6-D3-F freezes an independent Meta lifecycle before implementation', () => {
  assert.equal(contract.status, 'PASS_CONTRACT_ONLY_IMPLEMENTATION_GATE');
  assert.equal(contract.provider, 'meta');
  assert.equal(contract.current_gap.legacy_reference_has_meta_disconnect, true);
  assert.equal(contract.current_gap.embedded_meta_disconnect_route_exists, false);
  assert.equal(contract.current_gap.canonical_meta_disconnect_store_operation_exists, false);
  assert.equal(contract.schema_migration_required, false);
  assert.equal(contract.production_provider_contact, false);
  assert.equal(contract.supabase_mutation, false);
  assert.equal(contract.next_gate, 'R6-D3-F_META_DISCONNECT_IMPLEMENTATION');
});

test('R6-D3-F is revoke-first, optimistic and isolated from other providers and analytics', () => {
  assert.equal(contract.provider_revoke.app_access_token_allowed, false);
  assert.equal(contract.provider_revoke.revoke_failure_changes_local_connection, false);
  assert.equal(contract.canonical_disconnect.optimistic_connection_version_required, true);
  assert.equal(contract.canonical_disconnect.provider_scope, 'meta_only');
  assert.equal(contract.independence_invariants.klaviyo_connection_changed, false);
  assert.equal(contract.independence_invariants.google_ads_connection_changed, false);
  assert.equal(contract.independence_invariants.historical_analytics_deleted, false);
  assert.equal(contract.independence_invariants.dataset_write_triggered, false);
});

test('Execution Plan keeps Meta open until Disconnect and clean Reconnect acceptance', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /R6-D3-F Meta bağımsız Disconnect yaşam döngüsü — Contract PASS \/ implementation gate/);
  assert.match(plan, /Her provider Connect → Connected → Disconnect → temiz Reconnect zincirini bağımsız tamamlamadan kendi kabulü kapanmaz/);
  assert.match(plan, /Sıradaki kapı R6-D3-F uygulamasıdır; Google Ads henüz başlamaz/);
});

test('The documented reference and embedded gap are present in the current code', () => {
  const server = read('server.js');
  const dashboard = read('public/dashboard.html');
  const embedded = read('src/shopify/embedded-app-home.js');
  const routes = read('src/routes/shopify-ad-account-routes.js');
  const store = read('src/providers/workspace-provider-connection-store.js');
  assert.match(server, /graph\.facebook\.com\/\$\{META_GRAPH_VERSION\}\/me\/permissions/);
  assert.match(dashboard, /\/api\/platform\/meta\/disconnect/);
  assert.match(embedded, /id === "klaviyo".*disconnect-modal/);
  assert.doesNotMatch(routes, /providers\/meta\/accounts\/disconnect/);
  assert.doesNotMatch(store, /disconnectMeta/);
});

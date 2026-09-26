'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(path.join(root, 'contracts/r6d3f-meta-independent-disconnect-v1.json'), 'utf8'));
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

test('R6-D3-F preserves the decision baseline and records live Disconnect and reconnect acceptance', () => {
  assert.equal(contract.status, 'PASS_PRODUCTION_DISCONNECT_AND_CLEAN_RECONNECT');
  assert.equal(contract.provider, 'meta');
  assert.equal(contract.decision_baseline_gap.legacy_reference_has_meta_disconnect, true);
  assert.equal(contract.decision_baseline_gap.embedded_meta_disconnect_route_exists, false);
  assert.equal(contract.decision_baseline_gap.canonical_meta_disconnect_store_operation_exists, false);
  assert.equal(contract.repository_implementation.status, 'PASS');
  assert.equal(contract.repository_implementation.production_deployment_verified, true);
  assert.equal(contract.repository_implementation.live_disconnect_verified, true);
  assert.equal(contract.repository_implementation.clean_reconnect_verified, true);
  assert.equal(contract.live_acceptance.result, 'PASS');
  assert.equal(contract.live_acceptance.clean_reconnect_account_count, 1);
  assert.equal(contract.live_acceptance.dataset_v2_rows_after, 0);
  assert.equal(fs.existsSync(path.join(root, contract.live_acceptance.evidence)), true);
  assert.equal(contract.schema_migration_required, false);
  assert.equal(contract.production_provider_contact, true);
  assert.equal(contract.supabase_mutation, true);
  assert.equal(contract.next_gate, 'R6-D4_GOOGLE_ADS_ANALYST_BRIEF');
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
  assert.match(plan, /R6-D3-F Meta bağımsız Disconnect yaşam döngüsü — Production Disconnect and clean Reconnect PASS \/ R6-D3 complete/);
  assert.match(plan, /Meta Connect → OAuth → verified account selection → data acceptance → Disconnect → temiz Reconnect zinciri canlıda tamamlanmıştır/);
  assert.match(plan, /R6-D3 tamamlandı; sıradaki kapı R6-D4 Google Ads analist brief'idir/);
});

test('The documented reference and pre-implementation gap remain auditable', () => {
  const server = read('server.js');
  const dashboard = read('public/dashboard.html');
  assert.match(server, /graph\.facebook\.com\/\$\{META_GRAPH_VERSION\}\/me\/permissions/);
  assert.match(dashboard, /\/api\/platform\/meta\/disconnect/);
  assert.equal(contract.decision_baseline_gap.embedded_meta_disconnect_route_exists, false);
  assert.equal(contract.decision_baseline_gap.canonical_meta_disconnect_store_operation_exists, false);
});

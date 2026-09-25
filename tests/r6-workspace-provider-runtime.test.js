'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const contract = JSON.parse(read('contracts/r6-workspace-provider-runtime-v1.json'));

test('R6 contract keeps workspace authority independent from commerce channel', () => {
  assert.equal(contract.canonical_tenant, 'workspace_id');
  assert.equal(contract.authorities.commerce_channel_role, 'authorization_adapter_only');
  assert.equal(contract.authorities.browser_tenant_fields_are_authority, false);
  assert.deepEqual(contract.active_providers, ['meta', 'google_ads', 'klaviyo']);
  assert.deepEqual(contract.parked_providers, ['tiktok', 'pinterest']);
});

test('R6 live preflight records preparation-only state without claiming activation', () => {
  assert.equal(contract.status, 'R7A_MERCHANT_ACCEPTANCE_PASS_R6D_PREPARATION');
  assert.equal(contract.live_preflight.result, 'PASS_PREPARATION_ONLY');
  assert.equal(contract.live_preflight.dataset_v2_rows, 0);
  assert.equal(contract.live_preflight.canonical_connection_rows, 0);
  assert.equal(contract.live_preflight.reporting_currency_rows, 0);
  assert.equal(contract.live_preflight.user_id_nullable, false);
  assert.equal(contract.live_preflight.database_mutation, false);
  assert.equal(contract.live_preflight.provider_contact, false);
});

test('R6-D is provider-by-provider and repository preparation grants no live authority', () => {
  assert.equal(contract.r6d_work_packages.status, 'R6_D1_COMPLETE_R6_D2_C1_MIGRATION_PASS_APPLICATION_DEPLOYMENT_CLOSED');
  assert.deepEqual(contract.r6d_work_packages.sequence, [
    'R6-D1 common fail-closed acceptance runner',
    'R6-D2 Klaviyo workspace live acceptance',
    'R6-D3 Meta workspace live acceptance',
    'R6-D4 Google Ads workspace live acceptance',
    'R6-D5 provider-by-provider controlled activation decision'
  ]);
  assert.equal(contract.r6d_work_packages.current_live_connection_state.klaviyo, 'connected_merchant_confirmed');
  assert.ok(contract.r6d_work_packages.rules.includes('repository preparation does not authorize provider contact'));
  assert.ok(contract.r6d_work_packages.rules.includes('repository preparation does not authorize Dataset V2 writes'));
  assert.ok(contract.r6d_work_packages.rules.includes('one provider acceptance does not activate another provider'));
  assert.equal(contract.r6d_work_packages.r6_d1.result, 'PASS_REPOSITORY_ONLY');
  assert.equal(contract.r6d_work_packages.r6_d1.production_registered, false);
  assert.equal(contract.r6d_work_packages.r6_d2.result, 'PASS_REPOSITORY_PREPARATION_ONLY');
  assert.equal(contract.r6d_work_packages.r6_d2.current_connection, 'connected_merchant_confirmed');
  assert.equal(contract.r6d_work_packages.r6_d2.provider_contact, false);
  assert.equal(contract.r6d_work_packages.r6_d2.dataset_v2_write, false);
  assert.equal(contract.next_gate, 'R6-D2-C1_application_deployment_review_and_explicit_approval');
});

test('R6 resolves the R6-R7 dependency cycle without early provider activation', () => {
  assert.deepEqual(contract.dependency_resolution.sequence, [
    'R6-A live inventory and versioned contract',
    'R6-B workspace runtime composition with no provider activation',
    'R6-C activation migration preparation and dry verification',
    'R7-A reporting currency and canonical Connect foundation',
    'R6-D provider live acceptance and controlled activation',
    'R3-C final workspace tenant enforcement',
    'R7-B final connected and disconnect experience'
  ]);
  assert.ok(contract.forbidden_until_later_gate.includes('production_provider_runtime_activation'));
  assert.ok(contract.forbidden_until_later_gate.includes('dataset_v2_write'));
  assert.ok(contract.forbidden_until_later_gate.includes('tiktok_or_pinterest_activation'));
});

test('R6 preflight is read-only and fail-closed', () => {
  const sql = read('docs/security/sql/R6_WORKSPACE_RUNTIME_PREFLIGHT.sql');
  assert.match(sql, /PASS_PREPARATION_ONLY/);
  assert.match(sql, /BLOCK_DATASET_NOT_EMPTY/);
  assert.match(sql, /BLOCK_LEGACY_RUNTIME_ACTIVE/);
  assert.match(sql, /BLOCK_KLAVIYO_RESET_INCOMPLETE/);
  assert.doesNotMatch(sql, /\b(insert|update|delete|alter|drop|create|truncate)\b/i);
});

test('Execution Plan keeps R6-D2-C1 application deployment closed after the migration pass', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /R6-A\+B\+C\+D1 Done; R6-D2-C1 repository corrective prepared, production closed/);
  assert.match(plan, /R7-A.*R6-D/i);
  assert.match(plan, /R7-A merchant acceptance PASS; R6-D next; R7-B blocked by R6-D/i);
  assert.doesNotMatch(plan, /\| R7 \|[^\n]+`Blocked by R5–R6`/);
});

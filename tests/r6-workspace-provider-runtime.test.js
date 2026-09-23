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
  assert.equal(contract.status, 'R6A_PREFLIGHT_PASS_R6B_CODE_COMPLETE_R6C_LIVE_PASS_R7A_READY');
  assert.equal(contract.live_preflight.result, 'PASS_PREPARATION_ONLY');
  assert.equal(contract.live_preflight.dataset_v2_rows, 0);
  assert.equal(contract.live_preflight.canonical_connection_rows, 0);
  assert.equal(contract.live_preflight.reporting_currency_rows, 0);
  assert.equal(contract.live_preflight.user_id_nullable, false);
  assert.equal(contract.live_preflight.database_mutation, false);
  assert.equal(contract.live_preflight.provider_contact, false);
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

test('Execution Plan keeps R7-A behind the R6-C live apply and R6-D behind R7-A', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /R6-A\+B\+C Done; R7-A Ready/);
  assert.match(plan, /R6-D.*R7-A/i);
  assert.match(plan, /R7-A1 repository complete; R7-A2 next; R7-B blocked by R6-D/i);
  assert.doesNotMatch(plan, /\| R7 \|[^\n]+`Blocked by R5–R6`/);
});

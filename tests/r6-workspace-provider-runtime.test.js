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
  assert.equal(contract.status, 'R6_D2_C2_PRODUCT_CONTRACT_PASS_C3_APPROVAL_GATE');
  assert.equal(contract.live_preflight.result, 'PASS_PREPARATION_ONLY');
  assert.equal(contract.live_preflight.dataset_v2_rows, 0);
  assert.equal(contract.live_preflight.canonical_connection_rows, 0);
  assert.equal(contract.live_preflight.reporting_currency_rows, 0);
  assert.equal(contract.live_preflight.user_id_nullable, false);
  assert.equal(contract.live_preflight.database_mutation, false);
  assert.equal(contract.live_preflight.provider_contact, false);
});

test('R6-D is provider-by-provider and repository preparation grants no live authority', () => {
  assert.equal(contract.r6d_work_packages.status, 'R6_D1_COMPLETE_R6_D2_C1_DEPLOYED_R6_D2_C2_PRODUCT_CONTRACT_PASS');
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
  assert.equal(contract.next_gate, 'R6-D2-C3_read_only_sales_source_discovery_review_and_explicit_approval');
});

test('R6-D2-C2 freezes merchant-facing Klaviyo sales-source behavior without execution', () => {
  const ux = contract.r6d_work_packages.r6_d2.sales_source_product_contract;
  assert.equal(ux.result, 'PASS_CONTRACT_ONLY');
  assert.equal(ux.merchant_sees_technical_metric_id, false);
  assert.equal(ux.single_verified_candidate, 'read_only_summary_in_monthly_plan_cost_final_confirmation');
  assert.equal(ux.multiple_verified_candidates, 'single_choice_by_safe_integration_or_store_provenance');
  assert.equal(ux.zero_verified_candidates, 'fail_closed_not_connected');
  assert.equal(ux.separate_screen_for_single_candidate, false);
  assert.equal(ux.monthly_plan_cost_change_resets_sales_source, false);
  assert.deepEqual(ux.repeat_only_on, ['reconnect', 'account_change', 'provider_binding_invalid', 'explicit_settings_change']);
  assert.equal(ux.provider_contact, false);
  assert.equal(ux.database_write, false);
  assert.equal(ux.dataset_v2_write, false);
  assert.equal(ux.new_adapter_mapping_or_formula, false);
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

test('Execution Plan records both failed C6 attempts and the bounded token lifecycle corrective', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /C6-C second attempt FAILED_PROVIDER_ACCOUNT \/ rows 0; C6-D token lifecycle repository PASS, deployment pending/);
  assert.match(plan, /R6-D2-C2 Klaviyo satış kaynağı ürün sözleşmesi — PASS \/ C3 next/);
  assert.match(plan, /R6-D2-C3 salt-okunur satış kaynağı keşfi — Live PASS/);
  assert.match(plan, /R6-D2-C4 canonical satış kaynağı bağı — Live PASS/);
  assert.match(plan, /R6-D2-C5 salt-okunur Klaviyo runtime preflight — Live PASS/);
  assert.match(plan, /R6-D2-C6 kontrollü Dataset V2 canlı kabulü — C6-A\+C6-C failed closed \/ C6-B diagnostics PASS \/ Dataset V2 rows `0`/);
  assert.match(plan, /R6-D2-C6-D connected Klaviyo token lifecycle corrective — Repository PASS \/ deployment pending/);
  assert.match(plan, /Beklenmeyen provider `401` yalnız bir refresh \+ bir retry üretir/);
  assert.match(plan, /R6-D2-C7 postcheck ve Klaviyo PASS kararı/);
  assert.match(plan, /tamamlanmış E4\/E5\/E7.*yeniden geliştirilmez/i);
  assert.match(plan, /R7-A.*R6-D/i);
  assert.match(plan, /R7-A merchant acceptance PASS; R6-D next; R7-B blocked by R6-D/i);
  assert.doesNotMatch(plan, /\| R7 \|[^\n]+`Blocked by R5–R6`/);
});

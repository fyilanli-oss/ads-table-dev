const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contractPath = path.join(root, 'contracts', 'a6-eo-f3-code-dependency-carry-allowlist-v1.json');
const docPath = path.join(root, 'docs', 'A6_EO_F3_CODE_DEPENDENCY_CARRY_ALLOWLIST.md');
const masterPath = path.join(root, 'contracts', 'a6-eo-00-embedded-only-reestablishment-v1.json');
const executionPlanPath = path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');

const contract = JSON.parse(fs.readFileSync(contractPath, 'utf8'));
const doc = fs.readFileSync(docPath, 'utf8');
const master = JSON.parse(fs.readFileSync(masterPath, 'utf8'));
const executionPlan = fs.readFileSync(executionPlanPath, 'utf8');

test('EO-F3 freezes zero application modules as carry-as-is', () => {
  assert.equal(contract.status, 'eo_f3_complete_eo_f4_pending');
  assert.deepEqual(contract.classification_policy.application_modules_carry_as_is, []);
  assert.equal(contract.classification_policy.application_modules_carry_as_is_count, 0);
  assert.equal(contract.acceptance.application_modules_carried_as_is, 0);
  assert.match(doc, /Application\/runtime kodu için sonuç: \*\*sıfır modül\*\*/);
});

test('EO-F3 keeps every source behavior behind rewrite or contract gates', () => {
  const capabilities = contract.classification_policy.extract_and_rewrite.map((item) => item.capability);
  for (const required of [
    'shopify_tenant_install_session',
    'canonical_provider_connection',
    'provider_token_crypto',
    'meta_adapter',
    'google_ads_adapter',
    'klaviyo_adapter',
    'dataset_v2',
    'formula_query',
    'oauth_transaction',
    'scheduler_finality'
  ]) {
    assert.ok(capabilities.includes(required), `missing rewrite capability: ${required}`);
  }
  assert.ok(contract.classification_policy.contract_only.includes('shopify_embedded_UI_and_component_mapping'));
  assert.ok(contract.classification_policy.contract_only.includes('privacy_webhook_claim_workspace_deletion_clean_reinstall'));
});

test('EO-F3 excludes the legacy monolith and inactive providers from the target build', () => {
  const retired = contract.classification_policy.retire_from_target_build.join('\n');
  for (const forbidden of [
    'server.js',
    'public_legacy_standalone_product',
    'legacy_platform_connection_and_token_runtime',
    'V1_snapshot_dataset_jobs_schedules_and_user_scoped_backfill',
    'GA4_Google_Sheets_TikTok_Pinterest_Organic_runtime',
    'debug_test_operator_diagnostic_acceptance_historical_routes'
  ]) {
    assert.match(retired, new RegExp(forbidden.replace(/[.*+?^$()|[\]{}]/g, '\\$&')));
  }
  assert.equal(contract.target_build_negative_controls.forbidden_provider_or_legacy_imports, 0);
  assert.equal(contract.target_build_negative_controls.user_id_tenant_authority_uses, 0);
  assert.equal(contract.target_build_negative_controls.legacy_table_name_uses, 0);
});

test('EO-F3 rejects blind dependency carry and waits for EO-F4 data-plane choice', () => {
  const existing = contract.dependency_policy.existing_dependencies_not_automatically_carried;
  assert.equal(contract.dependency_policy.new_manifest_from_scratch, true);
  assert.equal(contract.dependency_policy.old_manifest_is_not_seed, true);
  assert.equal(existing.express, 'framework_choice_waits_for_EO_01');
  assert.equal(existing.googleapis, 'forbidden_as_google_ads_transitive_dependency');
  assert.equal(existing['@supabase/supabase-js'], 'data_plane_choice_waits_for_EO_F4');
  assert.equal(contract.dependency_policy.provider_SDK_default, 'not_allowed_without_evidence');
  assert.ok(contract.ci_required.includes('production_bundle_inspection'));
});

test('EO-F3 remains non-mutating and advances only to EO-F4', () => {
  assert.equal(contract.scope.production_mutation, false);
  assert.equal(contract.scope.database_mutation, false);
  assert.equal(contract.scope.provider_mutation, false);
  assert.equal(contract.scope.deployment_mutation, false);
  assert.equal(contract.scope.new_project_created, false);
  assert.equal(contract.implementation_authorized, false);
  assert.equal(contract.project_creation_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  assert.equal(contract.acceptance.next_gate, 'EO-F4');
  assert.match(master.status, /f3_complete/);
  assert.equal(master.completed_gates.at(-1).id, 'EO-F3');
  assert.match(executionPlan, /\*\*EO-F3 — Complete \/ carry allowlist frozen:\*\*/);
});

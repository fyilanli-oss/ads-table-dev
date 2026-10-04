const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-f7-go-decision-v1.json'), 'utf8'
));
const doc = fs.readFileSync(path.join(root, 'docs', 'A6_EO_F7_GO_DECISION.md'), 'utf8');
const master = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-00-embedded-only-reestablishment-v1.json'), 'utf8'
));
const plan = fs.readFileSync(
  path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'), 'utf8'
);

test('EO-F7 records the explicit product-owner GO decision', () => {
  assert.equal(contract.status, 'GO_frozen_EO-01_authorized');
  assert.equal(contract.authority.decision_owner, 'product_owner_user');
  assert.match(contract.authority.explicit_decision, /embedded-only temiz yeni kuruluş/);
  assert.equal(contract.selected_path, 'embedded_only_clean_reestablishment');
  assert.equal(contract.rejected_target_development_path, 'repair_and_extend_existing_monolith');
});

test('EO-F7 opens only controlled clean-project implementation', () => {
  assert.equal(contract.authorized_after_merge.overall_program_implementation, true);
  assert.equal(contract.authorized_after_merge.next_package, 'A6-EO-01');
  assert.equal(contract.authorized_after_merge.clean_github_repository_provisioning, true);
  assert.equal(contract.authorized_after_merge.preview_only_vercel_project_provisioning, true);
  assert.equal(contract.authorized_after_merge.separate_standard_supabase_project_provisioning, true);
  assert.equal(contract.target.repository_is_fork, false);
  assert.equal(contract.target.bulk_history_or_tree_copy, false);
  assert.equal(contract.target.preview_only_initial_deployment, true);
});

test('EO-F7 preserves three surfaces and three providers', () => {
  assert.deepEqual(contract.target.top_level_routes, ['/', '/ad-analysis', '/settings']);
  assert.deepEqual(contract.target.active_providers, ['meta', 'google_ads', 'klaviyo']);
  assert.equal(contract.target.github_repository_candidate, 'fyilanli-oss/ads-table-embedded');
  assert.equal(contract.target.vercel_project_candidate, 'ads-table-embedded');
});

test('EO-F7 does not authorize production, provider, carry, cutover or deletion', () => {
  assert.equal(contract.production_mutation_authorized, false);
  assert.equal(contract.provider_mutation_authorized, false);
  assert.equal(contract.live_data_carry_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  for (const forbidden of [
    'production_domain_switch',
    'legacy_production_shutdown',
    'live_provider_call_or_oauth_token_mutation',
    'legacy_delete_before_consumer_zero',
    'destructive_cleanup_or_retirement'
  ]) {
    assert.ok(contract.not_authorized.includes(forbidden));
  }
});

test('EO-F7 current decision package itself remains non-mutating', () => {
  for (const [key, value] of Object.entries(contract.current_package_mutation)) {
    assert.equal(value, false, key);
  }
  assert.equal(contract.current_package_mutation.github_repository_created, false);
  assert.equal(contract.current_package_mutation.vercel_project_created, false);
  assert.equal(contract.current_package_mutation.supabase_project_created, false);
});

test('EO-F7 advances the master and Execution Plan to EO-01', () => {
  assert.match(master.status, /f7_GO/);
  assert.equal(master.completed_gates.at(-1).id, 'EO-F7');
  assert.equal(master.strategic_decision.implementation_go, 'GO_EO-01_authorized');
  assert.match(plan, /\*\*EO-F7 — GO \/ embedded-only clean re-establishment:\*\*/);
  assert.match(doc, /\*\*GO — embedded-only temiz yeniden kuruluş\*\*/);
  assert.equal(contract.next_gate, 'A6-EO-01_clean_runtime_shell_CI_dependency_boundary');
});

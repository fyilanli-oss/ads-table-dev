const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-f6-effort-risk-comparison-v1.json'), 'utf8'
));
const doc = fs.readFileSync(path.join(root, 'docs', 'A6_EO_F6_EFFORT_RISK_COMPARISON.md'), 'utf8');
const master = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-00-embedded-only-reestablishment-v1.json'), 'utf8'
));
const plan = fs.readFileSync(
  path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'), 'utf8'
);

const sum = (items, key) => items.reduce((total, item) => total + item[key], 0);

test('EO-F6 compares equal review scope without removing safety gates', () => {
  assert.equal(contract.equal_scope_requirements.length, 15);
  assert.equal(contract.comparison.review_scope_reduced, false);
  assert.equal(contract.comparison.acceptance_gates_reduced, false);
  assert.equal(contract.comparison.legacy_retirement_before_consumer_zero, false);
  assert.ok(contract.equal_scope_requirements.includes('delete_my_data_executor_and_terminal_manifest'));
  assert.ok(contract.equal_scope_requirements.includes('dataset_v2_provenance_maturity_reconciliation_finality'));
});

test('EO-F6 estimate ranges reconcile exactly', () => {
  assert.equal(sum(contract.monolith_repair.units, 'minimum'), 59);
  assert.equal(sum(contract.monolith_repair.units, 'maximum'), 92);
  assert.equal(sum(contract.reestablishment.packages, 'minimum'), 42);
  assert.equal(sum(contract.reestablishment.packages, 'maximum'), 63);
  assert.equal(sum(contract.reestablishment.post_review_consumer_zero_retirement, 'minimum'), 7);
  assert.equal(sum(contract.reestablishment.post_review_consumer_zero_retirement, 'maximum'), 11);
  assert.deepEqual(contract.reestablishment.full_program_focused_days, { minimum: 49, maximum: 74 });
  assert.deepEqual(contract.comparison.focused_days_saved, { minimum: 17, maximum: 29 });
});

test('EO-F6 selects clean re-establishment with transparent scoring', () => {
  assert.equal(contract.comparison.selected, 'embedded_only_reestablishment');
  assert.deepEqual(contract.comparison.percent_saved, { minimum: 29, maximum: 32 });
  assert.deepEqual(contract.readiness_score.weighted_total_out_of_100, {
    monolith_repair: 44,
    embedded_only_reestablishment: 86
  });
  assert.equal(contract.decision.result, 'PASS_reestablishment_recommended');
  assert.equal(contract.decision.next_gate, 'EO-F7_human_GO_NO_GO');
});

test('EO-F6 separates focused effort from external calendar gates', () => {
  assert.equal(contract.estimation_unit.calendar_commitment, false);
  assert.ok(contract.estimation_unit.excludes.includes('shopify_app_review_queue'));
  assert.ok(contract.external_gates.includes('klaviyo_official_contract_revalidation_on_2026_10_15'));
  assert.ok(contract.external_gates.includes('provider_attribution_and_finality_windows'));
  assert.ok(contract.external_gates.includes('desktop_and_real_mobile_shopify_admin_acceptance'));
});

test('EO-F6 preserves the dependency-ordered critical path', () => {
  assert.deepEqual(contract.critical_path, [
    'EO-01', 'EO-02', 'EO-03', 'EO-04', 'EO-05', 'EO-06', 'EO-07', 'EO-08', 'review_ready_decision'
  ]);
  assert.equal(contract.source_measurements.target_top_level_surfaces, 3);
  assert.equal(contract.source_measurements.target_active_providers, 3);
  assert.equal(contract.source_measurements.carry_as_is_application_modules, 0);
});

test('EO-F6 remains non-mutating and advances only to the human GO decision', () => {
  assert.equal(contract.status, 'eo_f6_complete_eo_f7_human_decision_pending');
  assert.equal(contract.scope.production_mutation, false);
  assert.equal(contract.scope.database_mutation, false);
  assert.equal(contract.scope.deployment_mutation, false);
  assert.equal(contract.project_creation_authorized, false);
  assert.equal(contract.implementation_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  assert.match(master.status, /f6_complete/);
  assert.equal(master.completed_gates.at(-1).id, 'EO-F6');
  assert.match(plan, /\*\*EO-F6 — Complete \/ re-establishment recommended:\*\*/);
  assert.match(doc, /embedded-only temiz yeniden kuruluş.*tercih edilmelidir/s);
});

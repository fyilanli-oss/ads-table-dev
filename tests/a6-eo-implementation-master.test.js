const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-implementation-master-v1.json'), 'utf8'
));
const doc = fs.readFileSync(path.join(root, 'docs', 'A6_EO_IMPLEMENTATION_MASTER_TABLE.md'), 'utf8');
const plan = fs.readFileSync(
  path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'), 'utf8'
);
const go = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-f7-go-decision-v1.json'), 'utf8'
));

test('EO implementation has one active program and one active parent', () => {
  assert.equal(contract.status, 'baseline_frozen_EO-01_ready');
  assert.equal(contract.single_track_rules.only_active_program, 'A6-EO');
  assert.equal(contract.single_track_rules.maximum_active_parent_packages, 1);
  assert.equal(contract.single_track_rules.new_E_R_RM_implementation_packages_allowed, false);
  assert.equal(contract.single_track_rules.legacy_packages_role, 'reference_requirement_risk_and_evidence_only');
});

test('EO master contains exactly ten ordered parents and forty-three stable children', () => {
  assert.equal(contract.counts.parent_packages, 10);
  assert.equal(contract.counts.stable_child_packages, 43);
  assert.deepEqual(contract.packages.map((pkg) => pkg.id), [
    'A6-EO-01','A6-EO-02','A6-EO-03','A6-EO-04','A6-EO-05',
    'A6-EO-06','A6-EO-07','A6-EO-08','A6-EO-09','A6-EO-10'
  ]);
  assert.equal(contract.single_track_rules.maximum_named_child_depth, 1);
  assert.equal(contract.single_track_rules.deeper_work_items, 'checklist_or_test_not_package');
});

test('EO packages have their own nature and use old packages only as references', () => {
  for (const pkg of contract.packages) {
    assert.ok(pkg.title);
    assert.ok(pkg.outputs.length > 0);
    assert.ok(pkg.references.length > 0);
    assert.equal(
      pkg.reference_closure_rule,
      'not_closed_until_exact_EO_acceptance_evidence_and_explicit_closure_record'
    );
  }
  assert.equal(contract.packages[0].status, 'Ready');
  assert.ok(contract.packages.slice(1).every((pkg) => pkg.status === 'Not started'));
});

test('new findings route into an existing EO parent instead of creating a parallel family', () => {
  assert.deepEqual(
    [...new Set(Object.values(contract.new_finding_routing))],
    ['A6-EO-01','A6-EO-02','A6-EO-03','A6-EO-04','A6-EO-05','A6-EO-06','A6-EO-07','A6-EO-08','A6-EO-09','A6-EO-10']
  );
  assert.equal(contract.exception.creates_parallel_product_track, false);
  assert.ok(contract.exception.conditions.includes('does_not_expand_legacy_product'));
});

test('review and post-review boundaries remain explicit', () => {
  assert.deepEqual(contract.review_boundary.review_critical, [
    'A6-EO-01','A6-EO-02','A6-EO-03','A6-EO-04',
    'A6-EO-05','A6-EO-06','A6-EO-07','A6-EO-08'
  ]);
  assert.deepEqual(contract.review_boundary.post_review, ['A6-EO-09','A6-EO-10']);
  assert.equal(contract.review_boundary.review_ready_requires, 'A6-EO-08-D_and_explicit_human_decision');
  assert.ok(contract.review_boundary.legacy_retirement_requires.includes('separate_destructive_approval'));
});

test('the master is a governance prerequisite, not a production mutation', () => {
  assert.equal(go.status, 'GO_frozen_EO-01_authorized');
  assert.equal(contract.first_active_parent_after_merge, 'A6-EO-01');
  assert.equal(contract.technical_provisioning_before_merge, false);
  assert.equal(contract.production_mutation_authorized, false);
  assert.equal(contract.provider_mutation_authorized, false);
  assert.equal(contract.live_data_carry_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  assert.match(plan, /\*\*EO single-track implementation master:\*\*/);
  assert.match(doc, /tek aktif ürün hattı \*\*A6-EO\*\*/);
});

test('every stable child has a one-sentence scope summary and appears by display ID in the analyst table', () => {
  const children = contract.packages.flatMap((pkg) => pkg.children);
  assert.equal(children.length, 43);
  for (const child of children) {
    assert.ok(child.objective.length > 20, `${child.id} objective is too short`);
    assert.match(child.objective, /\.$/);
    const displayId = child.id.replace(/^A6-/, '');
    assert.match(doc, new RegExp(`\\*\\*${displayId} —`));
  }
  assert.equal(contract.single_track_rules.every_child_requires_one_sentence_scope_summary, true);
});

test('deepest-grain discovery precedes Ad Analysis implementation', () => {
  const eo7 = contract.packages.find((pkg) => pkg.id === 'A6-EO-07');
  assert.deepEqual(eo7.children.map((child) => [child.id, child.title]), [
    ['A6-EO-07-A', 'Settings surface'],
    ['A6-EO-07-B', 'Funnel App Home'],
    ['A6-EO-07-C', 'Cross-platform deepest-grain discovery'],
    ['A6-EO-07-D', 'Ad Analysis surface'],
    ['A6-EO-07-E', 'Attribution Differences nested view'],
    ['A6-EO-07-F', 'Desktop, real-mobile and accessibility acceptance']
  ]);
  assert.match(eo7.children[3].objective, /only after A6-EO-07-C passes/);
  assert.match(eo7.children[4].objective, /only after A6-EO-07-C and A6-EO-07-D pass/);
});

test('Reporting Store scope is single-active, effective-dated and fail-closed without candidate-based billing', () => {
  const decision = contract.cross_cutting_decisions.reporting_store_scope_v1;
  assert.equal(decision.creates_additional_shopify_workspace, false);
  assert.equal(decision.maximum_active_reporting_stores_per_workspace, 1);
  assert.equal(decision.multiple_candidates_require_settings_selection_before_dataset_write, true);
  assert.equal(decision.new_candidate_never_silently_changes_selection, true);
  assert.equal(decision.ambiguous_provider_write_state, 'store_scope_review_required_fail_closed');
  assert.equal(decision.selection_is_effective_dated, true);
  assert.equal(decision.switch_relabels_history, false);
  assert.equal(decision.new_scope_bootstrap, 'yesterday_and_today_only');
  assert.equal(decision.candidate_detection_or_single_active_switch_changes_price, false);
  assert.equal(decision.charge_by_detected_candidate_count, false);
  assert.deepEqual(decision.package_owners, [
    'A6-EO-02-C', 'A6-EO-03-C', 'A6-EO-04-A',
    'A6-EO-05-A', 'A6-EO-05-B', 'A6-EO-07-A'
  ]);
  assert.match(plan, /store_scope_review_required/);
  assert.match(doc, /tek aktif Reporting Store seçimi/);
});

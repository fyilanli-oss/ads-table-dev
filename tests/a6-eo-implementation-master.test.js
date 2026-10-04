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

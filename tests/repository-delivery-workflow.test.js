'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const contract = JSON.parse(read('contracts/repository-delivery-workflow-v1.json'));

test('repository delivery contract makes Connector primary without banning local execution', () => {
  assert.equal(contract.status, 'PASS_GOVERNANCE_ACTIVE');
  assert.equal(contract.source_of_truth.authority, 'github_remote');
  assert.equal(contract.workspace_model.primary_local_checkout_role, 'repository_anchor_and_read_only_inventory');
  assert.equal(contract.workspace_model.implementation_workspace, 'task_scoped_codex_managed_worktree');
  assert.equal(contract.workspace_model.local_source_edits_allowed_in_task_worktree, true);
  assert.equal(contract.workspace_model.local_only_delivery_allowed, false);
  assert.equal(contract.github_delivery.primary_transport, 'github_connector');
  assert.equal(contract.github_delivery.branch_base, 'verified_current_main');
  assert.equal(contract.github_delivery.required_ci_must_pass, true);
  assert.equal(contract.github_delivery.merge_requires_explicit_user_approval, true);
});

test('repository delivery contract fails closed instead of opening web or mutating Windows', () => {
  assert.equal(contract.failure_policy.github_connector_unavailable, 'STOP_AND_REPORT_SINGLE_BLOCKER');
  assert.equal(contract.failure_policy.web_editor_fallback, false);
  assert.equal(contract.failure_policy.local_git_fallback, false);
  assert.equal(contract.failure_policy.system_mutation_fallback, false);
  assert.ok(contract.forbidden_automatic_fallbacks.includes('github_web_editor_without_explicit_user_request'));
  assert.ok(contract.forbidden_automatic_fallbacks.includes('server_or_vm_restart'));
  assert.ok(contract.startup_gate.includes('stop_before_implementation_if_any_required_gate_fails'));
});

test('repository delivery contract closes local-only and worktree lifecycle risks', () => {
  assert.equal(contract.workspace_model.shared_git_metadata_may_be_deleted_with_attached_worktrees, false);
  assert.ok(contract.delivery_gate.includes('verify_remote_exact_content_or_hash'));
  assert.ok(contract.shutdown_gate.includes('verify_no_meaningful_local_only_content'));
  assert.ok(contract.shutdown_gate.includes('archive_managed_worktree_after_merge'));
  assert.ok(contract.shutdown_gate.includes('preserve_local_backup_files_until_separately_authorized'));
});

test('AGENTS and Execution Plan bind the repository delivery contract', () => {
  const agents = read('AGENTS.md');
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(agents, /Zorunlu Codex Windows çalışma ve GitHub teslim modeli/);
  assert.match(agents, /GitHub Connector'dır/);
  assert.match(agents, /aktif geliştirme alanı değildir/);
  assert.match(agents, /Connector kullanılamıyorsa web editörü/);
  assert.match(plan, /Codex Windows repository delivery governance — PASS/);
  assert.match(plan, /repository-delivery-workflow-v1\.json/);
  assert.match(plan, /local-only sıfır/);
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const migration = read('supabase/migrations/20260923154503_r6_dataset_v2_workspace_activation.sql');
const preflight = read('docs/security/sql/R6C_DATASET_ACTIVATION_PREFLIGHT.sql');
const postcheck = read('docs/security/sql/R6C_DATASET_ACTIVATION_POSTCHECK.sql');
const rollback = read('docs/security/sql/R6C_DATASET_ACTIVATION_ROLLBACK.sql');
const contract = JSON.parse(read('contracts/r6-workspace-provider-runtime-v1.json'));

test('R6-C only makes the compatibility actor optional', () => {
  assert.match(migration, /alter column user_id drop not null/i);
  assert.match(migration, /R6C_ACTIVATION_BLOCKED_UNBOUND_DATASET_ROWS/);
  assert.doesNotMatch(migration, /workspace_id\s+(?:alter column\s+)?set\s+not\s+null/i);
  assert.doesNotMatch(migration, /drop\s+(?:index|policy|column)/i);
  assert.doesNotMatch(migration, /insert\s+into|delete\s+from|truncate/i);
});

test('R6-C verification retains staged and legacy safeguards', () => {
  assert.match(preflight, /PASS_PREPARATION_ONLY/);
  assert.match(preflight, /dataset_v2_rows = 0/);
  assert.match(postcheck, /workspace_id_still_staged_nullable/);
  assert.match(postcheck, /legacy_unique_index_retained/);
  assert.match(postcheck, /legacy_select_policy_retained/);
  assert.match(postcheck, /workspace_indexes_retained/);
});

test('R6-C rollback fails closed after workspace-only rows exist', () => {
  assert.match(rollback, /where user_id is null/);
  assert.match(rollback, /R6C_ROLLBACK_BLOCKED_NULL_USER_ROWS_EXIST/);
  assert.match(rollback, /alter column user_id set not null/i);
});

test('R6-C contract records live schema acceptance without provider activation', () => {
  assert.equal(contract.status, 'R6_D2_KLAVIYO_LIVE_PASS_R6_D3C_META_CONNECTION_ACCOUNT_SELECTION_LIVE_PASS_DATA_RUNTIME_GATE');
  assert.equal(contract.r6c_activation_migration.prepared, true);
  assert.equal(contract.r6c_activation_migration.applied_live, true);
  assert.equal(contract.r6c_activation_migration.live_migration_version, '20260923154503');
  assert.equal(contract.r6c_activation_migration.explicit_production_approval_received, true);
  assert.equal(contract.r6c_activation_migration.postcheck_result, 'PASS');
  assert.equal(contract.r6c_activation_migration.provider_runtime_activated, false);
  assert.equal(contract.r6b_runtime_boundary.production_registered, false);
  assert.equal(contract.next_gate, 'R6-D4-D_GOOGLE_READ_ONLY_PRODUCTION_ACCEPTANCE');
});

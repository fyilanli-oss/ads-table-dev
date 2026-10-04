const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-f4-schema-data-carry-map-v1.json'), 'utf8'
));
const doc = fs.readFileSync(path.join(root, 'docs', 'A6_EO_F4_SCHEMA_DATA_CARRY_MAP.md'), 'utf8');
const master = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-00-embedded-only-reestablishment-v1.json'), 'utf8'
));
const plan = fs.readFileSync(
  path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'), 'utf8'
);

test('EO-F4 selects a separate standard Supabase project without authorizing creation', () => {
  assert.equal(contract.status, 'eo_f4_complete_eo_f5_pending');
  assert.equal(contract.data_plane_decision.selected, 'separate_standard_supabase_postgres_project');
  assert.equal(contract.data_plane_decision.orioledb, false);
  assert.equal(contract.data_plane_decision.project_creation_waits_for, 'EO-F7_human_GO');
  assert.equal(contract.project_creation_authorized, false);
  assert.equal(contract.schema_creation_authorized, false);
  assert.equal(contract.data_migration_authorized, false);
});

test('EO-F4 forbids service role and browser database authority', () => {
  assert.equal(contract.runtime_access.service_role, false);
  assert.equal(contract.runtime_access.data_api_runtime, false);
  assert.equal(contract.runtime_access.browser_database_client, false);
  assert.equal(contract.runtime_access.publishable_or_anon_runtime, false);
  assert.equal(contract.runtime_access.caller_supplied_workspace, false);
  assert.ok(contract.runtime_access.role_requirements.includes('NOBYPASSRLS'));
  assert.equal(contract.runtime_access.path, 'vercel_server_to_supavisor_transaction_mode');
});

test('EO-F4 exact carry is bounded to ten business rows', () => {
  assert.equal(contract.exact_carry_total_rows, 10);
  assert.deepEqual(
    Object.fromEntries(contract.exact_carry.map((item) => [item.source, item.rows])),
    {
      'public.workspaces': 1,
      'public.workspace_settings': 1,
      'public.shopify_installations': 1,
      'public.workspace_provider_connections': 3,
      'public.workspace_provider_email_spend_history': 4
    }
  );
  assert.equal(contract.sealed_token_carry.plaintext_export, false);
  assert.equal(contract.sealed_token_carry.old_key_envelopes_target_count, 0);
  assert.equal(contract.acceptance.plaintext_token_exposure, 0);
});

test('EO-F4 does not promote old Dataset or bulk FX rows into the clean authority', () => {
  assert.equal(contract.dataset_source_snapshot.total_rows, 5);
  assert.equal(contract.dataset_source_snapshot.direct_canonical_copy_rows, 0);
  assert.equal(contract.dataset_source_snapshot.providers.google_ads, 0);
  assert.deepEqual(contract.dataset_source_snapshot.refetch_bootstrap, ['yesterday', 'today']);
  assert.equal(contract.fx_source_snapshot.rows, 10478);
  assert.equal(contract.fx_source_snapshot.direct_bulk_copy_rows, 0);
  assert.equal(contract.fx_source_snapshot.mismatch_behavior, 'cutover_stop');
});

test('EO-F4 clean schema contains missing billing operations and privacy foundations', () => {
  for (const table of ['workspace_subscriptions', 'workspace_entitlements', 'trial_ledger']) {
    assert.ok(contract.target_schemas.billing.includes(table));
  }
  for (const table of ['refresh_runs', 'refresh_leases', 'provider_checkpoints', 'reconciliation_ledger']) {
    assert.ok(contract.target_schemas.operations.includes(table));
  }
  for (const table of ['webhook_claims', 'deletion_runs', 'deletion_manifests']) {
    assert.ok(contract.target_schemas.privacy.includes(table));
  }
  assert.ok(contract.target_schemas.integrations.includes('oauth_transactions'));
});

test('EO-F4 preserves rollback and advances only to EO-F5', () => {
  assert.equal(contract.scope.production_mutation, false);
  assert.equal(contract.scope.database_mutation, false);
  assert.equal(contract.scope.deployment_mutation, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  assert.equal(contract.rollback.source_project_mutation_during_migration, false);
  assert.equal(contract.rollback.silent_data_loss_allowed, false);
  assert.equal(contract.acceptance.next_gate, 'EO-F5');
  assert.match(master.status, /f4_complete/);
  assert.equal(master.completed_gates.at(-1).id, 'EO-F4');
  assert.match(plan, /\*\*EO-F4 — Complete \/ separate Supabase data-plane selected:\*\*/);
  assert.match(doc, /ayrı Supabase project/);
});

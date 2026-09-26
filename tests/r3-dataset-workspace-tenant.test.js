'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const fixtures = require('../funnel-core/fixtures');
const {
  WORKSPACE_CANONICAL_CONTRACT_VERSION,
  validateWorkspaceCanonicalRow
} = require('../funnel-core/workspace-canonical-contract');
const {
  WorkspaceInMemoryDatasetRepository,
  workspaceCanonicalUniqueKey
} = require('../funnel-core/workspace-dataset-repository');
const {
  WORKSPACE_UPSERT_CONFLICT,
  workspaceCanonicalToDbRow,
  workspaceDbToCanonicalRow
} = require('../funnel-core/workspace-supabase-dataset-repository');
const { WorkspaceFunnelQueryService } = require('../funnel-core/funnel-query-service');
const { WorkspaceCanonicalWriteBoundary } = require('../funnel-core/workspace-canonical-write-boundary');
const {
  WorkspaceDatasetRuntime,
  requireServerWorkspaceAuthority
} = require('../funnel-core/workspace-dataset-runtime');
const {
  workspaceCheckpointIdentity,
  createWorkspaceCheckpoint
} = require('../src/backfill/workspace-checkpoint');
const { createWorkspaceIdempotentBackfillBatchWriter } = require('../src/backfill/workspace-idempotent-batch');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const migration = read('supabase/migrations/20260923083734_add_dataset_v2_workspace_tenant.sql');
const enforcementMigration = read('supabase/migrations/20260925103000_r3c1_dataset_workspace_first_write_enforcement.sql');
const enforcementPreflight = read('docs/security/sql/R3C1_DATASET_FIRST_WRITE_PREFLIGHT.sql');
const enforcementPostcheck = read('docs/security/sql/R3C1_DATASET_FIRST_WRITE_POSTCHECK.sql');
const enforcementRollback = read('docs/security/sql/R3C1_DATASET_FIRST_WRITE_ROLLBACK.sql');
const contract = JSON.parse(read('contracts/r3-dataset-workspace-v1.json'));
const WORKSPACE_A = '11111111-1111-4111-8111-111111111111';
const WORKSPACE_B = '22222222-2222-4222-8222-222222222222';

function clone(value) { return JSON.parse(JSON.stringify(value)); }
function workspaceRow(workspaceId, overrides = {}) {
  const row = fixtures.metaPaid(overrides);
  row.identity.workspace_id = workspaceId;
  return row;
}

test('R3-A migration is additive and keeps legacy compatibility during staging', () => {
  assert.match(migration, /add column workspace_id uuid/);
  assert.match(migration, /performance_dataset_rows_v2_workspace_fk/);
  assert.match(migration, /references public\.workspaces\(id\)/);
  assert.match(migration, /validate constraint performance_dataset_rows_v2_workspace_fk/);
  assert.match(migration, /performance_dataset_rows_v2_workspace_canonical_uidx/);
  assert.match(migration, /where workspace_id is not null/);
  assert.doesNotMatch(migration, /workspace_id\s+set\s+not\s+null/i);
  assert.doesNotMatch(migration, /drop\s+(?:column|index).*user/i);
  assert.doesNotMatch(migration, /insert\s+into\s+public\.performance_dataset_rows_v2/i);
});

test('R3 contract preserves exact workspace tenant and production gates', () => {
  assert.equal(contract.status, 'R3C1_FIRST_WRITE_TENANT_ENFORCEMENT_LIVE_PASS');
  assert.equal(contract.canonical_tenant, 'workspace_id');
  assert.equal(contract.canonical_contract_version, 'v2');
  assert.deepEqual(contract.workspace_unique_key, [
    'workspace_id', 'platform', 'platform_account_id', 'business_date', 'traffic_type', 'entity_key'
  ]);
  assert.equal(contract.authority.browser_workspace_claim_is_authority, false);
  assert.equal(contract.backfill_checkpoint_disposition.physical_workspace_checkpoint_activation, 'R8');
  assert.equal(contract.production_gate.live_migration_version, '20260923083734');
  assert.equal(contract.production_gate.postcheck_result, 'PASS');
  assert.equal(contract.production_gate.explicit_approval_received, true);
  const liveEvidence = JSON.parse(read(contract.production_gate.evidence));
  assert.equal(liveEvidence.migration.applied_live, true);
  assert.equal(liveEvidence.postcheck.result, 'PASS');
  assert.equal(liveEvidence.postcheck.dataset_v2_rows, 0);
  assert.equal(liveEvidence.data_backfill_performed, false);
  assert.equal(liveEvidence.runtime_cutover_performed, false);
  const runtimeEvidence = JSON.parse(read(contract.r3b_runtime_verification.evidence));
  assert.equal(runtimeEvidence.result, 'PASS_WITH_ACTIVATION_SCHEMA_DEPENDENCY');
  assert.equal(runtimeEvidence.live_schema.user_id_nullable, false);
  assert.equal(runtimeEvidence.production_runtime_activated, false);
  const evidence = JSON.parse(read(contract.live_preflight.evidence));
  assert.equal(evidence.result, 'PASS_REPOSITORY_PREPARATION_ONLY');
  assert.equal(evidence.counts.dataset_v2_rows, 0);
  assert.equal(evidence.schema.backfill_checkpoints_exists, false);
  assert.equal(evidence.database_mutation, false);
});

test('R3-C1 enforces the workspace tenant before the first Dataset V2 write', () => {
  assert.match(enforcementMigration, /R3C1_BLOCKED_DATASET_NOT_EMPTY/);
  assert.match(enforcementMigration, /alter column workspace_id set not null/i);
  assert.match(enforcementMigration, /drop policy if exists performance_dataset_rows_v2_select_own/i);
  assert.match(enforcementMigration, /revoke select on table public\.performance_dataset_rows_v2 from authenticated/i);
  for (const index of [
    'performance_dataset_rows_v2_canonical_uidx',
    'performance_dataset_rows_v2_user_date_idx',
    'performance_dataset_rows_v2_account_scope_date_idx',
    'performance_dataset_rows_v2_entity_history_idx'
  ]) assert.match(enforcementMigration, new RegExp(`drop index if exists public\\.${index}`));
  assert.doesNotMatch(enforcementMigration, /drop\s+column\s+user_id/i);
  assert.doesNotMatch(enforcementMigration, /insert\s+into\s+public\.performance_dataset_rows_v2/i);
  assert.equal(contract.r3c1_first_write_enforcement.status, 'LIVE_PASS');
  assert.equal(contract.r3c1_first_write_enforcement.provider_contact, false);
  assert.equal(contract.r3c1_first_write_enforcement.dataset_write, false);
  assert.equal(contract.r3c1_first_write_enforcement.postcheck_result, 'PASS');
  assert.equal(contract.r3c1_first_write_enforcement.live_dataset_rows, 0);
  assert.equal(contract.r3c1_first_write_enforcement.c6_retry_performed, false);
  const r3c1Evidence = JSON.parse(read(contract.r3c1_first_write_enforcement.evidence));
  assert.equal(r3c1Evidence.postcheck.result, 'PASS');
  assert.equal(r3c1Evidence.postcheck.dataset_v2_rows, 0);
});

test('R3-C1 live scripts are zero-row fail-closed and reversible only before facts exist', () => {
  assert.match(enforcementPreflight, /BLOCK_DATASET_NOT_EMPTY/);
  assert.match(enforcementPreflight, /BLOCK_DEPENDENT_VIEWS/);
  assert.match(enforcementPostcheck, /workspace_required/);
  assert.match(enforcementPostcheck, /legacy_user_indexes_retired/);
  assert.match(enforcementPostcheck, /authenticated_direct_select_revoked/);
  assert.match(enforcementPostcheck, /service_role_workspace_access_preserved/);
  assert.match(enforcementRollback, /R3C1_ROLLBACK_BLOCKED_DATASET_ROWS_EXIST/);
  assert.match(enforcementRollback, /create policy performance_dataset_rows_v2_select_own/);
});

test('workspace canonical contract requires a real workspace UUID', () => {
  const row = workspaceRow(WORKSPACE_A);
  assert.equal(validateWorkspaceCanonicalRow(row), row);
  assert.equal(WORKSPACE_CANONICAL_CONTRACT_VERSION, 'v2');
  const missing = fixtures.metaPaid();
  assert.throws(() => validateWorkspaceCanonicalRow(missing), /identity\.workspace_id/);
  const invalid = workspaceRow('not-a-uuid');
  assert.throws(() => validateWorkspaceCanonicalRow(invalid), /must be a UUID/);
});

test('workspace v2 permits an absent compatibility actor but never an absent tenant', () => {
  const row = workspaceRow(WORKSPACE_A);
  delete row.identity.user_id;
  assert.equal(validateWorkspaceCanonicalRow(row), row);
  const db = workspaceCanonicalToDbRow(row);
  assert.equal(db.workspace_id, WORKSPACE_A);
  assert.equal(db.user_id, null);
  assert.equal(workspaceDbToCanonicalRow(db).identity.user_id, null);
});

test('workspace canonical identity isolates identical provider facts across workspaces', async () => {
  const repository = new WorkspaceInMemoryDatasetRepository();
  const first = workspaceRow(WORKSPACE_A);
  const second = workspaceRow(WORKSPACE_B);
  assert.notEqual(workspaceCanonicalUniqueKey(first), workspaceCanonicalUniqueKey(second));
  await repository.upsertCanonicalRawFacts([first, second]);
  const rowsA = await repository.readCanonicalRawFacts({ workspace_id: WORKSPACE_A, from: '2026-08-15', to: '2026-08-15' });
  const rowsB = await repository.readCanonicalRawFacts({ workspace_id: WORKSPACE_B, from: '2026-08-15', to: '2026-08-15' });
  assert.equal(rowsA.length, 1);
  assert.equal(rowsB.length, 1);
  assert.equal(rowsA[0].identity.workspace_id, WORKSPACE_A);
  assert.equal(rowsB[0].identity.workspace_id, WORKSPACE_B);
});

test('workspace canonical write boundary rejects missing workspace before repository delegation', async () => {
  const calls = [];
  const boundary = new WorkspaceCanonicalWriteBoundary({
    repository: { async upsertCanonicalRawFacts(rows) { calls.push(rows); return rows; } }
  });
  await assert.rejects(boundary.write([fixtures.metaPaid()]), /identity\.workspace_id/);
  assert.equal(calls.length, 0);
  const accepted = await boundary.write([workspaceRow(WORKSPACE_A)]);
  assert.equal(accepted.length, 1);
  assert.equal(calls.length, 1);
});

test('same workspace canonical key remains idempotent', async () => {
  const repository = new WorkspaceInMemoryDatasetRepository();
  await repository.upsertCanonicalRawFacts([workspaceRow(WORKSPACE_A)]);
  await repository.upsertCanonicalRawFacts([workspaceRow(WORKSPACE_A, { metrics: { purchase: 9, purchase_value: 999 } })]);
  const rows = await repository.readCanonicalRawFacts({ workspace_id: WORKSPACE_A, from: '2026-08-15', to: '2026-08-15' });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].raw_metrics.purchase, 9);
  assert.equal(rows[0].raw_metrics.purchase_value, 999);
});

test('workspace Supabase mapping uses the workspace conflict key and round-trips', () => {
  const row = workspaceRow(WORKSPACE_A);
  const db = workspaceCanonicalToDbRow(row);
  assert.equal(db.workspace_id, WORKSPACE_A);
  assert.equal(db.canonical_contract_version, 'v2');
  assert.equal(WORKSPACE_UPSERT_CONFLICT, 'workspace_id,platform,platform_account_id,business_date,traffic_type,entity_key');
  const roundTrip = workspaceDbToCanonicalRow({ id: 'x', created_at: 'x', ...db });
  assert.equal(roundTrip.identity.workspace_id, WORKSPACE_A);
  assert.equal(roundTrip.canonical_contract_version, 'v2');
});

test('workspace query service rejects cross-workspace repository leakage', async () => {
  const row = workspaceRow(WORKSPACE_B);
  const service = new WorkspaceFunnelQueryService({
    repository: { async readCanonicalRawFacts() { return [clone(row)]; } }
  });
  await assert.rejects(
    service.query({ workspace_id: WORKSPACE_A, from: '2026-08-15', to: '2026-08-15' }),
    /Cross-workspace query result rejected/
  );
});

test('workspace query service returns workspace-scoped currency and totals', async () => {
  const repository = new WorkspaceInMemoryDatasetRepository([workspaceRow(WORKSPACE_A)]);
  const service = new WorkspaceFunnelQueryService({ repository });
  const result = await service.query({ workspace_id: WORKSPACE_A, from: '2026-08-15', to: '2026-08-15' });
  assert.equal(result.meta.workspace_id, WORKSPACE_A);
  assert.equal(result.meta.canonical_contract_version, 'v2');
  assert.equal(result.meta.currency, 'TRY');
  assert.equal(result.rows.length, 1);
});

test('workspace runtime accepts only server-resolved authority and rejects caller tenant fields', async () => {
  const repository = new WorkspaceInMemoryDatasetRepository();
  const runtime = new WorkspaceDatasetRuntime({
    repository,
    resolveAuthority: async () => ({
      authority: 'server_resolved_workspace',
      workspace_id: WORKSPACE_A,
      source: 'shopify_verified_session'
    })
  });
  await assert.rejects(
    runtime.query({ authority_input: { workspace_id: WORKSPACE_B }, filters: { from: '2026-08-15', to: '2026-08-15' } }),
    /not tenant authority/
  );
  await assert.rejects(
    runtime.query({ authority_input: { session_token: 'opaque' }, filters: { workspace_id: WORKSPACE_B, from: '2026-08-15', to: '2026-08-15' } }),
    /not tenant authority/
  );
  assert.throws(
    () => requireServerWorkspaceAuthority({ authority: 'browser', workspace_id: WORKSPACE_A, source: 'request_body' }),
    /SERVER_WORKSPACE_AUTHORITY_REQUIRED/
  );
});

test('workspace runtime binds missing tenant and rejects cross-workspace writes', async () => {
  const repository = new WorkspaceInMemoryDatasetRepository();
  const runtime = new WorkspaceDatasetRuntime({
    repository,
    resolveAuthority: async () => ({
      authority: 'server_resolved_workspace',
      workspace_id: WORKSPACE_A,
      source: 'scheduled_workspace_job'
    })
  });
  const unbound = fixtures.metaPaid();
  delete unbound.identity.user_id;
  const written = await runtime.write({ authority_input: { job: 'opaque' }, rows: [unbound] });
  assert.equal(written[0].identity.workspace_id, WORKSPACE_A);
  assert.equal(written[0].identity.user_id, undefined);
  await assert.rejects(
    runtime.write({ authority_input: { job: 'opaque' }, rows: [workspaceRow(WORKSPACE_B)] }),
    /CROSS_WORKSPACE_WRITE_REJECTED/
  );
  const result = await runtime.query({
    authority_input: { job: 'opaque' },
    filters: { from: '2026-08-15', to: '2026-08-15' }
  });
  assert.equal(result.meta.workspace_id, WORKSPACE_A);
  assert.equal(result.rows.length, 1);
});

test('workspace backfill boundary cannot write another workspace', async () => {
  const checkpoint = { ...createWorkspaceCheckpoint({
    workspace_id: WORKSPACE_A,
    platform: 'meta',
    platform_account_id: 'meta-acct-1',
    business_date: '2026-08-15',
    date_key: 'today',
    finality: 'provisional',
    priority: 2
  }), status: 'running' };
  assert.deepEqual(workspaceCheckpointIdentity(checkpoint), {
    workspace_id: WORKSPACE_A,
    platform: 'meta',
    platform_account_id: 'meta-acct-1',
    business_date: '2026-08-15',
    date_key: 'today'
  });
  const writer = createWorkspaceIdempotentBackfillBatchWriter({
    writeBoundary: { async write(rows) { return rows; } }
  });
  await assert.rejects(writer.write({ checkpoint, rows: [workspaceRow(WORKSPACE_B)] }), /outside workspace checkpoint scope/);
  const accepted = await writer.write({ checkpoint, rows: [workspaceRow(WORKSPACE_A)] });
  assert.equal(accepted.persisted, 1);
  assert.match(accepted.idempotency_conflict, /^workspace_id,/);
});

test('R3 security scripts preserve a fail-closed live gate', () => {
  const preflight = read(contract.production_gate.preflight);
  const postcheck = read(contract.production_gate.postcheck);
  const rollback = read(contract.production_gate.rollback);
  assert.match(preflight, /BLOCK_NONEMPTY_DATASET_WITHOUT_EXPLICIT_BINDING/);
  assert.match(preflight, /BLOCK_UNEXPECTED_BACKFILL_TABLE/);
  assert.match(postcheck, /r3_postcheck_gate/);
  assert.match(postcheck, /legacy_unique_index_retained/);
  assert.match(rollback, /R3_ROLLBACK_BLOCKED_WORKSPACE_ROWS_EXIST/);
});

test('Execution Plan preserves first-write tenant enforcement across the controlled C6 attempts', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /R3-A\+B\+C1 Done; first-write tenant enforcement live PASS/);
  assert.match(plan, /20260925103312_r3c1_dataset_workspace_first_write_enforcement/);
  assert.match(plan, /postcheck `PASS` verdi: Dataset V2 `0` satır kaldı/);
  assert.match(plan, /C6-A ilk canlı write denemesi fail-closed `503` verdi ve Dataset V2 `0` kaldı/);
  assert.match(plan, /C6-C ikinci deneme `KLAVIYO_DATASET_ACCEPTANCE_FAILED_PROVIDER_ACCOUNT` ile fail-closed kaldı; Dataset V2 hâlâ `0` satırdır/);
  assert.match(plan, /R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4 Google Ads analyst brief next/);
  assert.match(plan, /Done \/ R4-A\+B\+C/);
  assert.doesNotMatch(plan, /\| R4 \|[^\n]+`Blocked by R3`/);
});

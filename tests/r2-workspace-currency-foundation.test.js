"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");
const migration = read("supabase/migrations/20260920090105_create_workspace_currency_foundation.sql");
const contract = JSON.parse(read("contracts/r2-workspace-currency-v1.json"));

test("R2 creates a platform-independent workspace and merchant-selected currency schema", () => {
  assert.match(migration, /create table public\.workspaces/);
  assert.match(migration, /create table public\.workspace_settings/);
  assert.match(migration, /reporting_currency_source = 'merchant_selected'/);
  assert.match(migration, /reporting_currency ~ '\^\[A-Z\]\{3\}\$'/);
  assert.doesNotMatch(migration, /shopify_(store|presentment)_currency/i);
});

test("R2 seeds only verified installation workspace IDs and adds a validated foreign key", () => {
  assert.match(migration, /from public\.shopify_installations as installation/);
  assert.match(migration, /group by installation\.workspace_id/);
  assert.match(migration, /add constraint shopify_installations_workspace_fk/);
  assert.match(migration, /validate constraint shopify_installations_workspace_fk/);
});

test("R2 tables deny browser roles and expose explicit server-only CRUD", () => {
  for (const table of ["workspaces", "workspace_settings"]) {
    assert.match(migration, new RegExp(`alter table public\\.${table} enable row level security`));
    assert.match(migration, new RegExp(`alter table public\\.${table} force row level security`));
    assert.match(migration, new RegExp(`revoke all on table public\\.${table} from public, anon, authenticated`));
    assert.match(migration, new RegExp(`revoke all on table public\\.${table} from service_role`));
    assert.match(migration, new RegExp(`grant select, insert, update, delete on table public\\.${table} to service_role`));
  }
  assert.doesNotMatch(migration, /create policy/i);
});

test("R2 keeps provider currency separate and blocks inferred reporting currency", () => {
  assert.equal(contract.contract_version, "r2-workspace-currency-v1");
  assert.equal(contract.status, "LIVE_APPLIED_POSTCHECK_PASS");
  assert.equal(contract.canonical_tenant, "workspace_id");
  assert.equal(contract.reporting_currency.source, "merchant_selected");
  assert.equal(contract.reporting_currency.provider_source_currency_is_separate, true);
  assert.equal(contract.reporting_currency.forbidden_sources.includes("shopify_store_currency"), true);
  assert.equal(contract.reporting_currency.forbidden_sources.includes("shopify_presentment_currency"), true);
  assert.equal(contract.seed.workspace_settings_seeded, false);
});

test("R2 ships preflight, postcheck and a fail-closed rollback", () => {
  const preflight = read(contract.production_gate.preflight);
  const postcheck = read(contract.production_gate.postcheck);
  const rollback = read(contract.production_gate.rollback);
  assert.match(preflight, /r2_preflight_gate/);
  assert.match(preflight, /BLOCK_EMBEDDED_MIGRATION_LEDGER_DRIFT/);
  assert.match(postcheck, /r2_postcheck_gate/);
  assert.match(postcheck, /browser_roles_denied/);
  assert.match(rollback, /R2_ROLLBACK_BLOCKED_CONFIGURED_WORKSPACES/);
  assert.match(rollback, /R2_ROLLBACK_BLOCKED_DOWNSTREAM_DEPENDENCY/);
});

test("R2 live preflight evidence blocks apply without exposing identifiers", () => {
  const evidence = JSON.parse(read("docs/security/evidence/R2_WORKSPACE_CURRENCY_PREFLIGHT_2026-09-20.json"));
  assert.equal(evidence.result, "BLOCK_EMBEDDED_MIGRATION_LEDGER_DRIFT");
  assert.equal(evidence.schema.backfill_checkpoints_exists, false);
  assert.equal(evidence.schema.embedded_oauth_workspace_column_exists, true);
  assert.equal(evidence.migration_ledger["20260911130000_embedded_oauth_recorded"], false);
  assert.equal(evidence.contains_identifiers, false);
  assert.equal(evidence.contains_tokens, false);
  assert.equal(evidence.database_mutation, false);
});

test("R2-A evidence records the exact ledger repair", () => {
  const evidence = JSON.parse(read("docs/security/evidence/R2A_EMBEDDED_MIGRATION_LEDGER_RECONCILIATION_2026-09-20.json"));
  assert.equal(evidence.result, "PASS");
  assert.deepEqual(evidence.ledger_versions_added, ["20260911130000", "20260911150000"]);
  assert.equal(evidence.business_rows_changed, false);
  assert.equal(evidence.schema_ddl_executed, false);
  assert.equal(evidence.foundation_migration_applied, false);
});

test("R2 live evidence records postcheck pass without inferred currency", () => {
  const evidence = JSON.parse(read("docs/security/evidence/R2_WORKSPACE_CURRENCY_FOUNDATION_APPLY_2026-09-20.json"));
  assert.equal(evidence.result, "PASS");
  assert.equal(evidence.live_migration_version, "20260920090105");
  assert.equal(evidence.counts.workspaces, 1);
  assert.equal(evidence.counts.workspace_settings, 0);
  assert.equal(evidence.reporting_currency_inferred, false);
  assert.equal(evidence.oauth_or_provider_rows_changed, false);
  assert.equal(contract.production_gate.required_before_apply, null);
});

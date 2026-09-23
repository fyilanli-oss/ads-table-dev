"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");
const decision = JSON.parse(read("contracts/r1-workspace-authority-v1.json"));

test("R1 freezes workspace_id as the only canonical tenant authority", () => {
  assert.equal(decision.contract_version, "r1-workspace-authority-v1");
  assert.equal(decision.status, "DECISION_COMPLETE_R2_REQUIRES_EXPLICIT_APPROVAL");
  assert.deepEqual(decision.canonical_tenant, {
    field: "workspace_id",
    database_type: "uuid",
    authority: "server_resolved",
    not_tenant_authority: ["user_id", "shopify_user_id", "shop_id", "email", "provider_account_id"],
  });
  assert.equal(decision.actor_identity.must_not_replace_workspace_authority, true);
});

test("R1 limits the active slice and parks TikTok and Pinterest", () => {
  assert.deepEqual(decision.active_provider_scope, ["meta", "google_ads", "klaviyo"]);
  assert.deepEqual(decision.parked_provider_scope, ["tiktok", "pinterest"]);
});

test("R1 inventory covers every required authority boundary", () => {
  const legacyAreas = new Set(decision.current_authority_inventory.legacy_user_scoped.map((item) => item.area));
  const workspaceAreas = new Set(decision.current_authority_inventory.workspace_scoped.map((item) => item.area));
  for (const area of [
    "canonical_envelope", "dataset_v2_schema_indexes_rls", "dataset_repository_and_query",
    "query_service", "provider_mappers_and_writers", "refresh_and_snapshot_jobs",
    "backfill_checkpoint_schema", "backfill_runtime", "standalone_oauth_transactions",
    "standalone_provider_connections",
  ]) assert.equal(legacyAreas.has(area), true, `missing legacy inventory: ${area}`);
  for (const area of [
    "shopify_installation_binding", "embedded_oauth_transaction_authority",
    "embedded_provider_connections", "verified_session_context",
  ]) assert.equal(workspaceAreas.has(area), true, `missing workspace inventory: ${area}`);
});

test("R1 records repository evidence without pretending source migration is complete", () => {
  assert.match(read("funnel-core/canonical-contract.js"), /identity\.user_id/);
  assert.match(read("supabase/migrations/20260816101220_create_performance_dataset_rows_v2.sql"), /\(user_id, platform, platform_account_id, business_date, traffic_type, entity_key\)/);
  assert.match(read("supabase/migrations/20260908074500_create_backfill_checkpoints.sql"), /unique \(user_id, platform, platform_account_id, business_date, date_key\)/);
  assert.match(read("supabase/migrations/20260911130000_add_embedded_oauth_authority.sql"), /add column workspace_id uuid/);
  assert.match(read("supabase/migrations/20260911150000_create_shopify_workspace_provider_connections.sql"), /primary key \(workspace_id, provider\)/);
});

test("R1 delivery order is deterministic and protected by an explicit R2 gate", () => {
  assert.equal(decision.ordered_delivery.length, 14);
  assert.equal(decision.ordered_delivery[0], "R2.1_read_only_preflight_and_collision_inventory");
  assert.equal(decision.ordered_delivery.at(-1), "R7.1_enable_currency_first_shopify_native_connect_account_selection_cost_and_disconnect_flow");
  assert.equal(decision.mandatory_gates.before_any_ddl.includes("explicit_R2_approval"), true);
  assert.deepEqual(decision.r1_mutations, {
    runtime_code: false,
    database: false,
    provider: false,
    deployment: false,
  });
});

test("R1 documentation supersedes shop_id tenant wording and the plan points to R1 first", () => {
  const tenantDoc = read("docs/E10_T2_SHOP_WORKSPACE_TENANT_MODEL.md");
  const r1Doc = read("docs/R1_WORKSPACE_AUTHORITY_DECISION.md");
  const plan = read("codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md");
  assert.match(tenantDoc, /Canonical AdsTable tenant anahtarı artık `workspace_id`/);
  assert.match(r1Doc, /R2 ayrı açık onay almadan başlayamaz/);
  assert.match(plan, /R1 \| Workspace authority kararını contract'lara işleme/);
});

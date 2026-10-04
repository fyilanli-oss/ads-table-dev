const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(
  path.join(root, "contracts/a6-eo-f1-f2-authority-project-boundary-v1.json"),
  "utf8"
));

test("EO-F1 preserves canonical authority and forbids legacy authority carry", () => {
  assert.equal(contract.eo_f1.result, "complete_with_blocking_gaps_transferred");
  assert.equal(contract.eo_f1.canonical_authorities.tenant, "public.workspaces");
  assert.equal(contract.eo_f1.canonical_authorities.provider_grants_and_tokens, "public.workspace_provider_connections");
  assert.ok(contract.eo_f1.legacy_runtime_tables_forbidden.includes("platform_connections"));
  assert.ok(contract.eo_f1.legacy_runtime_tables_forbidden.includes("performance_dataset_rows"));
  assert.equal(contract.eo_f1.live_counts.active_legacy_schedules, 0);
});

test("EO-F2 requires physically separate GitHub and Vercel boundaries", () => {
  assert.equal(contract.eo_f2.result, "separate_repository_and_separate_vercel_project_selected");
  assert.ok(contract.eo_f2.score.separate_repository_separate_vercel > contract.eo_f2.score.same_repository_separate_root);
  assert.equal(contract.eo_f2.not_created_yet, true);
  assert.equal(contract.project_creation_authorized, false);
});

test("data plane remains blocked from mixed-project service_role access", () => {
  assert.equal(contract.data_plane_gate.status, "blocked_until_EO_F4");
  assert.equal(contract.data_plane_gate.forbidden, "mixed_legacy_supabase_service_role_in_new_runtime");
  assert.equal(contract.implementation_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.deletion_authorized, false);
});

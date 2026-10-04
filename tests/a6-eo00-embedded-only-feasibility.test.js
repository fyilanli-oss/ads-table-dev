const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const read = (p) => fs.readFileSync(path.join(root, p), "utf8");

test("A6-EO-00 freezes embedded-only direction without authorizing cutover", () => {
  const contract = JSON.parse(read("contracts/a6-eo-00-embedded-only-reestablishment-v1.json"));

  assert.equal(contract.status, "strategic_direction_frozen_implementation_go_pending");
  assert.equal(contract.strategic_decision.product_surface, "shopify_embedded_only");
  assert.deepEqual(contract.strategic_decision.active_providers, ["meta", "google_ads", "klaviyo"]);
  assert.equal(contract.strategic_decision.big_bang_cutover, false);
  assert.equal(contract.current_package_effect.authorizes_cutover, false);
  assert.equal(contract.current_package_effect.authorizes_legacy_deletion, false);
  assert.equal(contract.current_package_effect.creates_new_project, false);
});

test("A6-EO-00 has complete feasibility and retirement safety gates", () => {
  const contract = JSON.parse(read("contracts/a6-eo-00-embedded-only-reestablishment-v1.json"));
  assert.deepEqual(contract.feasibility_gates.map((gate) => gate.id), [
    "EO-F1", "EO-F2", "EO-F3", "EO-F4", "EO-F5", "EO-F6", "EO-F7"
  ]);
  assert.ok(contract.mandatory_controls.includes("side_by_side_parity_canary_and_rollback"));
  assert.ok(contract.mandatory_controls.includes("consumer_zero_before_legacy_retirement"));
  assert.ok(contract.denylist.includes("root_server_js_monolith"));
  assert.ok(contract.denylist.includes("ga4_google_sheets_tiktok_pinterest_organic_runtime"));
});

test("Execution Plan records the controlled constitutional exception", () => {
  const plan = read("codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md");
  assert.match(plan, /A6-EO-00 — Embedded-only yeniden kuruluş karar ve fizibilite kapısı/);
  assert.match(plan, /allowlist tabanlı kontrollü yeniden kuruluş/);
  assert.match(plan, /consumer-zero/);
});

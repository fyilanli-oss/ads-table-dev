"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const matrix = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6a-official-readiness.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6A_OFFICIAL_CAPABILITY_READINESS.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("official readiness passes without claiming Shopify contact", () => {
  assert.equal(matrix.outcome, "PASS");
  assert.equal(matrix.safe_code, "OFFICIAL_READINESS_PASS");
  assert.equal(matrix.shopify_contact, false);
  assert.equal(matrix.development_store_contact, false);
  assert.equal(matrix.production_contact, false);
});

test("current official API, App Home and template baselines are explicit", () => {
  assert.equal(matrix.official_baseline.dev_mcp.package, "@shopify/dev-mcp");
  assert.equal(matrix.official_baseline.dev_mcp.version, "1.15.0");
  assert.equal(matrix.official_baseline.admin_api_version.version, "2026-07");
  assert.equal(matrix.official_baseline.app_home.reference, "v1.0");
  assert.equal(matrix.official_baseline.template.repository, "Shopify/shopify-app-template-react-router");
  assert.equal(matrix.official_baseline.deprecated_ui_dependency.must_not_select, true);
});

test("T5 component direction has official validator evidence", () => {
  assert.equal(matrix.ui_validation.tool, "validate_component_codeblocks");
  assert.equal(matrix.ui_validation.api, "polaris-app-home");
  assert.equal(matrix.ui_validation.revision, 2);
  assert.equal(matrix.ui_validation.status, "PASS");
  for (const component of ["s-page", "s-table", "s-modal", "s-tooltip", "s-checkbox"])
    assert.ok(matrix.ui_validation.validated_components.includes(component));
});

test("ShopifyQL attribution candidate is exact and remains gated by live smoke", () => {
  const candidate = matrix.attribution_candidate;
  assert.equal(candidate.api_version, "2026-07");
  assert.equal(candidate.metrics.platform_purchase_count, "orders__last_click");
  assert.equal(candidate.metrics.platform_sales_value, "total_sales__last_click");
  assert.equal(candidate.dimension, "referring_platform");
  assert.equal(candidate.modifier, "LAST_CLICK_ATTRIBUTION");
  assert.equal(candidate.scope, "read_reports");
  assert.match(candidate.protected_customer_data, /^level_2/);
  assert.equal(candidate.status, "PASS_OFFICIAL_CONTRACT_REQUIRES_T6D_LIVE_SMOKE");
});

test("development and production gates remain human-controlled", () => {
  assert.equal(matrix.gates.t6a_pass, true);
  assert.equal(matrix.gates.t6b_allowed, false);
  assert.equal(matrix.gates.t6b_requires_explicit_development_approval, true);
  assert.equal(matrix.gates.scope_request_allowed, false);
  assert.equal(matrix.gates.production_allowed, false);
  assert.match(doc, /explicit development approval required/);
});

test("Execution Plan advances only to the development approval boundary", () => {
  assert.match(plan, /E10-T6-A — `Done \/ PASS`/);
  assert.match(plan, /E10-T6-B — `Ready \/ explicit development approval required`/);
  assert.match(plan, /read_reports.*Level 2 protected customer data/s);
});

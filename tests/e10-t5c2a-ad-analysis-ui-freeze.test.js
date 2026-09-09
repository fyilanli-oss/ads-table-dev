"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c2a-ad-analysis-ui.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C2A_AD_ANALYSIS_SHOPIFY_COMPONENT_FREEZE.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("Ad Analysis ranking uses the approved metric and directions", () => {
  assert.deepEqual(contract.ranking.metric_allowlist, ["purchase", "sales", "revenue"]);
  assert.equal(contract.ranking.default_metric, "sales");
  assert.equal(contract.ranking.normal_default_direction, "descending");
  assert.equal(contract.ranking.compare_default_key, "selected_metric_percent_change");
  assert.equal(contract.ranking.compare_default_direction, "descending");
  assert.equal(contract.ranking.total_row_ranked, false);
  assert.equal(contract.ranking.computed_by, "backend");
});

test("zero denominators and unsupported metrics cannot become sortable fiction", () => {
  assert.equal(contract.ranking.zero_denominator_bucket, "new_or_not_comparable");
  assert.equal(contract.ranking.unsupported_bucket, "last");
  assert.match(doc, /sonsuz yüzde uydurulmaz/);
  assert.match(doc, /Görünen metin, renk veya ok frontend sort authority olamaz/);
});

test("Creative is metadata-only until the separate capability decision", () => {
  const creative = contract.creative_first_slice;
  assert.equal(creative.mode, "metadata_and_preview_only");
  assert.equal(creative.stored_in_dataset_v2, false);
  assert.equal(creative.included_in_funnel_hierarchy, false);
  assert.equal(creative.inherits_ad_metrics, false);
  assert.equal(creative.allocates_ad_metrics, false);
  assert.equal(creative.requires_capability_package, "E10-T5-C2-B");
  assert.match(doc, /Ad Purchase\/Sales\/Spend\/Revenue değeri.*kopyalanamaz, eşit\/oransal dağıtılamaz/);
});

test("Margin is removed and analysis details use one Shopify modal", () => {
  assert.ok(!contract.main_columns.includes("margin"));
  assert.deepEqual(contract.removed_columns, ["margin"]);
  assert.equal(contract.detail_surfaces.simultaneous_modals, false);
  for (const component of ["s-page", "s-button-group", "s-table", "s-button", "s-modal", "s-badge", "s-banner"]) {
    assert.match(doc, new RegExp(component));
  }
});

test("Execution Plan records C2-A and the completed Creative capability gate", () => {
  assert.match(plan, /E10-T5-C2-A — `Done` — Ad Analysis UI/);
  assert.match(plan, /E10-T5-C2-B — `Done` — Creative provider capability ve data model/);
  assert.match(plan, /Varsayılan ranking `Sales` ve yüksekten düşüğe/);
  assert.match(plan, /Creative.*Dataset V2'ye yazılmaz/);
  assert.match(plan, /sıradaki ürün paketi E10-T5-C3 Dashboard/);
});

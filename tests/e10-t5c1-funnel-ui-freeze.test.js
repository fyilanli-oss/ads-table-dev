"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c1-funnel-ui.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C1_FUNNEL_SHOPIFY_COMPONENT_FREEZE.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("approved Funnel decisions are machine-readable", () => {
  assert.equal(contract.default_view, "funnel");
  assert.equal(contract.views.table.compare_mode, "single_focus_metric");
  assert.deepEqual(contract.views.table.focus_metric_columns, ["comparison", "current", "absolute_change", "percent_change"]);
  assert.equal(contract.toolbar.only_one_popover_open, true);
  assert.equal(contract.toolbar.mobile_overlay, "modal_or_sheet");
});

test("Funnel stages preserve the approved HTML metric vocabulary", () => {
  const metrics = contract.views.funnel.stages.flatMap((stage) => stage.metrics);
  for (const metric of ["impression", "click", "spend", "ctr", "cpc", "add_to_cart", "add_to_cart_value", "checkout", "checkout_value", "abandoned", "abandoned_value", "purchase", "sales", "revenue", "roas", "cps"]) {
    assert.ok(metrics.includes(metric), `${metric} must remain in the Funnel contract`);
  }
  assert.ok(!metrics.includes("margin"));
});

test("general controls map to Shopify components while custom UI stays inside data presentation", () => {
  for (const component of ["s-page", "s-button", "s-popover", "s-date-picker", "s-choice-list", "s-search-field", "s-checkbox", "s-clickable-chip", "s-button-group", "s-table"]) {
    assert.match(doc, new RegExp(component));
  }
  assert.equal(contract.custom_visualization_boundary.custom_global_shell_or_component_framework, false);
  assert.match(doc, /yalnız expandable Funnel\/tree-table \*\*veri sunum gövdesinde\*\*/);
  assert.match(doc, /Component adları implementation kilidi değildir/);
});

test("view switching retains query and disclosure state", () => {
  for (const state of ["time_range", "comparison", "filters", "currency", "data_sources", "expanded_entities", "compare_focus_metric"]) {
    assert.ok(contract.shared_state_on_switch.includes(state));
  }
  assert.match(doc, /Frontend aggregation, formül, hierarchy veya provider support uyduramaz/);
});

test("Execution Plan records C1 as done without prematurely completing T5-C", () => {
  assert.match(plan, /E10-T5-C1 — `Done` — Funnel/);
  assert.match(plan, /E10-T5-C2 — `In progress` — Ad Analysis/);
  assert.match(plan, /parent T5-C `In progress`/);
  assert.match(plan, /varsayılan görünüm \*\*Funnel\*\*/);
  assert.match(plan, /yalnız seçili focus metric/);
  assert.match(plan, /aynı anda yalnız bir popover/);
});

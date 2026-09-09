"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c3-dashboard-ui.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C3_DASHBOARD_SHOPIFY_COMPONENT_FREEZE.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("Dashboard uses completed-day presets and proportional automatic compare", () => {
  assert.deepEqual(contract.time.preset_completed_days, [7, 14, 30, 90]);
  assert.equal(contract.time.includes_today, false);
  assert.equal(contract.time.custom_range, false);
  assert.equal(contract.comparison.control, "on_off_only");
  assert.equal(contract.comparison.custom_dates, false);
  assert.equal(contract.comparison.derivation, "immediately_preceding_completed_period_of_equal_length");
});

test("Funnel Overview preserves count and value comparison for all stages", () => {
  assert.deepEqual(contract.funnel_overview.map((item) => item.stage), ["add_to_cart", "checkout", "abandoned", "purchase"]);
  assert.deepEqual(contract.funnel_overview.map((item) => item.value), ["add_to_cart_value", "checkout_value", "abandoned_value", "sales"]);
  assert.deepEqual(contract.compare_fields_per_count_and_value, ["current", "comparison", "change"]);
  assert.match(doc, /tek yüzde iki farklı metriği temsil edemez/);
});

test("Dashboard removes renderer switch and preserves Funnel drilldown context", () => {
  assert.equal(contract.navigation.funnel_table_switch_on_dashboard, false);
  assert.equal(contract.navigation.funnel_drilldown, "view_funnel_preserve_context");
  assert.deepEqual(contract.shared_context, ["time_range", "comparison", "filters", "currency", "data_sources"]);
});

test("chart and KPI semantics preserve backend math and metric direction", () => {
  assert.deepEqual(contract.chart.series, ["sales", "spend", "revenue"]);
  assert.equal(contract.chart.revenue_formula, "sales_minus_spend");
  assert.deepEqual(contract.kpis, ["impressions", "clicks", "ctr", "cpc", "roas", "cps"]);
  assert.match(doc, /CPC\/CPS düşüşü olumlu olabilir/);
});

test("Execution Plan preserves Dashboard after Platforms is approved", () => {
  assert.match(plan, /E10-T5-C3 — `Done` — Dashboard/);
  assert.match(plan, /E10-T5-C4 — `Done` — Platforms/);
  assert.match(plan, /E10-T5-C5 — `Product decision required` — Attribution/);
  assert.match(plan, /\*\*Compare:\*\* Yalnız On\/Off/);
  assert.match(plan, /Funnel\/Table switch kaldırılır/);
  assert.match(plan, /Add to Cart Value.*Checkout Value.*Abandoned Value.*Sales/);
});

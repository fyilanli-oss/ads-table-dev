"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const repositoryRoot = path.resolve(__dirname, "..");
const contractPath = path.join(repositoryRoot, "contracts", "r7b5-klaviyo-email-cost-allocation-v2.json");
const executionPlanPath = path.join(repositoryRoot, "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md");

const contract = JSON.parse(fs.readFileSync(contractPath, "utf8"));
const executionPlan = fs.readFileSync(executionPlanPath, "utf8");

test("v2 preserves v1 as history and freezes recipient-weighted daily email allocation", () => {
  assert.equal(contract.status, "decision_frozen_implementation_pending");
  assert.equal(contract.supersedes[0].contract_id, "r7b5-klaviyo-estimated-30-day-email-spend-v1");
  assert.equal(contract.supersedes[0].preserved_as, "historical decision record");
  assert.equal(contract.source_cost.daily_formula, "estimated_30_day_email_spend / 30");
  assert.equal(contract.source_cost.immutable_daily_total, true);
  assert.deepEqual(contract.eligible_leaf_rows.grain, ["campaign_message", "flow_message"]);
  assert.equal(contract.allocation.row_spend_formula, "daily_email_cost * row_recipients / sum(eligible_row_recipients)");
  assert.equal(contract.allocation.volume_weighted, true);
  assert.equal(contract.allocation.copy_daily_cost_to_each_leaf, false);
  assert.equal(contract.allocation.total_invariant, "sum(allocated_row_spend) == daily_email_cost");
  assert.equal(contract.allocation.rounding.invariant_after_rounding, "Persisted row spend sums exactly to persisted account-day daily email cost.");
});

test("zero-recipient dates preserve one unallocated account-day cost without a fake leaf", () => {
  assert.equal(contract.allocation.no_eligible_recipients.leaf_spend, null);
  assert.equal(contract.allocation.no_eligible_recipients.account_day_spend, "preserve daily_email_cost exactly once as unallocated email spend");
  assert.equal(contract.allocation.no_eligible_recipients.synthetic_leaf, false);
  assert.equal(contract.eligible_leaf_rows.synthetic_leaf_forbidden, true);
  assert.ok(contract.scope.excluded.includes("SMS spend"));
  assert.ok(contract.scope.excluded.includes("MMS spend"));
  assert.ok(contract.scope.excluded.includes("WhatsApp spend"));
});

test("Dataset V2 keeps raw facts while Formula Engine owns derived metrics", () => {
  assert.equal(contract.dataset_v2.persist_raw_facts_only, true);
  assert.equal(contract.dataset_v2.derived_metrics_persisted, false);
  assert.equal(contract.dataset_v2.derived_metrics_owner, "Formula Engine");
  assert.deepEqual(
    contract.formula_engine.cost_dependency.recalculated_at_same_aggregate_scope_but_not_cost_dependent,
    ["ctr", "abandoned", "abandoned_value"],
  );
  assert.deepEqual(
    contract.formula_engine.cost_dependency.directly_depends_on_allocated_spend,
    ["cpc", "roas", "cps", "revenue", "revenue_margin"],
  );
  assert.equal(contract.formula_engine.formulas.revenue, "sales - spend");
  assert.equal(contract.formula_engine.formulas.revenue_margin, "revenue / sales * 100");
  assert.deepEqual(contract.formula_engine.naming.canonical, ["revenue", "revenue_margin"]);
  assert.deepEqual(contract.formula_engine.naming.legacy_compatibility_only, ["profit", "margin"]);
});

test("2026-10-15 is a revalidation gate, not automatic variation activation", () => {
  const gate = contract.campaign_variation_ga_gate;
  assert.equal(gate.target_revision_date, "2026-10-15");
  assert.equal(gate.automatic_activation_on_date, false);
  assert.deepEqual(gate.campaign_target_hierarchy, ["campaign", "campaign_message", "campaign_variation"]);
  assert.deepEqual(gate.flow_target_hierarchy, ["flow", "flow_message"]);
  assert.equal(gate.reporting_variation_equals_resource_id_assumption_forbidden, true);
  assert.equal(gate.production_backfill_requires_separate_approval, true);
});

test("Execution Plan carries the v2 allocation, Formula Engine and GA gates", () => {
  for (const requiredText of [
    "### 2 Ekim 2026 — Klaviyo email maliyet dağıtımı v2 ve 15 Ekim GA kapısı",
    "row_spend = daily_email_cost * row_recipients / sum(eligible_row_recipients)",
    "revenue = sales - spend",
    "revenue_margin = revenue / sales * 100",
    "Campaign → Campaign Message → Campaign Variation",
    "Reporting API",
    "otomatik production activation veya backfill başlatmaz",
  ]) {
    assert.match(executionPlan, new RegExp(requiredText.replace(/[.*+?^$\{\}()|[\]\\]/g, "\\$&")));
  }
});

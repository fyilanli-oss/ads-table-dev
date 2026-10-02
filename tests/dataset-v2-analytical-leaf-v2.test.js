"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const repositoryRoot = path.resolve(__dirname, "..");
const analyticalContractPath = path.join(repositoryRoot, "contracts", "dataset-v2-analytical-leaf-v2.json");
const costContractPath = path.join(repositoryRoot, "contracts", "r7b5-klaviyo-email-cost-allocation-v2.json");
const executionPlanPath = path.join(repositoryRoot, "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md");

const analytical = JSON.parse(fs.readFileSync(analyticalContractPath, "utf8"));
const cost = JSON.parse(fs.readFileSync(costContractPath, "utf8"));
const executionPlan = fs.readFileSync(executionPlanPath, "utf8");

test("analytical leaf v2 delegates to and cannot modify the approved cost contract", () => {
  const delegation = analytical.cost_contract_delegation;
  assert.equal(delegation.modifies_cost_contract, false);
  assert.equal(delegation.authoritative_contract_id, cost.contract_id);
  assert.equal(
    delegation.authoritative_contract_path,
    "contracts/r7b5-klaviyo-email-cost-allocation-v2.json",
  );
  assert.deepEqual(delegation.current_email_allocation_grain, cost.eligible_leaf_rows.grain);
  assert.match(delegation.rule, /may not restate, replace or override/);
});

test("provider financial leaves are explicit and synthetic entities stay forbidden", () => {
  const providers = analytical.provider_hierarchies;
  assert.deepEqual(providers.meta.hierarchy, ["campaign", "ad_set", "ad"]);
  assert.equal(providers.meta.analytical_leaf, "ad");
  assert.deepEqual(providers.google_ads_standard.hierarchy, ["campaign", "ad_group", "ad"]);
  assert.equal(providers.google_ads_standard.analytical_leaf, "ad");
  assert.deepEqual(providers.google_ads_performance_max.hierarchy, ["campaign", "asset_group"]);
  assert.equal(providers.google_ads_performance_max.analytical_leaf, "asset_group");
  assert.deepEqual(
    providers.google_ads_performance_max.forbidden_complete_financial_leaves,
    ["synthetic_ad_group", "synthetic_ad", "individual_asset"],
  );
});

test("Klaviyo Campaign has a gated target while Flow remains message-level", () => {
  const campaign = analytical.provider_hierarchies.klaviyo_campaign;
  const flow = analytical.provider_hierarchies.klaviyo_flow;
  const audience = analytical.provider_hierarchies.klaviyo_audience;

  assert.deepEqual(campaign.current_hierarchy, ["campaign", "campaign_message"]);
  assert.equal(campaign.current_analytical_leaf, "campaign_message");
  assert.deepEqual(
    campaign.target_hierarchy_after_gate,
    ["campaign", "campaign_message", "campaign_variation"],
  );
  assert.equal(campaign.activation_requires_campaign_variation_ga_gate, true);

  assert.deepEqual(flow.current_hierarchy, ["flow", "flow_message"]);
  assert.deepEqual(flow.target_hierarchy, ["flow", "flow_message"]);
  assert.equal(flow.target_analytical_leaf, "flow_message");
  assert.equal(flow.flow_variation_assumption_forbidden, true);

  assert.equal(audience.role, "targeting_metadata");
  assert.equal(audience.performance_leaf, false);
  assert.equal(audience.allocated_spend_owner, false);
});

test("the GA date cannot activate identity, runtime or backfill by itself", () => {
  const gate = analytical.campaign_variation_ga_gate;
  assert.equal(gate.target_revision_date, "2026-10-15");
  assert.equal(gate.date_alone_activates_nothing, true);
  assert.equal(gate.automatic_activation_on_date, false);
  assert.equal(gate.reporting_variation_equals_resource_id_assumption_forbidden, true);
  assert.equal(gate.production_backfill_requires_separate_approval, true);
  assert.equal(analytical.dataset_v2_and_formula_engine.schema_change_in_this_package, false);
  assert.equal(analytical.dataset_v2_and_formula_engine.runtime_activation_in_this_package, false);
});

test("Execution Plan binds analytical leaf v2 without reviving the unpublished v1 draft", () => {
  for (const requiredText of [
    "contracts/dataset-v2-analytical-leaf-v2.json",
    "Meta → Ad",
    "Google Ads Standard → Ad",
    "Google Performance Max → Asset Group",
    "Campaign → Campaign Message",
    "Flow → Flow Message",
    "Klaviyo Audience",
    "contracts/r7b5-klaviyo-email-cost-allocation-v2.json",
    "maliyet sözleşmesini değiştirmez",
  ]) {
    assert.ok(executionPlan.includes(requiredText), `Execution Plan is missing: ${requiredText}`);
  }
  assert.equal(executionPlan.includes("contracts/dataset-v2-analytical-leaf-v1.json"), false);
});

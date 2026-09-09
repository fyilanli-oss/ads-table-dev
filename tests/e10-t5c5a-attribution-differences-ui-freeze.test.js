"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c5a-attribution-differences-ui.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C5A_ATTRIBUTION_DIFFERENCES_FREEZE.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("C5-A is Attribution Differences rather than an unsupported overlap claim", () => {
  assert.equal(contract.user_facing_name, "Attribution Differences");
  assert.equal(contract.context.grain_equivalence_claimed, false);
  for (const claim of ["overlap", "duplicate", "decrease_overlap", "deduplicate"]) {
    assert.ok(contract.forbidden_user_claims_or_actions.includes(claim));
  }
  assert.match(doc, /fark.*belirli bir Campaign\/Ad Set\/Ad'in hatalı olduğuna kanıt değildir/);
});

test("the neutral table uses backend signed differences with no compare or color judgement", () => {
  assert.deepEqual(contract.table.columns, [
    "platform", "provider_purchase", "shopify_attributed_purchase", "purchase_difference",
    "provider_sales", "shopify_attributed_sales", "sales_difference", "data_status", "action",
  ]);
  assert.equal(contract.table.difference_formula, "provider_minus_shopify_attributed");
  assert.equal(contract.table.computed_by, "backend");
  assert.equal(contract.context.comparison, false);
  assert.equal(contract.table.percent_change, false);
  assert.equal(contract.table.performance_color_semantics, false);
});

test("Review explains the difference but cannot mutate Funnel or provider facts", () => {
  assert.deepEqual(contract.review_modal.actions, ["close", "view_funnel_preserve_context"]);
  assert.equal(contract.review_modal.mutation, false);
  assert.deepEqual(contract.mutations, {
    dataset_v2: false,
    funnel_kpis: false,
    dashboard_kpis: false,
    provider_facts: false,
    manual_ad_allocation: false,
  });
  assert.match(doc, /Apply\/decrease\/fix\/deduplicate.*yoktur/);
});

test("verified leaf reconciliation remains a separate evidence-gated capability", () => {
  assert.equal(contract.c5b_verified_reconciliation.status, "deferred");
  assert.ok(contract.c5b_verified_reconciliation.requires.includes("same_order_stable_evidence"));
  assert.ok(contract.c5b_verified_reconciliation.requires.includes("server_resolved_leaf_ad"));
  assert.equal(contract.c5b_verified_reconciliation.provider_fact_overwrite, false);
  assert.deepEqual(contract.c5b_verified_reconciliation.forbidden_allocation, ["largest_ad", "proportional", "user_selected_ad"]);
});

test("Execution Plan closes C5-A and advances only to C7", () => {
  assert.match(plan, /E10-T5-C5-A — `Done` — Attribution Differences/);
  assert.match(plan, /E10-T5-C5-B — `Deferred` — Verified Reconciliation/);
  assert.match(plan, /E10-T5-C7 — `Done` — Integrated navigation\/acceptance/);
  assert.match(plan, /sıradaki iş \*\*E10-T6-B Development App Bootstrap — Ready \/ explicit development approval required\*\*/);
  assert.equal(contract.shopify_ui.official_components_only, true);
});

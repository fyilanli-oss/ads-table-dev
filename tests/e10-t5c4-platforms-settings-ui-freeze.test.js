"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c4-platforms-settings-ui.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C4_PLATFORMS_SETTINGS_SHOPIFY_FREEZE.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("currency is completed before the Shopify-native Platforms surface", () => {
  assert.deepEqual(contract.onboarding.sequence, ["currency_selection", "platforms"]);
  assert.equal(contract.onboarding.currency_required_before_platforms, true);
  assert.equal(contract.onboarding.authority, "backend_workspace");
});

test("the first Platforms slice follows the approved provider and modal flow", () => {
  assert.deepEqual(contract.providers.active, ["meta", "google", "tiktok", "klaviyo"]);
  assert.deepEqual(contract.providers.parked, ["pinterest"]);
  assert.deepEqual(contract.connect.sequence, ["explanation_modal", "explicit_connect", "top_level_oauth", "verified_account_selection"]);
  assert.equal(contract.connect.connected_requires_active_account, true);
  assert.equal(contract.connect.standalone_dashboard_return, false);
  assert.equal(contract.disconnect.warning_modal, true);
  assert.equal(contract.disconnect.deletes_historical_analytics, false);
});

test("Klaviyo completes only after the fixed monthly email plan cost", () => {
  assert.deepEqual(contract.klaviyo.sequence_after_account_selection, ["email_monthly_plan_cost"]);
  assert.equal(contract.klaviyo.cost_required_to_complete, true);
  assert.equal(contract.klaviyo.label, "Email Monthly Plan Cost");
  assert.match(doc, /Estimated Monthly Spend.*yasaktır/);
});

test("Settings exposes one server-authorized active ad account to Funnel", () => {
  assert.deepEqual(contract.settings.sections, ["currency", "klaviyo_email_monthly_plan_cost", "ad_accounts"]);
  assert.equal(contract.settings.ad_accounts.selection, "single");
  assert.equal(contract.settings.ad_accounts.maximum_active_accounts, 1);
  assert.equal(contract.settings.ad_accounts.funnel_scope, "active_account_only");
  assert.equal(contract.settings.ad_accounts.save_revalidates_server_ownership, true);
  assert.match(doc, /checkbox veya multi-select değildir/);
});

test("the freeze remains intact after Attribution advances to integrated acceptance", () => {
  assert.equal(contract.shopify_ui.official_components_only, true);
  assert.equal(contract.shopify_ui.custom_admin_shell, false);
  assert.equal(contract.shopify_ui.custom_control_framework, false);
  assert.match(plan, /E10-T5-C4 — `Done` — Platforms/);
  assert.match(plan, /E10-T5-C5-A — `Done` — Attribution Differences/);
  assert.match(plan, /E10-T5-C7 — `Done` — Integrated navigation\/acceptance/);
  assert.match(plan, /E10-T5-C6 — `Done` — Settings/);
  assert.match(plan, /sıradaki iş \*\*E10-T6-B Development App Bootstrap — Ready \/ explicit development approval required\*\*/);
});

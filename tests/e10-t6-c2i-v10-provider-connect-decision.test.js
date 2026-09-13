"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6-c2i-v10-provider-connect-decision.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6_C2I_V10_PROVIDER_CONNECT_DECISION.md"), "utf8");

test("V10 selects Klaviyo without authorizing execution", () => {
  assert.equal(contract.status, "DECISION_FROZEN_EXECUTION_BLOCKED");
  assert.equal(contract.selected_first_provider, "klaviyo");
  assert.equal(contract.fallback_provider, "google_ads");
  assert.equal(contract.blocking_prerequisite, "E10_T6_C2I_V9_REAL_DEVICE_SHOPIFY_NATIVE_PASS");
  assert.equal(contract.evidence_received, false);
  assert.deepEqual(contract.authorization, {
    repository_decision_work: true,
    provider_consent: false,
    provider_api_smoke: false,
    production_mutation: false,
  });
});

test("Klaviyo connect cannot collapse OAuth, account selection, and cost save", () => {
  assert.deepEqual(contract.connect_sequence, [
    "connect_trigger", "explanation_modal", "explicit_modal_connect", "top_level_oauth",
    "server_verified_callback", "verified_klaviyo_account_selection", "account_selection_close",
    "email_monthly_plan_cost_entry", "explicit_cost_save", "connected",
  ]);
  assert.equal(contract.connect_invariants.oauth_alone_means_connected, false);
  assert.equal(contract.connect_invariants.account_selection, "single");
  assert.equal(contract.connect_invariants.cost_required_to_complete, true);
  assert.equal(contract.terminology.klaviyo_account_is_ad_account, false);
  assert.equal(contract.terminology.cost_label, "Email Monthly Plan Cost");
  assert.match(doc, /Spend Amount.*gösterilmez/);
});

test("disconnect is explicit, cancel-safe, and retains analytics", () => {
  assert.deepEqual(contract.disconnect_sequence, [
    "disconnect_trigger", "warning_modal", "explicit_disconnect_or_cancel", "disconnect_confirmed",
    "provider_refresh_stopped", "historical_analytics_retained",
  ]);
  assert.equal(contract.disconnect_invariants.warning_tone, "critical");
  assert.equal(contract.disconnect_invariants.cancel_is_non_destructive, true);
  assert.equal(contract.disconnect_invariants.historical_analytics_deleted, false);
  assert.equal(contract.disconnect_invariants.privacy_deletion_implicit, false);
});

test("package order keeps V9 and every provider contact as an explicit gate", () => {
  assert.equal(contract.next_packages[0], "E10_T6_C2I_V9_REAL_DEVICE_ACCEPTANCE");
  assert.deepEqual(contract.next_packages.slice(1), [
    "E10_T6_C2I_V10A_KLAVIYO_CONNECT_MODAL",
    "E10_T6_C2I_V10B_KLAVIYO_CONSENT_DECISION",
    "E10_T6_C2I_V10C_KLAVIYO_ACCOUNT_SELECTION",
    "E10_T6_C2I_V10D_KLAVIYO_MONTHLY_PLAN_COST",
    "E10_T6_C2I_V10E_KLAVIYO_DISCONNECT",
    "E10_T6_C2I_V10F_KLAVIYO_READ_ONLY_API_SMOKE",
  ]);
  assert.match(doc, /Bir önceki adımın `PASS` olması sonraki provider teması için örtük onay değildir/);
});

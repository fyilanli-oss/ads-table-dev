"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const contract = require("../contracts/shopify/e10-t6c-provider-oauth-preflight.json");

test("E10-T6-C remains fail-closed until workspace OAuth authority is implemented", () => {
  assert.equal(contract.status, "BLOCKED_REPOSITORY_AUTHORITY_GAP");
  assert.equal(contract.verified_prerequisite, "E10_T6B_PASS");
  assert.deepEqual(contract.required_transaction_authority, [
    "shop_id", "workspace_id", "shopify_user_id", "provider", "surface", "return_target",
  ]);
  assert.equal(contract.required_surface, "shopify_embedded");
  assert.equal(contract.required_return_target, "/shopify/app/platforms");
  assert.equal(contract.forbidden_shortcuts.includes("treat_workspace_id_as_auth_user_id"), true);
  assert.equal(contract.forbidden_shortcuts.includes("start_live_provider_consent_before_authority_bridge_passes"), true);
  assert.equal(contract.production_contact, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.secrets_or_identifiers_recorded, false);
});

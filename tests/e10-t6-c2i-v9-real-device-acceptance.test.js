"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6-c2i-v9-real-device-acceptance.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6_C2I_V9_REAL_DEVICE_ACCEPTANCE.md"), "utf8");

test("V9 records the real-device persistence failure without storing the screenshot", () => {
  assert.equal(contract.status, "FAIL");
  assert.equal(contract.acceptance_method, "human_text_attestation");
  assert.equal(contract.acceptance_passed, false);
  assert.deepEqual(contract.observed_result, {
    inside_shopify_admin: true,
    app_home_rendered: false,
    failure_code: "SHOPIFY_INSTALL_PERSISTENCE_FAILED",
    screenshot_stored_in_repository: false,
  });
  assert.equal(contract.accepted_reply, "App Home açıldı; Data Sources / Platforms açıldı; Connect'e basmadım.");
  assert.equal(contract.image_upload_required, false);
  assert.equal(contract.png_required, false);
  assert.equal(contract.sha256_required, false);
  assert.equal(contract.manifest_required, false);
  assert.match(doc, /ekran görüntüsü, PNG dönüşümü, SHA-256/);
  assert.match(doc, /tek satır yeterlidir/);
  assert.match(doc, /FAIL — SHOPIFY_INSTALL_PERSISTENCE_FAILED/);
});

test("V9 text confirmation does not authorize provider contact", () => {
  assert.ok(contract.required_observations.includes("provider_connect_not_pressed"));
  assert.ok(contract.required_observations.includes("provider_oauth_or_api_contact_not_started"));
  assert.ok(contract.forbidden_interactions.includes("production_mutation"));
  assert.equal(contract.next_on_pass, "E10_T6_C2I_V10A_KLAVIYO_CONNECT_MODAL");
  assert.equal(contract.next_on_fail, "SHOPIFY_INSTALL_PERSISTENCE_CORRECTIVE");
  assert.match(doc, /Hiçbir provider `Connect` düğmesine basma/);
});

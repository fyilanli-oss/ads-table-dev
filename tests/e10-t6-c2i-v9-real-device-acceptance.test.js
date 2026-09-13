"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6-c2i-v9-real-device-acceptance.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6_C2I_V9_REAL_DEVICE_ACCEPTANCE.md"), "utf8");

test("V9 approval authorizes capture but cannot claim acceptance without evidence", () => {
  assert.equal(contract.execution_approved, true);
  assert.equal(contract.status, "AWAITING_REDACTED_REAL_DEVICE_EVIDENCE");
  assert.equal(contract.acceptance_passed, false);
  assert.equal(contract.capture_authority, "human_in_authenticated_shopify_admin");
  assert.deepEqual(contract.required_captures.map(({surface, route}) => ({surface, route})), [
    {surface: "app_home", route: "/shopify/app"},
    {surface: "data_sources_platforms", route: "/shopify/app/platforms"},
  ]);
  for (const capture of contract.required_captures) {
    assert.equal(capture.redacted_file, null);
    assert.equal(capture.sha256, null);
  }
});

test("V9 requires Shopify-native visuals and rejects imitation surfaces", () => {
  assert.deepEqual(contract.visual_gates, {
    inside_shopify_admin: true,
    official_shopify_components_rendered: true,
    app_navigation_visible: true,
    app_home_manage_data_sources_action_visible: true,
    platforms_provider_actions_visible: true,
    custom_admin_shell_absent: true,
    custom_component_css_absent: true,
    technical_release_marker_absent: true,
    nested_provider_iframe_absent: true,
  });
  assert.match(doc, /source\/test kapıları ile insan render kanıtı birlikte geçmelidir/);
});

test("capture evidence is redacted before it enters the repository", () => {
  assert.ok(Object.values(contract.privacy_gates).every(Boolean));
  assert.match(doc, /Repository'ye eklemeden \*\*önce\*\*/);
  assert.match(contract.acceptance_rule, /both redacted captures/);
  assert.match(contract.acceptance_rule, /human privacy attestation/);
});

test("V9 never authorizes Connect or provider contact", () => {
  assert.ok(Object.values(contract.interaction_gates).every(value => value === false));
  assert.equal(contract.next_on_pass, "E10_T6_C2I_V10A_KLAVIYO_CONNECT_MODAL");
  assert.equal(contract.next_on_fail, "E10_T6_C2I_V9_CORRECTIVE");
  assert.match(doc, /Hiçbir provider `Connect` düğmesine basma/);
  assert.match(doc, /V9 `PASS` verilene kadar V10-A dahil hiçbir Provider Connect execution alt paketi başlamaz/);
});

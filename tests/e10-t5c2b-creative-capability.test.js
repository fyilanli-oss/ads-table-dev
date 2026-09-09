"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t5c2b-creative-capability.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T5C2B_CREATIVE_CAPABILITY_DATA_MODEL.md"), "utf8");
const plan = fs.readFileSync(path.join(root, "codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"), "utf8");

test("Creative first slice is a metadata sidecar without Dataset or Funnel mutation", () => {
  assert.equal(contract.decision, "provider_specific_metadata_sidecar_no_dataset_v2_change");
  assert.equal(contract.first_slice.creative_metadata_enabled, true);
  assert.equal(contract.first_slice.creative_performance_enabled, false);
  assert.equal(contract.first_slice.dataset_v2_schema_change, false);
  assert.equal(contract.first_slice.funnel_hierarchy_change, false);
});

test("sidecar identity is temporal and rejects facts secrets and PII", () => {
  for (const key of ["analytical_entity_key", "provider_creative_or_asset_id", "association_effective_at"]) assert.ok(contract.sidecar_grain.includes(key));
  for (const key of ["purchase", "sales", "spend", "revenue", "customer_pii", "provider_token", "permanent_third_party_media_url"]) assert.ok(contract.sidecar_forbidden.includes(key));
});

test("all provider branches remain metadata-only and fail closed on performance", () => {
  assert.deepEqual(Object.keys(contract.providers), ["meta", "google_standard", "google_performance_max", "tiktok", "pinterest", "klaviyo"]);
  for (const provider of Object.values(contract.providers)) assert.equal(provider.first_slice, "metadata_only");
  assert.match(doc, /Parent Ad\/Asset Group ile Creative\/Asset satırları aynı toplama setine giremez/);
  assert.match(doc, /documented candidate.*production-ready veya parity-approved demek değildir/);
});

test("official references and revalidation limits are explicit", () => {
  for (const source of ["developers.facebook.com", "developers.google.com", "business-api.tiktok.com", "developers.pinterest.com", "developers.klaviyo.com"]) assert.match(doc, new RegExp(source.replace(".", "\\.")));
  assert.equal(contract.scope_gate, "no_new_scope_without_visible_output_and_human_approval");
});

test("Execution Plan closes C2 and advances only to Dashboard product freeze", () => {
  assert.match(plan, /E10-T5-C2-B — `Done` — Creative provider capability ve data model/);
  assert.match(plan, /E10-T5-C2 — `Done` — Ad Analysis/);
  assert.match(plan, /E10-T5-C3 — `Done` — Dashboard/);
  assert.match(plan, /E10-T5-C4 — `Product decision required` — Platforms/);
  assert.match(plan, /Dataset V2 ve Funnel hierarchy değişmez/);
});

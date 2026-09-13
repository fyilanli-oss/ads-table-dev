"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const artifact = JSON.parse(fs.readFileSync(path.join(__dirname, "../artifacts/e10-shopify/e10-t6b-migration-acceptance.json"), "utf8"));

test("E10-T6-B records a redacted one-time migration acceptance", () => {
  assert.equal(artifact.contract_version, "e10-t6b-migration-acceptance-v1");
  assert.equal(artifact.migration_version, "20260910100000");
  assert.equal(artifact.migration_applied, true);
  assert.equal(artifact.ledger_version_count, 1);
  assert.equal(artifact.outcome, "PASS_MIGRATION_ONLY");
});

test("remote Shopify storage remains empty and server-only before smoke", () => {
  assert.equal(artifact.shopify_installation_row_count, 0);
  assert.equal(artifact.shopify_installation_table.rls_enabled_and_forced, true);
  assert.equal(artifact.shopify_installation_table.browser_role_grant_count, 0);
  assert.ok(artifact.shopify_installation_table.service_role_grant_count > 0);
  assert.equal(artifact.shopify_installation_table.plaintext_token_column_count, 0);
  assert.deepEqual(artifact.managed_install_function, {count: 1, security: "invoker"});
});

test("migration evidence claims no Shopify or production contact", () => {
  assert.equal(artifact.shopify_contact, false);
  assert.equal(artifact.development_store_smoke, "PENDING_ENV_VISIBILITY");
  assert.equal(artifact.production_store_contact, false);
  assert.equal(artifact.secrets_or_identifiers_recorded, false);
  assert.doesNotMatch(JSON.stringify(artifact), /myshopify\.com|eyJ|supabase\.co|service_role_key/i);
});

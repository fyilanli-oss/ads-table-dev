"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const artifact = JSON.parse(fs.readFileSync(path.join(__dirname, "../artifacts/e10-shopify/e10-t6c-development-migration-acceptance.json"), "utf8"));

test("E10-T6-C records the approved development-only migrations", () => {
  assert.equal(artifact.contract_version, "e10-t6c-development-migration-acceptance-v1");
  assert.deepEqual(artifact.migrations, [
    "20260911130000_add_embedded_oauth_authority",
    "20260911150000_create_shopify_workspace_provider_connections"
  ]);
  assert.equal(artifact.outcome, "PASS_DEVELOPMENT_MIGRATION_ONLY");
});

test("E10-T6-C migration postcheck preserves the server-only boundary", () => {
  assert.equal(artifact.postcheck.oauth_authority_columns_present, true);
  assert.equal(artifact.postcheck.consume_contract_updated, true);
  assert.equal(artifact.postcheck.workspace_provider_connections_table_present, true);
  assert.equal(artifact.postcheck.rls_enabled_and_forced, true);
  assert.equal(artifact.postcheck.anonymous_role_denied, true);
  assert.equal(artifact.postcheck.authenticated_role_denied, true);
  assert.equal(artifact.postcheck.service_role_allowed, true);
});

test("E10-T6-C acceptance is redacted and made no external contact", () => {
  assert.equal(artifact.postcheck.embedded_transaction_row_count, 0);
  assert.equal(artifact.postcheck.workspace_provider_connection_row_count, 0);
  assert.equal(artifact.provider_contact, false);
  assert.equal(artifact.production_contact, false);
  assert.equal(artifact.secrets_or_identifiers_recorded, false);
  assert.doesNotMatch(JSON.stringify(artifact), /myshopify\.com|eyJ|supabase\.co|service_role_key/i);
});

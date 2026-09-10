"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const {developmentSmokePreflight} = require("../scripts/e10-t6b-development-smoke-preflight");

const completeEnv = Object.freeze({
  SHOPIFY_API_KEY: "api-key-must-not-leak",
  SHOPIFY_API_SECRET: "api-secret-must-not-leak",
  SHOPIFY_APP_URL: "https://example.test",
  SHOPIFY_DEV_STORE: "example.myshopify.com",
  PROVIDER_TOKEN_ACTIVE_KEY_ID: "test-key",
  PROVIDER_TOKEN_ENCRYPTION_KEYS: JSON.stringify({"test-key": Buffer.alloc(32, 7).toString("base64")}),
  SUPABASE_URL: "https://database.example.test",
  SUPABASE_SERVICE_ROLE_KEY: "service-role-must-not-leak",
});

test("development smoke preflight reports readiness without exposing values or lengths", () => {
  const result = developmentSmokePreflight(completeEnv);
  assert.deepEqual(result, {
    contract_version: "e10-t6b-development-smoke-preflight-v1",
    ready: true,
    shopify: {configured: true, visible_count: 4, required_count: 4, contract_valid: true},
    token_keyring: {valid_in_active_process: true, visible_count: 2, required_count: 2},
    remote_database: {credentials_visible: true},
    values_or_lengths_exposed: false,
  });
  const serialized = JSON.stringify(result);
  for (const value of Object.values(completeEnv)) assert.equal(serialized.includes(value), false);
});

test("development smoke preflight remains fail-closed when the keyring is not visible", () => {
  const {PROVIDER_TOKEN_ENCRYPTION_KEYS, ...env} = completeEnv;
  const result = developmentSmokePreflight(env);
  assert.deepEqual(result.token_keyring, {valid_in_active_process: false, visible_count: 1, required_count: 2});
  assert.equal(result.ready, false);
});

test("operator guidance identifies the canonical secret source without embedding key material", () => {
  const doc = fs.readFileSync(path.join(__dirname, "../docs/E10_T6B_MANAGED_INSTALLATION_BOOTSTRAP.md"), "utf8");
  assert.match(doc, /Shopify Partner Dashboard'dan alınan bir credential değildir/);
  assert.match(doc, /external secret kayıtlarının bulunmadığı anlamına gelmez/);
  assert.match(doc, /yeniden eklenmez, değiştirilmez veya rotate edilmez/);
  assert.match(doc, /kriptografik olarak rastgele 32 byte olup base64 kodlanır/);
  assert.doesNotMatch(doc, /[A-Za-z0-9+/]{43}=/);
});

test("handoff makes the next-task decision and acceptance sequence unambiguous", () => {
  const handoff = fs.readFileSync(path.join(__dirname, "../codex-input/E10_T6B_DEVELOPMENT_STORE_SMOKE_HANDOFF_TR.md"), "utf8");
  assert.match(handoff, /PR #186/);
  assert.match(handoff, /secret injection'lı \*\*yeni process\*\*/);
  assert.match(handoff, /token_keyring\.valid_in_active_process: true/);
  assert.match(handoff, /remote_database\.credentials_visible: true/);
  for (const gate of ["session token doğrulaması", "offline token exchange", "Admin API Shop identity", "encrypted token persistence", "reopen/reinstall idempotency"]) {
    assert.match(handoff, new RegExp(gate));
  }
  assert.match(handoff, /Production store\/credential/);
  assert.doesNotMatch(handoff, /[A-Za-z0-9+/]{43}=/);
});

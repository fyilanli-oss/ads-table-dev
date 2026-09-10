"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
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
    token_keyring: {configured: true, visible_count: 2, required_count: 2},
    remote_database: {credentials_visible: true},
    values_or_lengths_exposed: false,
  });
  const serialized = JSON.stringify(result);
  for (const value of Object.values(completeEnv)) assert.equal(serialized.includes(value), false);
});

test("development smoke preflight remains fail-closed when the keyring is not visible", () => {
  const {PROVIDER_TOKEN_ENCRYPTION_KEYS, ...env} = completeEnv;
  const result = developmentSmokePreflight(env);
  assert.deepEqual(result.token_keyring, {configured: false, visible_count: 1, required_count: 2});
  assert.equal(result.ready, false);
});

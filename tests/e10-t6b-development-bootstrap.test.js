"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");
const {inspectDevelopmentBootstrap} = require("../src/shopify/development-bootstrap");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6b-development-bootstrap.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6B_DEVELOPMENT_APP_BOOTSTRAP.md"), "utf8");

test("bootstrap remains fail-closed without credentials or CLI", () => {
  const report = inspectDevelopmentBootstrap({env: {}, cliAvailable: false});
  assert.equal(report.status, "BLOCKED");
  assert.equal(report.safe_code, "BLOCKED_BOOTSTRAP_ENV");
  assert.deepEqual(report.missing, contract.preflight.required_environment_names);
  assert.equal(report.shopify_contact, false);
  assert.equal(report.production_contact, false);
});

test("ready report exposes configuration but never credentials", () => {
  const env = {
    SHOPIFY_API_KEY: "public-key",
    SHOPIFY_API_SECRET: "super-secret-value",
    SHOPIFY_APP_URL: "https://dev.adstable.app/",
    SHOPIFY_DEV_STORE: "ads-table-dev.myshopify.com",
    SHOPIFY_SCOPES: "read_reports",
  };
  const report = inspectDevelopmentBootstrap({env, cliAvailable: true});
  assert.equal(report.status, "READY_FOR_MANUAL_BOOTSTRAP");
  assert.equal(report.callback_url, "https://dev.adstable.app/auth/shopify/callback");
  assert.deepEqual(report.scopes, ["read_reports"]);
  assert.doesNotMatch(JSON.stringify(report), /public-key|super-secret-value/);
});

test("unapproved scopes and unsafe endpoint values are rejected", () => {
  const base = {SHOPIFY_API_KEY: "k", SHOPIFY_API_SECRET: "s", SHOPIFY_APP_URL: "https://dev.adstable.app", SHOPIFY_DEV_STORE: "dev.myshopify.com"};
  assert.throws(() => inspectDevelopmentBootstrap({env: {...base, SHOPIFY_SCOPES: "read_orders"}, cliAvailable: true}), /SHOPIFY_SCOPE_NOT_APPROVED/);
  assert.throws(() => inspectDevelopmentBootstrap({env: {...base, SHOPIFY_APP_URL: "http://dev.adstable.app"}, cliAvailable: true}), /HTTPS/);
  assert.throws(() => inspectDevelopmentBootstrap({env: {...base, SHOPIFY_DEV_STORE: "store.example.com"}, cliAvailable: true}), /myshopify\.com/);
});

test("repository evidence records approval and no Shopify contact", () => {
  assert.equal(contract.development_approval_received, true);
  assert.equal(contract.outcome, "BLOCKED");
  assert.equal(contract.shopify_contact, false);
  assert.equal(contract.development_store_contact, false);
  assert.equal(contract.production_contact, false);
  assert.match(doc, /Development onayı.*Alındı/);
  assert.match(doc, /T6-D öncesinde ShopifyQL query/);
});

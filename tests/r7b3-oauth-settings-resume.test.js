"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {RETURN_TARGET, LEGACY_RETURN_TARGET} = require("../src/shopify/embedded-provider-oauth");
const {EMBEDDED_RETURN_TARGET} = require("../src/oauth/transaction-boundary");
const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");

test("R7-B3 creates new OAuth transactions for Settings and retains only the exact legacy alias", () => {
  assert.equal(RETURN_TARGET, "/shopify/app/settings");
  assert.equal(EMBEDDED_RETURN_TARGET, RETURN_TARGET);
  assert.equal(LEGACY_RETURN_TARGET, "/shopify/app/platforms");

  const sql = fs.readFileSync(path.join(__dirname, "..", "supabase", "migrations", "20260929100311_r7b3_allow_settings_oauth_return_target.sql"), "utf8");
  assert.match(sql, /return_target in \('\/shopify\/app\/settings', '\/shopify\/app\/platforms'\)/i);
  assert.doesNotMatch(sql, /https?:\/\//i);
  assert.doesNotMatch(sql, /delete\s+from|update\s+public\.oauth_transactions/i);
});

test("R7-B3 Settings UI exposes resumable pending setup and provider-specific guidance", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: true});
  for (const provider of ["meta", "google_ads", "klaviyo"]) {
    assert.match(html, new RegExp(`id="${provider}-resume" hidden`));
    assert.match(html, new RegExp(`<s-button variant="secondary" id="${provider}-resume-action">Resume setup<\\/s-button>`));
  }
  assert.match(html, /Facebook account that owns or can access/);
  assert.match(html, /Google may describe the consent broadly/);
  assert.match(html, /one verified Klaviyo account/);
  assert.match(html, /Historical analytics already stored will remain available/);
  assert.match(html, /Other provider connections are not affected/);
});

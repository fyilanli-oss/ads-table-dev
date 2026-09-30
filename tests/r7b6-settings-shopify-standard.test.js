"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");

test("R7-B6 Settings uses Shopify-native grouped cards and provider rows", () => {
  const html = renderEmbeddedPlatforms({
    clientId: "test-client",
    providerOAuthEnabled: true,
    providerAvailability: {meta: true, google_ads: true, klaviyo: true},
  });

  assert.match(html, /<s-stack gap="small">\s*<s-heading>Reporting Currency<\/s-heading>\s*<s-section>/);
  assert.match(html, /<s-stack gap="small">\s*<s-heading>Platforms<\/s-heading>\s*<s-section>/);
  assert.equal((html.match(/<s-divider><\/s-divider>/g) || []).length, 2);
  assert.match(html, /<s-heading>Meta<\/s-heading>/);
  assert.match(html, /<s-heading>Google Ads<\/s-heading>/);
  assert.match(html, /<s-heading>Klaviyo<\/s-heading>/);
  assert.doesNotMatch(html, /advertising performance and spend|Email performance and estimated 30-day spend/);
});

test("R7-B6 provider actions use neutral, success and critical Shopify semantics", () => {
  const html = renderEmbeddedPlatforms({
    clientId: "test-client",
    providerOAuthEnabled: true,
    providerAvailability: {meta: true, google_ads: true, klaviyo: true},
  });

  assert.match(html, /<s-clickable background="subdued"[^>]*minBlockSize="32px"[^>]*id="meta-connect-action"[^>]*commandFor="meta-connect-modal"/);
  assert.doesNotMatch(html, /id="meta-connect-action" variant="primary"/);
  assert.match(html, /<s-clickable background="subdued"[^>]*minBlockSize="32px"[^>]*id="meta-resume-action"[^>]*>.*Resume setup.*<\/s-clickable>/);
  assert.match(html, /<s-badge tone="success" size="large-100">Connected<\/s-badge>/);
  assert.match(html, /style="display:inline-flex;inline-size:auto" id="meta-reporting-action"/);
  assert.match(html, /<s-clickable[^>]*style="display:inline-flex;inline-size:auto;background:#FDE8E7"[^>]*commandFor="meta-disconnect-modal"/);
  assert.match(html, /<s-clickable background="subdued"[^>]*id="meta-reporting-action"[^>]*>.*Reporting account.*<\/s-clickable>/);
  assert.match(html, />Update spend<\/s-text><\/s-clickable>/);
  assert.match(html, />Change value<\/s-text><\/s-clickable>/);
  assert.doesNotMatch(html, />Correct value<\//);
  assert.doesNotMatch(html, /slot="breadcrumb-actions"[^>]*>Dashboard<\/s-link>/);
});

test("R7-B6 leaves provider OAuth and disconnect modal copy intact", () => {
  const html = renderEmbeddedPlatforms({
    clientId: "test-client",
    providerOAuthEnabled: true,
    providerAvailability: {meta: true, google_ads: true, klaviyo: true},
  });

  assert.match(html, /Continue with the Facebook account that owns or can access/);
  assert.match(html, /Historical analytics already stored will remain available/);
});

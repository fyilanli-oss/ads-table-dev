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

  assert.match(html, /<s-heading>Reporting Currency<\/s-heading>\s*<s-section>/);
  assert.match(html, /<s-heading>Platforms<\/s-heading>\s*<s-section>/);
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

  assert.match(html, /id="meta-connect-action" commandFor="meta-connect-modal"/);
  assert.doesNotMatch(html, /id="meta-connect-action" variant="primary"/);
  assert.match(html, /id="meta-resume-action">Resume setup<\/s-button>/);
  assert.match(html, /<s-badge tone="success">Connected<\/s-badge>/);
  assert.match(html, /<s-button tone="critical" commandFor="meta-disconnect-modal"/);
  assert.match(html, /id="meta-reporting-action"[^>]*>Reporting account<\/s-button>/);
  assert.match(html, />Update spend<\/s-button>/);
  assert.match(html, />Change value<\/s-button>/);
  assert.doesNotMatch(html, />Correct value<\/s-button>/);
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

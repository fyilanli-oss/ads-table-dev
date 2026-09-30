"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");

test("R7-B6 Settings uses two durable Shopify-native cards and separated provider rows", () => {
  const html = renderEmbeddedPlatforms({
    clientId: "test-client",
    providerOAuthEnabled: true,
    providerAvailability: {meta: true, google_ads: true, klaviyo: true},
  });

  assert.match(html, /<s-page heading="Settings" inlineSize="base">/);
  assert.match(html, /<s-heading>Reporting Currency<\/s-heading>\s*<s-box[^>]+accessibilityLabel="Reporting Currency">/);
  assert.match(html, /<s-heading>Platforms<\/s-heading>\s*<s-box id="provider-sections"[^>]+accessibilityLabel="Platforms">/);
  assert.doesNotMatch(html, /<s-section heading="Reporting Currency">|<s-section heading="Platforms">/);
  assert.equal((html.match(/accessibilityLabel="(?:Reporting Currency|Platforms)"/g) || []).length, 2);
  assert.equal((html.match(/background="base" borderWidth="base" borderColor="base" borderRadius="base"/g) || []).length, 2);
  assert.equal((html.match(/<s-divider><\/s-divider>/g) || []).length, 2);
  assert.match(html, /<s-heading>Meta<\/s-heading>/);
  assert.match(html, /<s-heading>Google Ads<\/s-heading>/);
  assert.match(html, /<s-heading>Klaviyo<\/s-heading>/);
  assert.doesNotMatch(html, /advertising performance and spend|Email performance and estimated 30-day spend/);
  assert.equal((html.match(/<s-query-container>/g) || []).length, 3);
  assert.equal((html.match(/gridTemplateColumns="@container \(inline-size > 700px\) 1fr auto, 1fr"/g) || []).length, 3);
});

test("R7-B6 provider actions use only official Shopify action and status semantics", () => {
  const html = renderEmbeddedPlatforms({
    clientId: "test-client",
    providerOAuthEnabled: true,
    providerAvailability: {meta: true, google_ads: true, klaviyo: true},
  });

  assert.match(html, /<s-button variant="secondary" id="meta-connect-action"[^>]*>Connect<\/s-button>/);
  assert.match(html, /<s-button variant="secondary" id="meta-resume-action">Resume setup<\/s-button>/);
  assert.match(html, /<s-badge tone="success" size="base">Connected<\/s-badge>/);
  assert.match(html, /<s-button variant="secondary" tone="critical"[^>]*>Disconnect<\/s-button>/);
  assert.match(html, /<s-button variant="secondary" id="meta-reporting-action"[^>]*>Reporting account<\/s-button>/);
  assert.match(html, /<s-button variant="secondary"[^>]*>Update spend<\/s-button>/);
  assert.match(html, /<s-button variant="secondary" id="klaviyo-spend-correct-open"[^>]*>Change value<\/s-button>/);
  assert.doesNotMatch(html, /<s-clickable\b|style\s*=|#[0-9a-f]{3,8}\b/i);
  assert.doesNotMatch(html, /class(?:Name)?\s*=/i);
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

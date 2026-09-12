"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");
const {enabled} = require("../src/shopify/runtime");
const {createEmbeddedProviderTokenExchanges, normalize} = require("../src/shopify/embedded-provider-token-exchange");

test("embedded Platforms renders all provider Connect actions against the canonical start boundary", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: true});
  for (const provider of ["meta", "google_ads", "klaviyo", "tiktok", "pinterest"]) {
    assert.match(html, new RegExp(`data-provider="${provider}"`));
  }
  assert.match(html, /window\.shopify\.idToken/);
  assert.match(html, /\/api\/shopify\/providers\//);
  assert.match(html, /window\.open\(body\.authorization_url,"_top"\)/);
  assert.doesNotMatch(html, /workspace[_-]id|shop[_-]id|user[_-]id/i);
});

test("embedded Platforms keeps Connect actions disabled until runtime activation", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: false});
  assert.equal((html.match(/ disabled/g) || []).length, 5);
});

test("provider token exchanges normalize object and nested TikTok token responses", () => {
  assert.deepEqual(normalize({access_token: "access", refresh_token: "refresh"}), {accessToken: "access", refreshToken: "refresh"});
  assert.deepEqual(normalize({data: {access_token: "access"}}), {accessToken: "access", refreshToken: null});
  assert.throws(() => normalize({data: {}}), /INVALID_PROVIDER_TOKEN_RESPONSE/);
});

test("provider token exchange clients keep credentials server-side and use exact endpoints", async () => {
  const calls = [];
  const exchanges = createEmbeddedProviderTokenExchanges({fetchImpl: async (url, options) => {
    calls.push({url, options});
    return {ok: true, json: async () => ({access_token: "access", refresh_token: "refresh"})};
  }});
  for (const exchange of Object.values(exchanges)) {
    await exchange({code: "code", redirectUri: "https://app.test/callback", pkceVerifier: "verifier", clientId: "client", clientSecret: "secret"});
  }
  assert.equal(calls.length, 5);
  assert.equal(calls.every(call => !call.url.includes("secret")), true);
  assert.deepEqual(calls.map(call => new URL(call.url).hostname), ["graph.facebook.com", "oauth2.googleapis.com", "a.klaviyo.com", "business-api.tiktok.com", "api.pinterest.com"]);
});

test("provider OAuth remains off unless the explicit activation flag is true", () => {
  assert.equal(enabled(undefined), false);
  assert.equal(enabled(""), false);
  assert.equal(enabled("true"), true);
});

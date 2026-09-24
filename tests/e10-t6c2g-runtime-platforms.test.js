"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");
const {enabled, providerRuntimeReady, registerShopifyRuntime} = require("../src/shopify/runtime");
const {createEmbeddedProviderTokenExchanges, normalize} = require("../src/shopify/embedded-provider-token-exchange");

test("embedded Platforms renders the R7-A currency gate, three active providers, and two parked providers", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: true});
  for (const provider of ["meta", "google_ads", "klaviyo"]) {
    assert.match(html, new RegExp(`data-provider="${provider}"`));
  }
  assert.doesNotMatch(html, /data-provider="tiktok"/);
  assert.doesNotMatch(html, /data-provider="pinterest"/);
  assert.match(html, /TikTok connection is parked for a later release/);
  assert.match(html, /Pinterest connection is not available in this release/);
  assert.match(html, /<s-paragraph>Parked<\/s-paragraph>/);
  assert.doesNotMatch(html, /Connect (?:Meta|Google Ads|Klaviyo|TikTok|Pinterest) to this Shopify workspace/);
  assert.match(html, /fetch\("\/api\/shopify\/providers\/klaviyo\/accounts" \+ path/);
  assert.match(html, /request\("\/status"\)/);
  assert.match(html, /window\.shopify\.idToken/);
  assert.match(html, /<div id="currency-setup" hidden>[\s\S]*heading="Finish setup"/);
  assert.match(html, /currencySetup\.hidden = true;[\s\S]*providerSections\.hidden = true;[\s\S]*Open AdsTable from Shopify Admin/);
  assert.doesNotMatch(html, /id="reporting-currency-summary"/);
  assert.match(html, /id="platforms-currency-modal" heading="Choose reporting currency" size="small-100"/);
  assert.match(html, /\/api\/shopify\/workspace\/reporting-currency/);
  assert.match(html, /<s-modal id="klaviyo-connect-modal" heading="Connect Klaviyo to AdsTable\?" size="small-100">/);
  assert.match(html, /<s-modal id="klaviyo-account-modal" heading="Finish Klaviyo setup">/);
  assert.match(html, /commandFor="klaviyo-connect-modal" command="--show"/);
  assert.match(html, /\/api\/shopify\/providers\//);
  assert.match(html, /open\(body\.authorization_url, "_top"\)/);
  assert.match(html, /cdn\.shopify\.com\/shopifycloud\/polaris-1\.js/);
  assert.match(html, /<s-app-nav>/);
  assert.match(html, /<s-page heading="Data sources">/);
  assert.equal((html.match(/data-provider=/g) || []).length, 3);
  assert.doesNotMatch(html, /<style>|<iframe/i);
  assert.doesNotMatch(html, /workspace[_-]id|shop[_-]id|user[_-]id/i);
});

test("embedded Platforms keeps Connect actions disabled until runtime activation", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: false});
  assert.equal((html.match(/ disabled/g) || []).length, 5);
  assert.match(html, /Connection setup unavailable/);
  assert.doesNotMatch(html, /Checking connection status/);
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

test("Google Ads account-discovery configuration cannot disable Klaviyo OAuth routing", () => {
  const env = {
    PROVIDER_TOKEN_ACTIVE_KEY_ID: "v1",
    PROVIDER_TOKEN_ENCRYPTION_KEYS: JSON.stringify({v1: Buffer.alloc(32, 1).toString("base64")}),
  };
  assert.equal(providerRuntimeReady({env, oauthTransactionStore: {}}), true);
  assert.equal(env.GOOGLE_ADS_DEVELOPER_TOKEN, undefined);
});

test("incomplete provider activation stays isolated without crashing Shopify App Home", () => {
  const routes = {};
  const app = {
    get: (path, handler) => { routes[`GET ${path}`] = handler; },
    post: (path, handler) => { routes[`POST ${path}`] = handler; },
  };
  const result = registerShopifyRuntime({
    app,
    env: {
      SHOPIFY_API_KEY: "key",
      SHOPIFY_API_SECRET: "secret",
      SHOPIFY_APP_URL: "https://dev.adstable.app",
      SHOPIFY_DEV_STORE: "store.myshopify.com",
      SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED: "true",
    },
    supabaseAdmin: {from: () => ({})},
  });
  assert.deepEqual(result, {enabled: true, providerOAuthEnabled: false, providerOAuthRequested: true, providerAvailability: {meta: false, google_ads: false, klaviyo: false}});
  assert.equal(typeof routes["GET /"], "function");
  assert.equal(typeof routes["GET /shopify/app/platforms"], "function");
  assert.equal(routes["POST /api/shopify/providers/meta/oauth/start"], undefined);
});


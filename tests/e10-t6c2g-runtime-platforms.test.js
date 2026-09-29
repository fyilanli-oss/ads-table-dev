"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {renderEmbeddedPlatforms} = require("../src/shopify/embedded-app-home");
const {enabled, providerRuntimeReady, registerShopifyRuntime} = require("../src/shopify/runtime");
const {createEmbeddedProviderTokenExchanges, normalize} = require("../src/shopify/embedded-provider-token-exchange");

test("embedded Settings renders immutable currency setup and only three active providers", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: true});
  for (const provider of ["meta", "google_ads", "klaviyo"]) {
    assert.match(html, new RegExp(`data-provider="${provider}"`));
  }
  assert.doesNotMatch(html, /data-provider="tiktok"/);
  assert.doesNotMatch(html, /data-provider="pinterest"/);
  assert.doesNotMatch(html, /TikTok|Pinterest|Parked|Unavailable/);
  assert.doesNotMatch(html, /Connect (?:Meta|Google Ads|Klaviyo|TikTok|Pinterest) to this Shopify workspace/);
  assert.match(html, /fetch\("\/api\/shopify\/providers\/klaviyo\/accounts" \+ path/);
  assert.match(html, /request\("\/status"\)/);
  assert.match(html, /window\.shopify\.idToken/);
  assert.match(html, /<s-section heading="Reporting Currency">/);
  assert.match(html, /id="reporting-currency-summary" hidden/);
  assert.match(html, /cannot be changed after confirmation/);
  assert.match(html, /id="review-reporting-currency"[^>]*>Review selection/);
  assert.match(html, /id="save-reporting-currency"[^>]*>Confirm reporting currency/);
  assert.match(html, /Removing the app does not delete data or reset this currency/);
  assert.match(html, /currencySetup\.hidden = true;[\s\S]*providerSections\.hidden = true;[\s\S]*Open AdsTable from Shopify Admin/);
  assert.match(html, /id="platforms-currency-modal" heading="Choose reporting currency" size="small-100"/);
  assert.match(html, /\/api\/shopify\/workspace\/reporting-currency/);
  assert.match(html, /<s-modal id="klaviyo-connect-modal" heading="Connect Klaviyo to AdsTable\?" size="small-100">/);
  assert.match(html, /<s-modal id="klaviyo-account-modal" heading="Finish Klaviyo setup">/);
  assert.match(html, /commandFor="klaviyo-connect-modal" command="--show"/);
  for (const provider of ["meta", "google_ads", "klaviyo"]) {
    assert.match(html, new RegExp(`id="${provider}-resume" hidden`));
    assert.match(html, new RegExp(`id="${provider}-resume-action">Resume setup`));
  }
  assert.match(html, /Facebook account that owns or can access the Meta ad accounts/);
  assert.match(html, /Google may describe the consent broadly/);
  assert.match(html, /select one verified Klaviyo account and enter its Estimated 30-Day Klaviyo Email Spend/);
  assert.match(html, /SMS costs are not included/);
  assert.match(html, /id="klaviyo-spend-update-modal"/);
  assert.match(html, /id="klaviyo-spend-correct-modal"/);
  assert.match(html, /Other provider connections are not affected/);
  assert.match(html, /disconnected period may not be recovered automatically/);
  assert.match(html, /\/api\/shopify\/providers\//);
  assert.match(html, /open\(body\.authorization_url, "_top"\)/);
  assert.match(html, /cdn\.shopify\.com\/shopifycloud\/polaris-1\.js/);
  assert.match(html, /<s-app-nav>/);
  assert.match(html, /<s-page heading="Settings">/);
  assert.match(html, /<s-section heading="Platforms">/);
  assert.equal((html.match(/data-provider=/g) || []).length, 3);
  assert.doesNotMatch(html, /<style>|<iframe/i);
  assert.doesNotMatch(html, /workspace[_-]id|shop[_-]id|user[_-]id/i);
});

test("embedded Platforms keeps Connect actions disabled until runtime activation", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: false});
  assert.equal((html.match(/ disabled/g) || []).length, 3);
  assert.match(html, /Connection setup unavailable/);
  assert.doesNotMatch(html, /Checking connection status/);
});

test("provider token exchanges normalize object and nested TikTok token responses", () => {
  assert.deepEqual(normalize({access_token: "access", refresh_token: "refresh", expires_in: 3600, scope: "accounts:read campaigns:read,flows:read"}), {accessToken: "access", refreshToken: "refresh", expiresIn: 3600, scopes: ["accounts:read", "campaigns:read", "flows:read"]});
  assert.deepEqual(normalize({data: {access_token: "access"}}), {accessToken: "access", refreshToken: null, expiresIn: null, scopes: []});
  assert.throws(() => normalize({data: {}}), /INVALID_PROVIDER_TOKEN_RESPONSE/);
});

test("provider token exchange clients keep credentials server-side and use exact endpoints", async () => {
  const calls = [];
  const exchanges = createEmbeddedProviderTokenExchanges({now: () => new Date("2026-09-25T12:00:00.000Z"), fetchImpl: async (url, options) => {
    calls.push({url, options});
    if (url.includes("/debug_token")) return {ok: true, json: async () => ({data: {app_id: "client", is_valid: true, expires_at: 1790341200, scopes: ["ads_read"]}})};
    const metaLongLived = url.includes("graph.facebook.com") && String(options.body || "").includes("fb_exchange_token");
    if (url.includes("oauth2.googleapis.com")) return {ok: true, json: async () => ({access_token: "access", refresh_token: "refresh", expires_in: 3600, scope: "https://www.googleapis.com/auth/adwords"})};
    return {ok: true, json: async () => ({access_token: metaLongLived ? "meta-long" : "access", refresh_token: "refresh"})};
  }});
  for (const exchange of Object.values(exchanges)) {
    await exchange({code: "code", redirectUri: "https://app.test/callback", pkceVerifier: "verifier", clientId: "client", clientSecret: "secret"});
  }
  assert.equal(calls.length, 7);
  assert.equal(calls.every(call => !call.url.includes("secret")), true);
  assert.deepEqual(calls.map(call => new URL(call.url).hostname), ["graph.facebook.com", "graph.facebook.com", "graph.facebook.com", "oauth2.googleapis.com", "a.klaviyo.com", "business-api.tiktok.com", "api.pinterest.com"]);
  assert.equal(calls[1].options.body.includes("grant_type=fb_exchange_token"), true);
  assert.equal(calls[2].options.headers.Authorization, "Bearer client|secret");
  assert.equal(calls[2].url.includes("meta-long"), true);
});

test("Meta exchange stores only a validated long-lived token with authoritative scopes and expiry", async () => {
  const calls = [];
  const exchange = createEmbeddedProviderTokenExchanges({
    now: () => new Date("2026-09-25T12:00:00.000Z"),
    fetchImpl: async (url, options) => {
      calls.push({url, options});
      if (url.includes("/debug_token")) return {ok: true, json: async () => ({data: {app_id: "app-1", is_valid: true, expires_at: 1790341200, scopes: ["public_profile", "ads_read"]}})};
      if (String(options.body || "").includes("fb_exchange_token")) return {ok: true, json: async () => ({access_token: "long-lived"})};
      return {ok: true, json: async () => ({access_token: "short-lived"})};
    },
  });
  assert.deepEqual(await exchange.meta({code: "code", redirectUri: "https://app/callback", clientId: "app-1", clientSecret: "secret"}), {
    accessToken: "long-lived",
    refreshToken: null,
    expiresIn: 3600,
    scopes: ["public_profile", "ads_read"],
  });
  assert.equal(calls.length, 3);
});

test("Meta validation fails closed for wrong app, missing ads_read, and expired authority", async () => {
  const cases = [
    [{app_id: "other", is_valid: true, expires_at: 1790341200, scopes: ["ads_read"]}, "META_TOKEN_APP_MISMATCH"],
    [{app_id: "app-1", is_valid: true, expires_at: 1790341200, scopes: ["public_profile"]}, "META_TOKEN_SCOPE_MISSING"],
    [{app_id: "app-1", is_valid: true, expires_at: 1790337599, scopes: ["ads_read"]}, "META_TOKEN_REAUTHORIZATION_REQUIRED"],
  ];
  for (const [data, code] of cases) {
    const exchange = createEmbeddedProviderTokenExchanges({
      now: () => new Date("2026-09-25T12:00:00.000Z"),
      fetchImpl: async (url, options) => {
        if (url.includes("/debug_token")) return {ok: true, json: async () => ({data})};
        if (String(options.body || "").includes("fb_exchange_token")) return {ok: true, json: async () => ({access_token: "long-lived"})};
        return {ok: true, json: async () => ({access_token: "short-lived"})};
      },
    });
    await assert.rejects(
      () => exchange.meta({code: "code", redirectUri: "https://app/callback", clientId: "app-1", clientSecret: "secret"}),
      error => error.code === code && !JSON.stringify(error).includes("long-lived") && !JSON.stringify(error).includes("secret"),
    );
  }
});

test("Platforms surfaces a controlled Meta reconnect action without exposing token details", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: true});
  assert.match(html, /id="meta-connect-action"/);
  assert.match(html, /oauth_error"\) === "reauthorization_required"/);
  assert.match(html, /reconnect\.textContent = "Reconnect Meta"/);
  assert.match(html, /connectAction\.textContent = 'Reconnect Meta'/);
  assert.match(html, /Meta authorization must be renewed before account selection/);
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

test("Google Ads runtime accepts the legacy developer-token name only as a server-side fallback", () => {
  const source = registerShopifyRuntime.toString();
  assert.match(source, /env\.GOOGLE_ADS_DEVELOPER_TOKEN \|\| env\.GOOGLE_DEVELOPER_TOKEN/);
  assert.doesNotMatch(source, /NEXT_PUBLIC_(?:GOOGLE_ADS_)?DEVELOPER_TOKEN/);
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
  assert.equal(typeof routes["GET /shopify/app/settings"], "function");
  assert.equal(typeof routes["GET /shopify/app/platforms"], "function");
  assert.equal(typeof routes["GET /shopify/app/funnel"], "function");
  assert.equal(typeof routes["GET /shopify/app/analysis"], "function");
  assert.equal(routes["POST /api/shopify/providers/meta/oauth/start"], undefined);
});


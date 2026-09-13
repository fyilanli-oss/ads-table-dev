"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {PROVIDERS} = require("../src/routes/shopify-provider-oauth-routes");
const {createEmbeddedProviderOAuthAdapters} = require("../src/shopify/embedded-provider-oauth-adapters");

function fixture(overrides = {}) {
  const calls = [];
  const authority = {authority: "shopify_verified_session", shop_id: "shop-a", workspace_id: "workspace-a", shopify_user_id: "user-a"};
  const providerStrategies = Object.fromEntries(PROVIDERS.map(provider => [provider, {
    redirectUri: `https://app.test/api/shopify/providers/${provider}/oauth/callback`,
    buildAuthorizationUrl: async input => (calls.push(["authorize", provider, input]), `https://provider.test/${provider}`),
    exchangeCode: async input => (calls.push(["exchange", provider, input]), {accessToken: `${provider}-access`} ),
  }]));
  const adapters = createEmbeddedProviderOAuthAdapters({
    authenticateEmbedded: async input => (calls.push(["authenticate", input]), authority),
    createEmbeddedTransaction: async (context, provider, redirectUri) => (calls.push(["create", context, provider, redirectUri]), {state: `${provider}-state`}),
    consumeTransaction: overrides.consumeTransaction || (async (state, provider) => ({surface: "shopify_embedded", provider, return_target: "/shopify/app/platforms", user_id: null, ...authority, state})),
    connectionStore: {writeFromOAuthTransaction: async input => calls.push(["write", input])},
    providerStrategies,
  });
  return {adapters, calls};
}

test("composition requires one complete strategy for every allowlisted provider", () => {
  const required = {authenticateEmbedded() {}, createEmbeddedTransaction() {}, consumeTransaction() {}, connectionStore: {writeFromOAuthTransaction() {}}, providerStrategies: {}};
  assert.throws(() => createEmbeddedProviderOAuthAdapters(required), /providerStrategies\.meta\.redirectUri/);
  assert.deepEqual(PROVIDERS, ["meta", "google_ads", "klaviyo", "tiktok", "pinterest"]);
});

test("all provider starts derive authority from the verified embedded session", async () => {
  const {adapters, calls} = fixture();
  for (const provider of PROVIDERS) {
    const result = await adapters[provider].start({sessionToken: `${provider}-session`});
    assert.equal(result.navigation, "top_level");
  }
  assert.equal(calls.filter(([name]) => name === "authenticate").length, PROVIDERS.length);
  assert.equal(calls.filter(([name]) => name === "create").length, PROVIDERS.length);
});

test("callback persistence is bound to consumed transaction authority, not caller input", async () => {
  const {adapters, calls} = fixture();
  await adapters.meta.callback({state: "opaque", code: "code", workspace_id: "workspace-attacker"});
  const write = calls.find(([name]) => name === "write")[1];
  assert.equal(write.transaction.workspace_id, "workspace-a");
  assert.equal(write.transaction.shop_id, "shop-a");
  assert.equal(write.accessToken, "meta-access");
});

test("replayed or cross-provider state stops before exchange and persistence", async () => {
  const {adapters, calls} = fixture({consumeTransaction: async () => null});
  await assert.rejects(() => adapters.meta.callback({state: "replayed", code: "code"}), /INVALID_EMBEDDED_OAUTH_TRANSACTION/);
  assert.equal(calls.some(([name]) => name === "exchange"), false);
  assert.equal(calls.some(([name]) => name === "write"), false);
});

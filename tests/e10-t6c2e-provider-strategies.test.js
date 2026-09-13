"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {createEmbeddedProviderStrategies, SPECS} = require("../src/shopify/embedded-provider-strategies");

const env = Object.fromEntries(Object.values(SPECS).flatMap(spec => [[spec.client, `${spec.client}-value`], [spec.secret, `${spec.secret}-value`]]));
const exchanges = Object.fromEntries(Object.keys(SPECS).map(provider => [provider, async input => ({accessToken: `${provider}:${input.code}`})]));

test("provider strategies fail closed when any credential or exchange is missing", () => {
  assert.throws(() => createEmbeddedProviderStrategies({env: {}, appUrl: "https://dev.example", exchangeCodeByProvider: exchanges}), /EMBEDDED_META_OAUTH_CONFIG_INCOMPLETE/);
  const incomplete = {...exchanges}; delete incomplete.pinterest;
  assert.throws(() => createEmbeddedProviderStrategies({env, appUrl: "https://dev.example", exchangeCodeByProvider: incomplete}), /EMBEDDED_PINTEREST_OAUTH_CONFIG_INCOMPLETE/);
});

test("all callbacks use canonical embedded routes instead of legacy dashboard callbacks", () => {
  const strategies = createEmbeddedProviderStrategies({env, appUrl: "https://dev.example", exchangeCodeByProvider: exchanges});
  for (const [provider, strategy] of Object.entries(strategies)) assert.equal(strategy.redirectUri, `https://dev.example/api/shopify/providers/${provider}/oauth/callback`);
});

test("authorization URLs bind client, state, callback and minimum provider scope", async () => {
  const strategies = createEmbeddedProviderStrategies({env, appUrl: "https://dev.example", exchangeCodeByProvider: exchanges});
  for (const [provider, strategy] of Object.entries(strategies)) {
    const pkce = strategy.createPkce?.();
    const url = new URL(await strategy.buildAuthorizationUrl({state: "opaque-state", pkceChallenge: pkce?.challenge}));
    assert.equal(url.searchParams.get("state"), "opaque-state");
    assert.equal(url.searchParams.get("redirect_uri"), strategy.redirectUri);
    assert.equal(url.searchParams.get(provider === "tiktok" ? "app_id" : "client_id"), env[SPECS[provider].client]);
  }
  assert.equal(new URL(await strategies.google_ads.buildAuthorizationUrl({state: "s"})).searchParams.get("access_type"), "offline");
  const pkce = strategies.klaviyo.createPkce();
  const klaviyo = new URL(await strategies.klaviyo.buildAuthorizationUrl({state: "s", pkceChallenge: pkce.challenge}));
  assert.equal(klaviyo.searchParams.get("code_challenge_method"), "S256");
  await assert.rejects(() => strategies.klaviyo.buildAuthorizationUrl({state: "s"}), /KLAVIYO_PKCE_REQUIRED/);
});

test("secrets are passed only to server-side exchange and never authorization URLs", async () => {
  const strategies = createEmbeddedProviderStrategies({env, appUrl: "https://dev.example", exchangeCodeByProvider: exchanges});
  for (const [provider, strategy] of Object.entries(strategies)) {
    const url = await strategy.buildAuthorizationUrl({state: "state", pkceChallenge: strategy.createPkce?.().challenge});
    assert.equal(url.includes(env[SPECS[provider].secret]), false);
    assert.deepEqual(await strategy.exchangeCode({code: "code", redirectUri: strategy.redirectUri}), {accessToken: `${provider}:code`});
  }
});

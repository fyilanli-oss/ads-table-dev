"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {loadShopifyConfig, shopifyConfigStatus, ShopifyConfigError} = require("../src/config/shopify-config");
const {createShopifyExchangeClient, createShopifyAdminClient} = require("../src/shopify/admin-api-client");

const env = {SHOPIFY_API_KEY: "key", SHOPIFY_API_SECRET: "secret", SHOPIFY_APP_URL: "https://dev.adstable.app", SHOPIFY_DEV_STORE_DOMAIN: "ads-table-dev.myshopify.com"};

test("Shopify config is disabled when absent and fails closed when partial", () => {
  assert.deepEqual(loadShopifyConfig({}), {enabled: false});
  assert.deepEqual(shopifyConfigStatus({}), {configured: false, visible_count: 0, required_count: 4});
  assert.throws(() => loadShopifyConfig({SHOPIFY_API_KEY: "key"}), (error) => error instanceof ShopifyConfigError && error.code === "SHOPIFY_CONFIG_INCOMPLETE" && error.missing.length === 3);
});

test("Shopify config validates all four values without exposing a redacted status secret", () => {
  const config = loadShopifyConfig(env);
  assert.equal(config.enabled, true);
  assert.equal(config.appUrl, "https://dev.adstable.app");
  assert.equal(config.developmentStoreDomain, "ads-table-dev.myshopify.com");
  assert.deepEqual(shopifyConfigStatus(env), {configured: true, visible_count: 4, required_count: 4});
  assert.doesNotMatch(JSON.stringify(shopifyConfigStatus(env)), /key|secret|adstable/i);
});

test("token exchange calls the canonical shop endpoint and normalizes expirations", async () => {
  let request;
  const client = createShopifyExchangeClient({clientId: "key", clientSecret: "secret", now: () => 1000000, fetchImpl: async (...args) => {
    request = args;
    return {ok: true, json: async () => ({access_token: "access", scope: "", expires_in: 3600, refresh_token: "refresh", refresh_token_expires_in: 7200})};
  }});
  const result = await client.exchange({shop_domain: "store.myshopify.com", subject_token: "id-token", subject_token_type: "id-type", requested_token_type: "offline-type"});
  assert.equal(request[0], "https://store.myshopify.com/admin/oauth/access_token");
  assert.match(request[1].body, /grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange/);
  assert.deepEqual(result, {access_token: "access", expires_at: 4600, refresh_token: "refresh", refresh_token_expires_at: 8200, scope: ""});
});

test("Admin client accepts identity only from the requested canonical shop", async () => {
  const client = createShopifyAdminClient({fetchImpl: async (url, options) => {
    assert.equal(url, "https://store.myshopify.com/admin/api/2026-07/graphql.json");
    assert.equal(options.headers["X-Shopify-Access-Token"], "access");
    return {ok: true, json: async () => ({data: {shop: {id: "gid://shopify/Shop/1", myshopifyDomain: "store.myshopify.com"}}})};
  }});
  assert.deepEqual(await client.getShopIdentity({shop_domain: "store.myshopify.com", access_token: "access"}), {authority: "shopify_verified_admin", shop_id: "gid://shopify/Shop/1", shop_domain: "store.myshopify.com"});
});

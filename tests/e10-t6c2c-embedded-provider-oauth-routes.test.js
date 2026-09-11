"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {registerShopifyProviderOAuthRoutes, PROVIDERS} = require("../src/routes/shopify-provider-oauth-routes");
const {enabled} = require("../src/shopify/runtime");

function fixture(overrides = {}) {
  const routes = {};
  const app = {
    post(path, handler) { routes[`POST ${path}`] = handler; },
    get(path, handler) { routes[`GET ${path}`] = handler; },
  };
  const calls = [];
  const adapters = Object.fromEntries(PROVIDERS.map(provider => [provider, overrides[provider] || {
    start: async input => (calls.push(["start", provider, input]), {authorization_url: "https://consent", navigation: "top_level"}),
    callback: async input => (calls.push(["callback", provider, input]), {redirect_to: "/shopify/app/platforms"}),
  }]));
  registerShopifyProviderOAuthRoutes(app, {adapters});
  return {routes, calls};
}

function response() {
  return {statusCode: 0, body: null, location: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; }, redirect(location) { this.location = location; return this; }};
}

test("embedded provider OAuth runtime flag is strict and defaults off", () => {
  assert.equal(enabled(undefined), false);
  assert.equal(enabled("false"), false);
  assert.equal(enabled("true"), true);
  assert.throws(() => enabled("TRUE"), /must be true or false/);
});

test("start accepts authority only through the Shopify bearer session", async () => {
  const {routes, calls} = fixture();
  const res = response();
  await routes["POST /api/shopify/providers/:provider/oauth/start"]({params: {provider: "meta"}, get: () => "Bearer verified.session"}, res, assert.fail);
  assert.deepEqual(calls, [["start", "meta", {sessionToken: "verified.session"}]]);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, {authorization_url: "https://consent", navigation: "top_level"});
});

test("callback forwards only state and code and always returns to canonical Platforms", async () => {
  const {routes, calls} = fixture();
  const res = response();
  await routes["GET /api/shopify/providers/:provider/oauth/callback"]({params: {provider: "google_ads"}, query: {state: "opaque", code: "code", workspace_id: "attacker"}}, res);
  assert.deepEqual(calls, [["callback", "google_ads", {state: "opaque", code: "code"}]]);
  assert.equal(res.location, "/shopify/app/platforms?oauth_connected=google_ads&account_selection_required=1");
});

test("unknown providers and replay failures cannot escape the canonical surface", async () => {
  const {routes} = fixture();
  const missing = response();
  await routes["POST /api/shopify/providers/:provider/oauth/start"]({params: {provider: "unknown"}, get: () => "Bearer token"}, missing, assert.fail);
  assert.deepEqual(missing.body, {code: "SHOPIFY_PROVIDER_NOT_FOUND"});

  const failed = response();
  const replay = fixture({meta: {start: async () => ({}), callback: async () => { throw new Error("replayed"); }}});
  await replay.routes["GET /api/shopify/providers/:provider/oauth/callback"]({params: {provider: "meta"}, query: {}}, failed);
  assert.match(failed.location, /^\/shopify\/app\/platforms\?oauth_/);
});

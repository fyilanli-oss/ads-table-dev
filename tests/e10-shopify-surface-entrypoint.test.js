"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

function response() {
  const headers = new Map();
  return {headers, setHeader: (key, value) => headers.set(key.toLowerCase(), value), end(body) { this.body = body; }};
}

function withShopifyApiKey(value, callback) {
  const previous = process.env.SHOPIFY_API_KEY;
  if (value === undefined) delete process.env.SHOPIFY_API_KEY;
  else process.env.SHOPIFY_API_KEY = value;
  try {
    callback();
  } finally {
    if (previous === undefined) delete process.env.SHOPIFY_API_KEY;
    else process.env.SHOPIFY_API_KEY = previous;
  }
}

test("dedicated Shopify entrypoint renders the native App Home without booting the full server", () => {
  const handler = require("../api/shopify-app");
  const res = response();
  withShopifyApiKey("test-key", () => handler({method: "GET", url: "/shopify/app"}, res));
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers.get("x-adstable-release"), "e10-t6c2k");
  assert.equal(res.headers.get("surrogate-control"), "no-store");
  assert.match(res.body, /<s-page heading="AdsTable">/);
  assert.doesNotMatch(res.body, /<iframe|<style/i);
});

test("dedicated Shopify entrypoint fails explicitly when its public client id is unavailable", () => {
  const handler = require("../api/shopify-app");
  const res = response();
  withShopifyApiKey(undefined, () => handler({method: "GET", url: "/"}, res));
  assert.equal(res.statusCode, 503);
  assert.equal(res.headers.get("content-type"), "text/plain; charset=utf-8");
});

test("dedicated Shopify entrypoint only serves its allowlisted read routes", () => {
  const handler = require("../api/shopify-app");
  const unknown = response();
  const mutation = response();
  const head = response();

  withShopifyApiKey("test-key", () => {
    handler({method: "GET", url: "/shopify/app/not-a-page"}, unknown);
    handler({method: "POST", url: "/shopify/app"}, mutation);
    handler({method: "HEAD", url: "/shopify/app"}, head);
  });

  assert.equal(unknown.statusCode, 404);
  assert.equal(unknown.body, undefined);
  assert.equal(mutation.statusCode, 405);
  assert.equal(mutation.headers.get("allow"), "GET, HEAD");
  assert.equal(mutation.body, undefined);
  assert.equal(head.statusCode, 200);
  assert.equal(head.body, undefined);
});

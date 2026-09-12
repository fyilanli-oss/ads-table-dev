"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

function response() {
  const headers = new Map();
  return {headers, setHeader: (key, value) => headers.set(key.toLowerCase(), value), end(body) { this.body = body; }};
}

test("dedicated Shopify entrypoint renders the native App Home without booting the full server", () => {
  const previous = process.env.SHOPIFY_API_KEY;
  process.env.SHOPIFY_API_KEY = "test-key";
  const handler = require("../api/shopify-app");
  const res = response();
  handler({url: "/shopify/app"}, res);
  if (previous === undefined) delete process.env.SHOPIFY_API_KEY; else process.env.SHOPIFY_API_KEY = previous;
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers.get("x-adstable-release"), "e10-t6c2k");
  assert.match(res.body, /<s-page heading="AdsTable">/);
  assert.doesNotMatch(res.body, /<iframe|<style/i);
});

test("dedicated Shopify entrypoint fails explicitly when its public client id is unavailable", () => {
  const previous = process.env.SHOPIFY_API_KEY;
  delete process.env.SHOPIFY_API_KEY;
  const handler = require("../api/shopify-app");
  const res = response();
  handler({url: "/"}, res);
  if (previous !== undefined) process.env.SHOPIFY_API_KEY = previous;
  assert.equal(res.statusCode, 503);
  assert.equal(res.headers.get("content-type"), "text/plain; charset=utf-8");
});

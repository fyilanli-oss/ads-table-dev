"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {probe, validateSurface} = require("../scripts/e10-production-surface-probe");

const nativeBody = `
  <script src="https://cdn.shopify.com/shopifycloud/app-bridge.js"></script>
  <script src="https://cdn.shopify.com/shopifycloud/polaris-1.js"></script>
  <s-page heading="AdsTable">
  <s-button id="platforms" variant="primary" href="/shopify/app/platforms">Manage data sources</s-button>`;

test("production surface requires the deployed release and Shopify-native markup", () => {
  const headers = new Headers({"x-adstable-release": "e10-t6c2k"});
  assert.equal(validateSurface({status: 200, headers, body: nativeBody}).pass, true);
  assert.equal(validateSurface({status: 200, headers: new Headers(), body: nativeBody}).pass, false);
  assert.equal(validateSurface({status: 200, headers, body: `${nativeBody}<style></style>`}).pass, false);
  assert.equal(validateSurface({status: 200, headers, body: `${nativeBody}<iframe></iframe>`}).pass, false);
});

test("probe contacts only the canonical HTTPS App Home without following redirects", async () => {
  let request;
  const result = await probe({fetchImpl: async (url, options) => {
    request = {url, options};
    return new Response(nativeBody, {status: 200, headers: {"x-adstable-release": "e10-t6c2k"}});
  }});
  assert.equal(result.pass, true);
  assert.equal(request.url.origin, "https://dev.adstable.app");
  assert.equal(request.url.pathname, "/");
  assert.equal(request.url.searchParams.get("surface_probe"), "e10-t6c2k");
  assert.equal(request.options.redirect, "error");
  await assert.rejects(() => probe({url: "https://example.com/"}), /canonical App Home root/);
});

test("canonical production App Home serves the Shopify-native release", {skip: process.env.E10_LIVE_SURFACE_PROBE !== "true"}, async () => {
  const result = await probe();
  assert.equal(result.pass, true, JSON.stringify(result));
});

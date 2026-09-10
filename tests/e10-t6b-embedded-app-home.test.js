"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {renderEmbeddedAppHome, registerEmbeddedAppHome} = require("../src/shopify/embedded-app-home");

test("embedded App Home obtains fresh Shopify ID tokens for bootstrap, idempotency, and session", () => {
  const html = renderEmbeddedAppHome({clientId: "development-client"});
  assert.match(html, /name="shopify-api-key" content="development-client"/);
  assert.match(html, /cdn\.shopify\.com\/shopifycloud\/app-bridge\.js/);
  assert.equal((html.match(/window\.shopify\.idToken\(\)/g) || []).length, 3);
  assert.equal((html.match(/request\("\/api\/shopify\/bootstrap", "POST"/g) || []).length, 2);
  assert.match(html, /request\("\/api\/shopify\/session", "GET"/);
  assert.doesNotMatch(html, /console\.|localStorage|sessionStorage|shop_domain|workspace_id|access_token/);
});

test("embedded App Home escapes the public client id", () => {
  const html = renderEmbeddedAppHome({clientId: '\"><script>alert(1)</script>'});
  assert.doesNotMatch(html, /content=""><script>/);
  assert.match(html, /&quot;&gt;&lt;script&gt;/);
});

test("App Home handler only claims Shopify embedded requests and disables caching", () => {
  const handlers = new Map();
  registerEmbeddedAppHome({get(path, fn) { handlers.set(path, fn); }}, {clientId: "client"});
  assert.deepEqual([...handlers.keys()], ["/", "/shopify/app"]);
  const handler = handlers.get("/");
  let nextCalled = false;
  handler({query: {}}, {}, () => { nextCalled = true; });
  assert.equal(nextCalled, true);

  const response = {
    headers: {},
    set(name, value) { this.headers[name] = value; },
    type(value) { this.contentType = value; return this; },
    send(value) { this.body = value; return this; },
  };
  handler({query: {embedded: "1"}}, response, () => assert.fail("must not fall through"));
  assert.equal(response.headers["Cache-Control"], "no-store");
  assert.equal(response.headers["Content-Security-Policy"], "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
  assert.equal(response.contentType, "html");
  assert.match(response.body, /Development store connected securely/);

  const hostResponse = {...response, headers: {}, set: response.set, type: response.type, send: response.send};
  handler({query: {host: "redacted-shopify-context"}}, hostResponse, () => assert.fail("host context must not fall through"));
  assert.equal(hostResponse.headers["Cache-Control"], "no-store");
});

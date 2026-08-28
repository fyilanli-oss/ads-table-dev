"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createOAuthTransactionBoundary } = require("../src/oauth/transaction-boundary");

test("cleans expired transactions before creating a user/provider-bound transaction", async () => {
  const calls = [];
  const expected = { state: "opaque" };
  const boundary = createOAuthTransactionBoundary({
    transactionStore: {
      cleanupExpired: async () => calls.push("cleanup"),
      create: async (input) => (calls.push(["create", input]), expected),
    },
  });

  const result = await boundary.createTransaction("user-1", "google", "https://app/callback", "verifier");
  assert.equal(result, expected);
  assert.deepEqual(calls, [
    "cleanup",
    ["create", { userId: "user-1", provider: "google", redirectUri: "https://app/callback", pkceVerifier: "verifier" }],
  ]);
});

test("consumes only through the transaction store with normalized state", async () => {
  const calls = [];
  const boundary = createOAuthTransactionBoundary({
    transactionStore: { consume: async (input) => (calls.push(input), { userId: "user-1" }) },
  });
  assert.deepEqual(await boundary.consumeTransaction(123, "meta", "https://app/callback"), { userId: "user-1" });
  assert.deepEqual(calls, [{ state: "123", provider: "meta", redirectUri: "https://app/callback" }]);
});

test("fails closed for create and returns null for non-consumable state", async () => {
  const boundary = createOAuthTransactionBoundary();
  await assert.rejects(() => boundary.createTransaction("user", "meta", "callback"), /not configured/);
  assert.equal(await boundary.consumeTransaction("state", "meta", "callback"), null);

  let consumed = false;
  const configured = createOAuthTransactionBoundary({ transactionStore: { consume: async () => { consumed = true; } } });
  assert.equal(await configured.consumeTransaction("", "meta", "callback"), null);
  assert.equal(consumed, false);
});

test("keeps JSON handshake and browser redirect response modes explicit", () => {
  const boundary = createOAuthTransactionBoundary();
  const json = { body: null, json(body) { this.body = body; return this; }, redirect() { assert.fail("must not redirect"); } };
  boundary.sendAuthorizationResponse({ query: { response_mode: "json" } }, json, "https://provider/auth");
  assert.deepEqual(json.body, { authorization_url: "https://provider/auth" });

  const browser = { location: null, json() { assert.fail("must not emit JSON"); }, redirect(location) { this.location = location; return this; } };
  boundary.sendAuthorizationResponse({ query: {} }, browser, "https://provider/auth");
  assert.equal(browser.location, "https://provider/auth");
  assert.equal(Object.isFrozen(boundary), true);
});

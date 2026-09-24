"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {CONFIRMATION, createKlaviyoDisconnect} = require("../src/shopify/klaviyo-disconnect");

const authority = {authority: "server_resolved_workspace", workspace_id: "11111111-1111-4111-8111-111111111111", source: "shopify_verified_session"};

test("Klaviyo disconnect requires action-time confirmation", async () => {
  let reads = 0;
  const service = createKlaviyoDisconnect({clientId: "client", clientSecret: "secret", store: {
    readKlaviyo: async () => { reads++; }, disconnectKlaviyo: async () => {},
  }});
  await assert.rejects(service.execute(authority, "yes"), error => error.code === "KLAVIYO_DISCONNECT_CONFIRMATION_REQUIRED");
  assert.equal(reads, 0);
});

test("provider revoke succeeds before canonical credentials are cleared", async () => {
  const calls = [];
  const service = createKlaviyoDisconnect({clientId: "client", clientSecret: "secret", fetchImpl: async (url, options) => {
    calls.push(["revoke", url, options]);
    return {ok: true, status: 200};
  }, store: {
    readKlaviyo: async () => ({status: "connected", connection_version: 7, refreshToken: "refresh-secret"}),
    disconnectKlaviyo: async input => calls.push(["disconnect", input]),
  }});
  assert.deepEqual(await service.execute(authority, CONFIRMATION), {status: "not_connected", historical_data_preserved: true});
  assert.equal(calls[0][1], "https://a.klaviyo.com/oauth/revoke");
  assert.match(calls[0][2].body, /token_type_hint=refresh_token/);
  assert.deepEqual(calls[1][1], {authority, version: 7});
});

test("failed provider revoke keeps the canonical connection active", async () => {
  let writes = 0;
  const service = createKlaviyoDisconnect({clientId: "client", clientSecret: "secret", fetchImpl: async () => ({ok: false, status: 503}), store: {
    readKlaviyo: async () => ({status: "connected", connection_version: 2, refreshToken: "refresh"}),
    disconnectKlaviyo: async () => { writes++; },
  }});
  await assert.rejects(service.execute(authority, CONFIRMATION), error => error.code === "KLAVIYO_REVOKE_FAILED");
  assert.equal(writes, 0);
});

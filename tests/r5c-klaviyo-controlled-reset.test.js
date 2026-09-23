"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {CONFIRMATION, createKlaviyoControlledReset} = require("../src/shopify/klaviyo-controlled-reset");
const {createWorkspaceProviderConnectionStore} = require("../src/shopify/workspace-provider-connection-store");

const authority = {authority: "shopify_verified_session", workspace_id: "workspace-a", shop_id: "shop-a"};
const response = status => ({ok: status >= 200 && status < 300, status});

test("controlled reset requires exact action-time confirmation before reading or contacting provider", async () => {
  let reads = 0;
  let requests = 0;
  const reset = createKlaviyoControlledReset({
    clientId: "client",
    clientSecret: "secret",
    store: {readKlaviyoForReset: async () => { reads++; }, markKlaviyoRevoked: async () => {}},
    fetchImpl: async () => { requests++; return response(200); },
  });
  await assert.rejects(reset.execute(authority, "yes"), error => error.code === "KLAVIYO_RESET_CONFIRMATION_REQUIRED");
  assert.equal(reads, 0);
  assert.equal(requests, 0);
});

test("provider success precedes local revoked state and preserves historical fields by omission", async () => {
  const calls = [];
  const reset = createKlaviyoControlledReset({
    clientId: "client",
    clientSecret: "secret",
    store: {
      readKlaviyoForReset: async received => {
        calls.push(["read", received]);
        return {status: "connected", updated_at: "version-1", refreshToken: "refresh-secret"};
      },
      markKlaviyoRevoked: async input => calls.push(["mark", input]),
    },
    fetchImpl: async (url, options) => {
      calls.push(["revoke", url, options]);
      return response(200);
    },
  });
  const result = await reset.execute(authority, CONFIRMATION);
  assert.deepEqual(result, {status: "revoked", canonical_connection_created: false, historical_data_preserved: true});
  assert.deepEqual(calls.map(call => call[0]), ["read", "revoke", "mark"]);
  assert.equal(calls[1][1], "https://a.klaviyo.com/oauth/revoke");
  assert.equal(calls[1][2].method, "POST");
  assert.match(calls[1][2].body, /token_type_hint=refresh_token/);
  assert.match(calls[1][2].body, /token=refresh-secret/);
  assert.deepEqual(calls[2][1], {authority, version: "version-1"});
  assert.equal(JSON.stringify(calls[2][1]).includes("refresh-secret"), false);
});

test("provider failure leaves local state unchanged and never starts OAuth", async () => {
  let writes = 0;
  const reset = createKlaviyoControlledReset({
    clientId: "client",
    clientSecret: "secret",
    store: {
      readKlaviyoForReset: async () => ({status: "connected", updated_at: "v", refreshToken: "refresh"}),
      markKlaviyoRevoked: async () => { writes++; },
    },
    fetchImpl: async url => {
      assert.equal(url, "https://a.klaviyo.com/oauth/revoke");
      return response(503);
    },
  });
  await assert.rejects(reset.execute(authority, CONFIRMATION), error => error.code === "KLAVIYO_REVOKE_FAILED");
  assert.equal(writes, 0);
});

test("already revoked state is idempotent and performs no provider request", async () => {
  let requests = 0;
  const reset = createKlaviyoControlledReset({
    clientId: "client",
    clientSecret: "secret",
    store: {readKlaviyoForReset: async () => ({status: "revoked"}), markKlaviyoRevoked: async () => {}},
    fetchImpl: async () => { requests++; return response(200); },
  });
  assert.deepEqual(await reset.execute(authority, CONFIRMATION), {
    status: "already_revoked", canonical_connection_created: false, historical_data_preserved: true,
  });
  assert.equal(requests, 0);
});

test("local finalization updates only status and version under verified workspace authority", async () => {
  const filters = [];
  let mutation;
  const query = {
    eq(key, value) { filters.push([key, value]); return this; },
    neq(key, value) { filters.push([key, value]); return this; },
    select() { return this; },
    maybeSingle: async () => ({data: {status: "revoked"}, error: null}),
  };
  const store = createWorkspaceProviderConnectionStore({
    client: {from: table => {
      assert.equal(table, "shopify_workspace_provider_connections");
      return {update: row => { mutation = row; return query; }};
    }},
    vault: {encrypt: value => value, decrypt: value => value},
    now: () => new Date("2026-09-23T15:00:00.000Z"),
  });
  assert.deepEqual(await store.markKlaviyoRevoked({authority, version: "version-1"}), {status: "revoked"});
  assert.deepEqual(mutation, {status: "revoked", updated_at: "2026-09-23T15:00:00.000Z"});
  assert.deepEqual(filters, [
    ["workspace_id", "workspace-a"], ["shop_id", "shop-a"], ["provider", "klaviyo"],
    ["updated_at", "version-1"], ["status", "revoked"],
  ]);
});

test("R5-C contract records the failed verification branch and keeps reset non-destructive", () => {
  const root = path.join(__dirname, "..");
  const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/r5-klaviyo-consolidation-v2.json"), "utf8"));
  const doc = fs.readFileSync(path.join(root, "docs/R5C_KLAVIYO_CONTROLLED_CLEAN_RESET.md"), "utf8");
  assert.equal(contract.observed_live_outcome.result, "reauthorization_required");
  assert.deepEqual(contract.gates.map(gate => gate.id), ["R5-A", "R5-B", "R5-C"]);
  assert.ok(contract.forbidden.includes("clear_token_envelopes_during_reset"));
  assert.ok(contract.forbidden.includes("start_oauth_in_the_revoke_request"));
  assert.match(doc, /henüz route'a veya kullanıcı düğmesine bağlanmamıştır/i);
  assert.match(doc, /canlı provider çağrısı veya Supabase mutation/i);
});

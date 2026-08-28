"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createSharedClients } = require("../src/clients/shared-clients");

test("creates one shared dependency graph with server-only Supabase options", () => {
  const calls = [];
  const supabaseAdmin = { kind: "supabase" };
  const oauthTransactionStore = { kind: "oauth" };
  const providerTokenVault = { kind: "vault" };
  const providerTokenStore = { kind: "tokens" };
  const clients = createSharedClients({
    env: { SUPABASE_URL: "https://project.invalid", SUPABASE_SERVICE_ROLE_KEY: "secret" },
    providerTokenEncryptionEnabled: true,
    providerTokenLegacyReadsEnabled: false,
    createSupabaseClient: (...args) => (calls.push(["supabase", ...args]), supabaseAdmin),
    createTransactionStore: (options) => (calls.push(["oauth", options]), oauthTransactionStore),
    createTokenVault: (env) => (calls.push(["vault", env]), providerTokenVault),
    createTokenStore: (options) => (calls.push(["tokens", options]), providerTokenStore),
  });

  assert.deepEqual(clients, {
    supabaseAdmin,
    oauthTransactionStore,
    providerTokenVault,
    providerTokenStore,
  });
  assert.equal(Object.isFrozen(clients), true);
  assert.deepEqual(calls[0], [
    "supabase",
    "https://project.invalid",
    "secret",
    { auth: { persistSession: false } },
  ]);
  assert.deepEqual(calls[1], ["oauth", { client: supabaseAdmin }]);
  assert.equal(calls[2][0], "vault");
  assert.deepEqual(calls[3], ["tokens", {
    client: supabaseAdmin,
    vault: providerTokenVault,
    legacyReadsEnabled: false,
  }]);
});

test("keeps optional clients absent without invoking their factories", () => {
  const forbidden = () => assert.fail("optional factory must not run");
  const clients = createSharedClients({
    env: {},
    createSupabaseClient: forbidden,
    createTransactionStore: forbidden,
    createTokenVault: forbidden,
    createTokenStore: forbidden,
  });

  assert.deepEqual(clients, {
    supabaseAdmin: null,
    oauthTransactionStore: null,
    providerTokenVault: null,
    providerTokenStore: null,
  });
});

test("creates the encryption vault without fabricating database clients", () => {
  const vault = { kind: "vault" };
  const env = { PROVIDER_TOKEN_KEYRING: "redacted" };
  const clients = createSharedClients({
    env,
    providerTokenEncryptionEnabled: true,
    createSupabaseClient: () => assert.fail("Supabase factory must not run"),
    createTransactionStore: () => assert.fail("OAuth factory must not run"),
    createTokenVault: (received) => (assert.equal(received, env), vault),
    createTokenStore: () => assert.fail("token store factory must not run"),
  });

  assert.equal(clients.providerTokenVault, vault);
  assert.equal(clients.providerTokenStore, null);
});

test("fails closed for invalid composition dependencies", () => {
  assert.throws(() => createSharedClients({ env: null }), /env must be an object/);
  for (const dependency of [
    "createSupabaseClient",
    "createTransactionStore",
    "createTokenVault",
    "createTokenStore",
  ]) {
    assert.throws(
      () => createSharedClients({ [dependency]: null }),
      new RegExp(`${dependency} must be a function`),
    );
  }
});

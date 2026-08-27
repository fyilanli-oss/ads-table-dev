"use strict";

const { createClient } = require("@supabase/supabase-js");
const { createOAuthTransactionStore } = require("../../security/oauth-transaction-store");
const { createProviderTokenVaultFromEnv } = require("../../security/provider-token-vault");
const { createProviderTokenStore } = require("../../security/provider-token-store");

function requireFactory(name, value) {
  if (typeof value !== "function") throw new TypeError(`${name} must be a function`);
}

function createSharedClients({
  env = process.env,
  providerTokenEncryptionEnabled = false,
  providerTokenLegacyReadsEnabled = true,
  createSupabaseClient = createClient,
  createTransactionStore = createOAuthTransactionStore,
  createTokenVault = createProviderTokenVaultFromEnv,
  createTokenStore = createProviderTokenStore,
} = {}) {
  if (!env || typeof env !== "object" || Array.isArray(env)) {
    throw new TypeError("env must be an object");
  }
  requireFactory("createSupabaseClient", createSupabaseClient);
  requireFactory("createTransactionStore", createTransactionStore);
  requireFactory("createTokenVault", createTokenVault);
  requireFactory("createTokenStore", createTokenStore);

  const hasSupabaseCredentials = Boolean(
    env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY,
  );
  const supabaseAdmin = hasSupabaseCredentials
    ? createSupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
        auth: { persistSession: false },
      })
    : null;
  const oauthTransactionStore = supabaseAdmin
    ? createTransactionStore({ client: supabaseAdmin })
    : null;
  const providerTokenVault = providerTokenEncryptionEnabled
    ? createTokenVault(env)
    : null;
  const providerTokenStore = providerTokenEncryptionEnabled && supabaseAdmin
    ? createTokenStore({
        client: supabaseAdmin,
        vault: providerTokenVault,
        legacyReadsEnabled: providerTokenLegacyReadsEnabled,
      })
    : null;

  return Object.freeze({
    supabaseAdmin,
    oauthTransactionStore,
    providerTokenVault,
    providerTokenStore,
  });
}

module.exports = { createSharedClients };

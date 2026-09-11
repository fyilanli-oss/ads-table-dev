"use strict";

const EMBEDDED_SURFACE = "shopify_embedded";
const PROVIDERS = new Set(["meta", "google_ads", "klaviyo", "tiktok", "pinterest"]);

function required(value, field) {
  if (typeof value !== "string" || !value.trim() || value !== value.trim()) throw new TypeError(`${field} is required`);
  return value;
}

function assertEmbeddedTransaction(transaction) {
  if (!transaction || transaction.surface !== EMBEDDED_SURFACE || transaction.user_id !== null) {
    throw new Error("EMBEDDED_OAUTH_TRANSACTION_REQUIRED");
  }
  for (const field of ["shop_id", "workspace_id", "shopify_user_id"]) required(transaction[field], `transaction.${field}`);
  if (transaction.return_target !== "/shopify/app/platforms") throw new Error("INVALID_EMBEDDED_RETURN_TARGET");
  if (!PROVIDERS.has(transaction.provider)) throw new Error("UNSUPPORTED_EMBEDDED_PROVIDER");
  return transaction;
}

function createWorkspaceProviderConnectionStore({client, vault, now = () => new Date()} = {}) {
  if (!client || typeof client.from !== "function") throw new TypeError("Supabase service-role client is required");
  if (!vault || typeof vault.encrypt !== "function" || typeof vault.decrypt !== "function") throw new TypeError("provider token vault is required");
  const context = (workspaceId, provider, tokenType) => ({userId: `workspace:${workspaceId}`, platform: provider, tokenType});

  async function writeFromOAuthTransaction({transaction, accessToken, refreshToken = null}) {
    const authority = assertEmbeddedTransaction(transaction);
    required(accessToken, "accessToken");
    const row = {
      workspace_id: authority.workspace_id,
      shop_id: authority.shop_id,
      provider: authority.provider,
      status: "pending_account_selection",
      access_token_envelope: vault.encrypt(accessToken, context(authority.workspace_id, authority.provider, "access")),
      refresh_token_envelope: vault.encrypt(refreshToken, context(authority.workspace_id, authority.provider, "refresh")),
      updated_at: now().toISOString(),
    };
    const {error} = await client.from("shopify_workspace_provider_connections").upsert(row, {onConflict: "workspace_id,provider"});
    if (error) throw new Error(error.message);
    return Object.freeze({workspace_id: authority.workspace_id, provider: authority.provider, status: row.status});
  }

  async function resolve({workspaceId, provider}) {
    required(workspaceId, "workspaceId");
    if (!PROVIDERS.has(provider)) throw new Error("UNSUPPORTED_EMBEDDED_PROVIDER");
    const {data, error} = await client.from("shopify_workspace_provider_connections")
      .select("status,access_token_envelope,refresh_token_envelope")
      .eq("workspace_id", workspaceId).eq("provider", provider).maybeSingle();
    if (error) throw new Error(error.message);
    if (!data) return null;
    return Object.freeze({
      status: data.status,
      accessToken: vault.decrypt(data.access_token_envelope, context(workspaceId, provider, "access")),
      refreshToken: vault.decrypt(data.refresh_token_envelope, context(workspaceId, provider, "refresh")),
    });
  }

  return Object.freeze({writeFromOAuthTransaction, resolve});
}

module.exports = Object.freeze({createWorkspaceProviderConnectionStore, assertEmbeddedTransaction});

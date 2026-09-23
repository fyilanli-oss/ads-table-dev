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
      active_account_id: null,
      ...(authority.provider === "klaviyo" ? {email_monthly_plan_cost: null, account_currency: null} : {}),
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

  function klaviyoQuery(authority, query) {
    if (authority?.authority !== "shopify_verified_session") throw new Error("UNVERIFIED_SHOPIFY_SESSION");
    return query.eq("workspace_id", required(authority.workspace_id, "workspace_id"))
      .eq("shop_id", required(authority.shop_id, "shop_id")).eq("provider", "klaviyo");
  }

  async function readKlaviyo(authority) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections")
      .select("status,active_account_id,email_monthly_plan_cost,account_currency,updated_at,access_token_envelope,refresh_token_envelope")).maybeSingle();
    if (error) throw new Error("CONNECTION_READ_FAILED");
    if (!data) return null;
    return {...data,
      accessToken: data.status === "revoked" ? null : vault.decrypt(data.access_token_envelope, context(authority.workspace_id, "klaviyo", "access")),
      refreshToken: data.status === "revoked" ? null : vault.decrypt(data.refresh_token_envelope, context(authority.workspace_id, "klaviyo", "refresh")),
    };
  }

  async function readKlaviyoStatus(authority) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections")
      .select("status,email_monthly_plan_cost,account_currency")).maybeSingle();
    if (error) throw new Error("CONNECTION_READ_FAILED");
    if (!data || data.status === "revoked") return null;
    return {
      status: data.status,
      email_monthly_plan_cost: data.email_monthly_plan_cost,
      account_currency: data.account_currency,
    };
  }

  async function readKlaviyoForReset(authority) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections")
      .select("status,updated_at,refresh_token_envelope")).maybeSingle();
    if (error) throw new Error("CONNECTION_READ_FAILED");
    if (!data) return null;
    return {
      status: data.status,
      updated_at: data.updated_at,
      refreshToken: data.status === "revoked"
        ? null
        : vault.decrypt(data.refresh_token_envelope, context(authority.workspace_id, "klaviyo", "refresh")),
    };
  }

  async function refreshKlaviyo({authority, version, accessToken, refreshToken}) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections").update({
      access_token_envelope: vault.encrypt(accessToken, context(authority.workspace_id, "klaviyo", "access")),
      refresh_token_envelope: vault.encrypt(refreshToken, context(authority.workspace_id, "klaviyo", "refresh")),
      updated_at: now().toISOString(),
    })).eq("updated_at", required(version, "connection version")).neq("status", "revoked").select("updated_at").maybeSingle();
    if (error) throw new Error("CONNECTION_WRITE_FAILED");
    if (!data) throw Object.assign(new Error("CONNECTION_CHANGED"), {code: "CONNECTION_CHANGED", status: 409});
  }

  async function completeKlaviyo({authority, version, account, cost}) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections").update({
      status: "connected", active_account_id: account.id, account_currency: account.currency,
      email_monthly_plan_cost: cost, updated_at: now().toISOString(),
    })).eq("updated_at", required(version, "connection version")).neq("status", "revoked").select("active_account_id").maybeSingle();
    if (error) throw new Error("CONNECTION_WRITE_FAILED");
    if (!data) throw Object.assign(new Error("CONNECTION_CHANGED"), {code: "CONNECTION_CHANGED", status: 409});
  }

  async function markKlaviyoRevoked({authority, version}) {
    const {data, error} = await klaviyoQuery(authority, client.from("shopify_workspace_provider_connections").update({
      status: "revoked",
      updated_at: now().toISOString(),
    })).eq("updated_at", required(version, "connection version")).neq("status", "revoked")
      .select("status").maybeSingle();
    if (error) throw new Error("CONNECTION_WRITE_FAILED");
    if (!data) throw Object.assign(new Error("CONNECTION_CHANGED"), {code: "CONNECTION_CHANGED", status: 409});
    return Object.freeze({status: "revoked"});
  }

  return Object.freeze({
    writeFromOAuthTransaction,
    resolve,
    readKlaviyo,
    readKlaviyoStatus,
    readKlaviyoForReset,
    completeKlaviyo,
    refreshKlaviyo,
    markKlaviyoRevoked,
  });
}

module.exports = Object.freeze({createWorkspaceProviderConnectionStore, assertEmbeddedTransaction});

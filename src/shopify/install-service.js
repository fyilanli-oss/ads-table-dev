"use strict";

const {normalizeShopDomain} = require("./tenant-model");

const PLATFORM = "shopify";

function requireDependency(value, method, name) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${name}.${method} is required`);
  return value;
}

function required(value, field) {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${field} is required`);
  return value;
}

function createShopifyInstallService({client, vault}) {
  requireDependency(client, "rpc", "client");
  requireDependency(vault, "encrypt", "vault");

  return Object.freeze({
    async complete({shop, shopify_user_id, token_material}) {
      if (!shop || shop.authority !== "shopify_verified") throw new Error("UNVERIFIED_SHOPIFY_INSTALL");
      const shopId = required(shop.shop_id, "shop_id");
      const shopDomain = normalizeShopDomain(shop.shop_domain);
      const accessToken = required(token_material?.access_token, "access_token");
      const context = (tokenType) => ({userId: shopId, platform: PLATFORM, tokenType});
      const accessEnvelope = vault.encrypt(accessToken, context("access"));
      const refreshEnvelope = token_material.refresh_token
        ? vault.encrypt(token_material.refresh_token, context("refresh"))
        : null;
      const {data, error} = await client.rpc("complete_shopify_managed_install", {
        p_shop_id: shopId,
        p_shop_domain: shopDomain,
        p_shopify_user_id: required(shopify_user_id, "shopify_user_id"),
        p_access_token_envelope: accessEnvelope,
        p_refresh_token_envelope: refreshEnvelope,
        p_access_token_expires_at: new Date(token_material.expires_at * 1000).toISOString(),
        p_refresh_token_expires_at: token_material.refresh_token_expires_at
          ? new Date(token_material.refresh_token_expires_at * 1000).toISOString()
          : null,
        p_granted_scopes: token_material.scope
          ? token_material.scope.split(",").map((scope) => scope.trim()).filter(Boolean)
          : [],
      });
      if (error) throw new Error("SHOPIFY_INSTALL_PERSISTENCE_FAILED");
      const row = Array.isArray(data) ? data[0] : data;
      if (!row || row.shop_id !== shopId || row.status !== "active" || !row.workspace_id) {
        throw new Error("SHOPIFY_INSTALL_PERSISTENCE_FAILED");
      }
      return Object.freeze({
        status: "active",
        shop_id: row.shop_id,
        workspace_id: row.workspace_id,
        token_encrypted: true,
      });
    },
  });
}

function createShopifyTenantResolver({client}) {
  requireDependency(client, "from", "client");
  return async ({shop_domain}) => {
    const domain = normalizeShopDomain(shop_domain);
    const {data, error} = await client
      .from("shopify_installations")
      .select("shop_id,shop_domain,workspace_id,status")
      .eq("shop_domain", domain)
      .maybeSingle();
    if (error) throw new Error("SHOPIFY_TENANT_LOOKUP_FAILED");
    return data || null;
  };
}

module.exports = Object.freeze({PLATFORM, createShopifyInstallService, createShopifyTenantResolver});

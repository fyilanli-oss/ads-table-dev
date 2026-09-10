"use strict";

const {normalizeShopDomain} = require("./tenant-model");

const API_VERSION = "2026-07";
const TOKEN_PATH = "/admin/oauth/access_token";
const SHOP_IDENTITY_QUERY = "query AdsTableShopIdentity { shop { id myshopifyDomain } }";

function requireFetch(fetchImpl) {
  if (typeof fetchImpl !== "function") throw new TypeError("fetch must be a function");
  return fetchImpl;
}

async function responseJson(response, code) {
  const data = await response.json().catch(() => null);
  if (!response.ok || !data || typeof data !== "object" || Array.isArray(data)) throw new Error(code);
  return data;
}

function createShopifyExchangeClient({clientId, clientSecret, fetchImpl = globalThis.fetch, now = () => Date.now()}) {
  requireFetch(fetchImpl);
  if (!clientId || !clientSecret) throw new TypeError("Shopify client credentials are required");
  return Object.freeze({
    async exchange({shop_domain, subject_token, subject_token_type, requested_token_type}) {
      const shop = normalizeShopDomain(shop_domain);
      const response = await fetchImpl(`https://${shop}${TOKEN_PATH}`, {
        method: "POST",
        headers: {"Content-Type": "application/x-www-form-urlencoded", Accept: "application/json"},
        body: new URLSearchParams({
          client_id: clientId,
          client_secret: clientSecret,
          grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
          subject_token,
          subject_token_type,
          requested_token_type,
          expiring: "1",
        }).toString(),
      });
      const data = await responseJson(response, "SHOPIFY_TOKEN_EXCHANGE_FAILED");
      const obtainedAt = Math.floor(now() / 1000);
      const expiresIn = Number(data.expires_in);
      const refreshExpiresIn = data.refresh_token_expires_in === undefined ? null : Number(data.refresh_token_expires_in);
      if (!data.access_token || !Number.isSafeInteger(expiresIn) || expiresIn <= 0 || typeof data.scope !== "string") {
        throw new Error("SHOPIFY_TOKEN_EXCHANGE_FAILED");
      }
      return {
        access_token: data.access_token,
        expires_at: obtainedAt + expiresIn,
        refresh_token: data.refresh_token,
        refresh_token_expires_at: refreshExpiresIn === null ? undefined : obtainedAt + refreshExpiresIn,
        scope: data.scope,
      };
    },
  });
}

function createShopifyAdminClient({fetchImpl = globalThis.fetch, apiVersion = API_VERSION} = {}) {
  requireFetch(fetchImpl);
  if (!/^20\d\d-(01|04|07|10)$/.test(apiVersion)) throw new TypeError("Shopify Admin API version is invalid");
  return Object.freeze({
    async getShopIdentity({shop_domain, access_token}) {
      const shop = normalizeShopDomain(shop_domain);
      if (!access_token) throw new TypeError("access_token is required");
      const response = await fetchImpl(`https://${shop}/admin/api/${apiVersion}/graphql.json`, {
        method: "POST",
        headers: {"Content-Type": "application/json", Accept: "application/json", "X-Shopify-Access-Token": access_token},
        body: JSON.stringify({query: SHOP_IDENTITY_QUERY}),
      });
      const payload = await responseJson(response, "SHOPIFY_ADMIN_IDENTITY_FAILED");
      if (payload.errors || !payload.data?.shop?.id || normalizeShopDomain(payload.data.shop.myshopifyDomain) !== shop) {
        throw new Error("SHOPIFY_ADMIN_IDENTITY_FAILED");
      }
      return Object.freeze({authority: "shopify_verified_admin", shop_id: payload.data.shop.id, shop_domain: shop});
    },
  });
}

module.exports = Object.freeze({API_VERSION, TOKEN_PATH, SHOP_IDENTITY_QUERY, createShopifyExchangeClient, createShopifyAdminClient});

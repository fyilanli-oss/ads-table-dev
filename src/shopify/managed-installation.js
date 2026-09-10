"use strict";

const {verifySessionToken} = require("./embedded-auth");

const CONTRACT_VERSION = "e10-t6b-managed-install-v1";
const ID_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:id_token";
const OFFLINE_ACCESS_TOKEN_TYPE = "urn:shopify:params:oauth:token-type:offline-access-token";

function dependency(value, method, name) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${name}.${method} is required`);
  return value;
}

function required(value, field) {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${field} is required`);
  return value;
}

function exactExchangeResult(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  const allowed = ["access_token", "expires_at", "refresh_token", "refresh_token_expires_at", "scope"];
  if (Object.keys(value).some((key) => !allowed.includes(key))) throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  if (required(value.access_token, "access_token").length < 16) throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  if (!Number.isSafeInteger(value.expires_at) || value.expires_at <= 0) throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  if (typeof value.scope !== "string") throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  const hasRefreshToken = value.refresh_token !== undefined;
  const hasRefreshExpiry = value.refresh_token_expires_at !== undefined;
  if (hasRefreshToken !== hasRefreshExpiry) throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  if (hasRefreshToken && (typeof value.refresh_token !== "string" || value.refresh_token.length < 16 || !Number.isSafeInteger(value.refresh_token_expires_at) || value.refresh_token_expires_at <= value.expires_at)) {
    throw new Error("MANAGED_TOKEN_EXCHANGE_FAILED");
  }
  return value;
}

async function bootstrapManagedInstallation({
  session_token,
  auth_config,
  exchange_client,
  admin_client,
  install_service,
}) {
  dependency(exchange_client, "exchange", "exchange_client");
  dependency(admin_client, "getShopIdentity", "admin_client");
  dependency(install_service, "complete", "install_service");

  const verified = verifySessionToken(session_token, auth_config);
  const token = exactExchangeResult(await exchange_client.exchange({
    shop_domain: verified.shop_domain,
    subject_token: session_token,
    subject_token_type: ID_TOKEN_TYPE,
    requested_token_type: OFFLINE_ACCESS_TOKEN_TYPE,
  }));
  const identity = await admin_client.getShopIdentity({
    shop_domain: verified.shop_domain,
    access_token: token.access_token,
  });
  if (!identity || identity.authority !== "shopify_verified_admin" || identity.shop_domain !== verified.shop_domain) {
    throw new Error("SHOP_IDENTITY_VERIFICATION_FAILED");
  }
  const installed = await install_service.complete({
    shop: {
      authority: "shopify_verified",
      shop_id: required(identity.shop_id, "shop_id"),
      shop_domain: verified.shop_domain,
    },
    shopify_user_id: verified.shopify_user_id,
    token_material: {
      access_token: token.access_token,
      expires_at: token.expires_at,
      refresh_token: token.refresh_token,
      refresh_token_expires_at: token.refresh_token_expires_at,
      scope: token.scope,
      token_type: "offline",
    },
  });
  if (!installed || installed.status !== "active" || installed.shop_id !== identity.shop_id || installed.token_encrypted !== true) {
    throw new Error("MANAGED_INSTALL_COMPLETION_FAILED");
  }
  required(installed.workspace_id, "workspace_id");
  return Object.freeze({
    contract_version: CONTRACT_VERSION,
    status: "active",
    workspace_ready: true,
    reauthorization_required: false,
  });
}

module.exports = Object.freeze({CONTRACT_VERSION, ID_TOKEN_TYPE, OFFLINE_ACCESS_TOKEN_TYPE, bootstrapManagedInstallation});

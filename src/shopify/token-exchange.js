"use strict";

const {verifySessionToken} = require("./embedded-auth");

const CONTRACT_VERSION = "e10-t3b-v1";

function dependency(value, method, name) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${name}.${method} is required`);
  return value;
}

function exactTokenResult(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("TOKEN_EXCHANGE_CONTRACT_FAILED");
  const allowed = ["access_token", "expires_at", "scope", "token_type"];
  if (Object.keys(value).some((key) => !allowed.includes(key))) throw new Error("TOKEN_EXCHANGE_CONTRACT_FAILED");
  if (typeof value.access_token !== "string" || value.access_token.length < 16) throw new Error("TOKEN_EXCHANGE_CONTRACT_FAILED");
  if (!Number.isSafeInteger(value.expires_at) || value.expires_at <= 0) throw new Error("TOKEN_EXCHANGE_CONTRACT_FAILED");
  if (typeof value.scope !== "string" || typeof value.token_type !== "string") throw new Error("TOKEN_EXCHANGE_CONTRACT_FAILED");
  return value;
}

async function exchangeAndPersistShopToken({session_token, auth_config, tenant_resolver, exchange_client, token_store}) {
  if (typeof tenant_resolver !== "function") throw new TypeError("tenant_resolver is required");
  dependency(exchange_client, "exchange", "exchange_client");
  dependency(token_store, "put", "token_store");
  const verified = verifySessionToken(session_token, auth_config);
  const tenant = await tenant_resolver({shop_domain: verified.shop_domain});
  if (!tenant || tenant.status !== "active" || tenant.shop_domain !== verified.shop_domain) throw new Error("SHOP_REAUTHORIZATION_REQUIRED");
  const token = exactTokenResult(await exchange_client.exchange({
    shop_domain: verified.shop_domain,
    subject_token: session_token,
    requested_token_type: "online_access_token",
  }));
  const stored = await token_store.put({
    shop_id: tenant.shop_id,
    workspace_id: tenant.workspace_id,
    access_token: token.access_token,
    expires_at: token.expires_at,
    scope: token.scope,
    token_type: token.token_type,
  });
  if (!stored || stored.shop_id !== tenant.shop_id || stored.encrypted !== true) throw new Error("TOKEN_PERSISTENCE_FAILED");
  return Object.freeze({
    contract_version: CONTRACT_VERSION,
    status: "active",
    shop_id: tenant.shop_id,
    workspace_id: tenant.workspace_id,
    expires_at: token.expires_at,
    reauthorization_required: false,
  });
}

module.exports = Object.freeze({CONTRACT_VERSION, exchangeAndPersistShopToken});

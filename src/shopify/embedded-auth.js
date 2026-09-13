"use strict";

const crypto = require("node:crypto");
const {normalizeShopDomain} = require("./tenant-model");

const CONTRACT_VERSION = "e10-t3a-v1";
const MAX_CLOCK_SKEW_SECONDS = 10;

function required(value, field) {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${field} is required`);
  return value;
}

function decodeBase64UrlJson(value, field) {
  try {
    const text = Buffer.from(required(value, field), "base64url").toString("utf8");
    const parsed = JSON.parse(text);
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error();
    return parsed;
  } catch {
    throw new Error(`INVALID_${field.toUpperCase()}`);
  }
}

function safeEqualHex(actual, expected) {
  if (!/^[a-f0-9]{64}$/i.test(actual || "")) return false;
  const left = Buffer.from(actual.toLowerCase(), "hex");
  const right = Buffer.from(expected.toLowerCase(), "hex");
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function verifyInstallCallback(query, {client_secret}) {
  required(client_secret, "client_secret");
  if (!query || typeof query !== "object" || Array.isArray(query)) throw new TypeError("query is required");
  const hmac = required(query.hmac, "hmac");
  const pairs = [];
  for (const key of Object.keys(query).sort()) {
    if (key === "hmac" || key === "signature") continue;
    const value = query[key];
    if (typeof value !== "string") throw new Error("INVALID_INSTALL_CALLBACK");
    pairs.push(`${key}=${value}`);
  }
  const expected = crypto.createHmac("sha256", client_secret).update(pairs.join("&")).digest("hex");
  if (!safeEqualHex(hmac, expected)) throw new Error("INVALID_INSTALL_HMAC");
  return Object.freeze({
    authority: "shopify_verified_callback",
    shop_domain: normalizeShopDomain(required(query.shop, "shop")),
    state: required(query.state, "state"),
  });
}

async function consumeInstallState({state_store, state, shop_domain, now}) {
  if (!state_store || typeof state_store.consume !== "function") throw new TypeError("state_store.consume is required");
  const consumed = await state_store.consume(required(state, "state"));
  if (!consumed || consumed.shop_domain !== shop_domain || consumed.expires_at <= now) {
    throw new Error("INVALID_OR_EXPIRED_INSTALL_STATE");
  }
  return Object.freeze({shop_domain, ads_table_user_id: required(consumed.ads_table_user_id, "ads_table_user_id")});
}

function verifySessionToken(token, {client_id, client_secret, now_seconds = Math.floor(Date.now() / 1000)}) {
  required(client_id, "client_id");
  required(client_secret, "client_secret");
  if (!Number.isSafeInteger(now_seconds) || now_seconds < 0) throw new TypeError("now_seconds is invalid");
  const parts = required(token, "session_token").split(".");
  if (parts.length !== 3) throw new Error("INVALID_SESSION_TOKEN");
  const header = decodeBase64UrlJson(parts[0], "session_header");
  const payload = decodeBase64UrlJson(parts[1], "session_payload");
  if (header.alg !== "HS256" || header.typ !== "JWT") throw new Error("INVALID_SESSION_ALGORITHM");
  const expected = crypto.createHmac("sha256", client_secret).update(`${parts[0]}.${parts[1]}`).digest();
  let actual;
  try { actual = Buffer.from(parts[2], "base64url"); } catch { throw new Error("INVALID_SESSION_SIGNATURE"); }
  if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) throw new Error("INVALID_SESSION_SIGNATURE");
  if (payload.aud !== client_id || !Number.isSafeInteger(payload.exp) || !Number.isSafeInteger(payload.nbf)) throw new Error("INVALID_SESSION_CLAIMS");
  if (payload.exp <= now_seconds - MAX_CLOCK_SKEW_SECONDS || payload.nbf > now_seconds + MAX_CLOCK_SKEW_SECONDS) throw new Error("EXPIRED_OR_EARLY_SESSION");
  const destination = new URL(required(payload.dest, "dest"));
  const shopDomain = normalizeShopDomain(destination.hostname);
  if (destination.protocol !== "https:" || destination.pathname !== "/" || destination.search || destination.hash) throw new Error("INVALID_SESSION_DESTINATION");
  const issuer = new URL(required(payload.iss, "iss"));
  if (issuer.protocol !== "https:" || issuer.hostname !== shopDomain || issuer.pathname !== "/admin" || issuer.search || issuer.hash) throw new Error("INVALID_SESSION_ISSUER");
  return Object.freeze({
    authority: "shopify_verified_session",
    shop_domain: shopDomain,
    shopify_user_id: required(String(payload.sub || ""), "sub"),
    session_id: required(payload.sid, "sid"),
    expires_at: payload.exp,
  });
}

async function authenticateEmbeddedRequest({session_token, config, tenant_resolver}) {
  if (typeof tenant_resolver !== "function") throw new TypeError("tenant_resolver is required");
  const verified = verifySessionToken(session_token, config);
  const tenant = await tenant_resolver({shop_domain: verified.shop_domain});
  if (!tenant || tenant.status !== "active" || tenant.shop_domain !== verified.shop_domain) throw new Error("SHOP_REAUTHORIZATION_REQUIRED");
  return Object.freeze({
    contract_version: CONTRACT_VERSION,
    authority: verified.authority,
    shop_id: required(tenant.shop_id, "shop_id"),
    workspace_id: required(tenant.workspace_id, "workspace_id"),
    shopify_user_id: verified.shopify_user_id,
    expires_at: verified.expires_at,
  });
}

module.exports = Object.freeze({CONTRACT_VERSION, verifyInstallCallback, consumeInstallState, verifySessionToken, authenticateEmbeddedRequest});

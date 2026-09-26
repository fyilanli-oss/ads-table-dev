"use strict";

const GOOGLE_ADS_SCOPE = "https://www.googleapis.com/auth/adwords";

function normalize(payload) {
  const data = payload?.data && typeof payload.data === "object" ? payload.data : payload;
  const accessToken = data?.access_token;
  if (typeof accessToken !== "string" || !accessToken) throw new Error("INVALID_PROVIDER_TOKEN_RESPONSE");
  const rawScopes = Array.isArray(data.scope) ? data.scope : (typeof data.scope === "string" ? data.scope.split(/[\s,]+/) : []);
  const scopes = [...new Set(rawScopes.map(scope => typeof scope === "string" ? scope.trim() : "").filter(Boolean))];
  const expiresIn = Number(data.expires_in);
  return Object.freeze({
    accessToken,
    refreshToken: typeof data.refresh_token === "string" && data.refresh_token ? data.refresh_token : null,
    expiresIn: Number.isSafeInteger(expiresIn) && expiresIn > 0 ? expiresIn : null,
    scopes: Object.freeze(scopes),
  });
}

async function requestToken(fetchImpl, url, options) {
  const response = await fetchImpl(url, options);
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || typeof payload !== "object") throw new Error("PROVIDER_TOKEN_EXCHANGE_FAILED");
  return normalize(payload);
}

function failure(code) {
  return Object.assign(new Error(code), {code});
}

async function requestMetaJson(fetchImpl, url, options, errorCode) {
  const response = await fetchImpl(url, options);
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || typeof payload !== "object") throw failure(errorCode);
  return payload;
}

function metaScopes(data) {
  return Object.freeze([...new Set((Array.isArray(data?.scopes) ? data.scopes : [])
    .map(scope => typeof scope === "string" ? scope.trim() : "").filter(Boolean))]);
}

async function exchangeAndValidateMetaToken({fetchImpl, graphVersion, now, formHeaders, form, code, redirectUri, clientId, clientSecret}) {
  const base = `https://graph.facebook.com/${graphVersion}`;
  const shortPayload = await requestMetaJson(fetchImpl, `${base}/oauth/access_token`, {
    method: "POST",
    headers: formHeaders,
    body: form({client_id: clientId, client_secret: clientSecret, redirect_uri: redirectUri, code}),
  }, "META_TOKEN_CODE_EXCHANGE_FAILED");
  const shortToken = normalize(shortPayload).accessToken;

  const longPayload = await requestMetaJson(fetchImpl, `${base}/oauth/access_token`, {
    method: "POST",
    headers: formHeaders,
    body: form({grant_type: "fb_exchange_token", client_id: clientId, client_secret: clientSecret, fb_exchange_token: shortToken}),
  }, "META_LONG_LIVED_TOKEN_EXCHANGE_FAILED");
  const longToken = normalize(longPayload).accessToken;

  const debugUrl = new URL(`${base}/debug_token`);
  debugUrl.searchParams.set("input_token", longToken);
  const debugPayload = await requestMetaJson(fetchImpl, debugUrl.href, {
    method: "GET",
    headers: {Accept: "application/json", Authorization: `Bearer ${clientId}|${clientSecret}`},
  }, "META_TOKEN_VALIDATION_FAILED");
  const data = debugPayload.data;
  if (!data || data.is_valid !== true) throw failure("META_TOKEN_REAUTHORIZATION_REQUIRED");
  if (String(data.app_id || "") !== String(clientId)) throw failure("META_TOKEN_APP_MISMATCH");
  const scopes = metaScopes(data);
  if (!scopes.includes("ads_read")) throw failure("META_TOKEN_SCOPE_MISSING");
  const expiresAtSeconds = Number(data.expires_at);
  const nowSeconds = Math.floor(now().getTime() / 1000);
  if (!Number.isSafeInteger(expiresAtSeconds) || expiresAtSeconds <= nowSeconds) {
    throw failure("META_TOKEN_REAUTHORIZATION_REQUIRED");
  }
  return Object.freeze({
    accessToken: longToken,
    refreshToken: null,
    expiresIn: expiresAtSeconds - nowSeconds,
    scopes,
  });
}

async function exchangeAndValidateGoogleToken({fetchImpl, formHeaders, form, code, redirectUri, clientId, clientSecret}) {
  const response = await fetchImpl("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: formHeaders,
    body: form({grant_type: "authorization_code", client_id: clientId, client_secret: clientSecret, redirect_uri: redirectUri, code}),
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || typeof payload !== "object") throw failure("GOOGLE_TOKEN_CODE_EXCHANGE_FAILED");
  const tokens = normalize(payload);
  if (!tokens.refreshToken) throw failure("GOOGLE_REFRESH_TOKEN_REQUIRED");
  if (!tokens.expiresIn) throw failure("GOOGLE_TOKEN_EXPIRY_REQUIRED");
  if (!tokens.scopes.includes(GOOGLE_ADS_SCOPE)) throw failure("GOOGLE_ADS_SCOPE_MISSING");
  return tokens;
}

function createEmbeddedProviderTokenExchanges({fetchImpl = fetch, metaGraphVersion = "v20.0", now = () => new Date()} = {}) {
  if (typeof fetchImpl !== "function") throw new TypeError("fetchImpl is required");
  if (!/^v\d+\.\d+$/.test(metaGraphVersion)) throw new TypeError("metaGraphVersion is invalid");
  if (typeof now !== "function") throw new TypeError("now is required");
  const form = values => new URLSearchParams(values).toString();
  const formHeaders = {"Content-Type": "application/x-www-form-urlencoded", Accept: "application/json"};
  return Object.freeze({
    meta: input => exchangeAndValidateMetaToken({fetchImpl, graphVersion: metaGraphVersion, now, formHeaders, form, ...input}),
    google_ads: input => exchangeAndValidateGoogleToken({fetchImpl, formHeaders, form, ...input}),
    klaviyo: ({code, redirectUri, pkceVerifier, clientId, clientSecret}) => requestToken(fetchImpl, "https://a.klaviyo.com/oauth/token", {method: "POST", headers: {...formHeaders, Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`}, body: form({grant_type: "authorization_code", redirect_uri: redirectUri, code, code_verifier: pkceVerifier})}),
    tiktok: ({code, clientId, clientSecret}) => requestToken(fetchImpl, "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/", {method: "POST", headers: {"Content-Type": "application/json", Accept: "application/json"}, body: JSON.stringify({app_id: clientId, secret: clientSecret, auth_code: code})}),
    pinterest: ({code, redirectUri, clientId, clientSecret}) => requestToken(fetchImpl, "https://api.pinterest.com/v5/oauth/token", {method: "POST", headers: {...formHeaders, Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`}, body: form({grant_type: "authorization_code", redirect_uri: redirectUri, code})}),
  });
}

module.exports = Object.freeze({createEmbeddedProviderTokenExchanges, normalize, exchangeAndValidateMetaToken, exchangeAndValidateGoogleToken, GOOGLE_ADS_SCOPE});


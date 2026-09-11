"use strict";

function normalize(payload) {
  const data = payload?.data && typeof payload.data === "object" ? payload.data : payload;
  const accessToken = data?.access_token;
  if (typeof accessToken !== "string" || !accessToken) throw new Error("INVALID_PROVIDER_TOKEN_RESPONSE");
  return Object.freeze({accessToken, refreshToken: typeof data.refresh_token === "string" && data.refresh_token ? data.refresh_token : null});
}

async function requestToken(fetchImpl, url, options) {
  const response = await fetchImpl(url, options);
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || typeof payload !== "object") throw new Error("PROVIDER_TOKEN_EXCHANGE_FAILED");
  return normalize(payload);
}

function createEmbeddedProviderTokenExchanges({fetchImpl = fetch} = {}) {
  if (typeof fetchImpl !== "function") throw new TypeError("fetchImpl is required");
  const form = values => new URLSearchParams(values).toString();
  const formHeaders = {"Content-Type": "application/x-www-form-urlencoded", Accept: "application/json"};
  return Object.freeze({
    meta: ({code, redirectUri, clientId, clientSecret}) => requestToken(fetchImpl, "https://graph.facebook.com/v20.0/oauth/access_token", {method: "POST", headers: formHeaders, body: form({client_id: clientId, client_secret: clientSecret, redirect_uri: redirectUri, code})}),
    google_ads: ({code, redirectUri, clientId, clientSecret}) => requestToken(fetchImpl, "https://oauth2.googleapis.com/token", {method: "POST", headers: formHeaders, body: form({grant_type: "authorization_code", client_id: clientId, client_secret: clientSecret, redirect_uri: redirectUri, code})}),
    klaviyo: ({code, redirectUri, pkceVerifier, clientId, clientSecret}) => requestToken(fetchImpl, "https://a.klaviyo.com/oauth/token", {method: "POST", headers: {...formHeaders, Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`}, body: form({grant_type: "authorization_code", redirect_uri: redirectUri, code, code_verifier: pkceVerifier})}),
    tiktok: ({code, clientId, clientSecret}) => requestToken(fetchImpl, "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/", {method: "POST", headers: {"Content-Type": "application/json", Accept: "application/json"}, body: JSON.stringify({app_id: clientId, secret: clientSecret, auth_code: code})}),
    pinterest: ({code, redirectUri, clientId, clientSecret}) => requestToken(fetchImpl, "https://api.pinterest.com/v5/oauth/token", {method: "POST", headers: {...formHeaders, Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`}, body: form({grant_type: "authorization_code", redirect_uri: redirectUri, code})}),
  });
}

module.exports = Object.freeze({createEmbeddedProviderTokenExchanges, normalize});

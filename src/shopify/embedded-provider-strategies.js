"use strict";

const crypto = require("node:crypto");

const SPECS = Object.freeze({
  meta: {client: "META_APP_ID", secret: "META_APP_SECRET", scope: "ads_read", authorize: "https://www.facebook.com/v20.0/dialog/oauth"},
  google_ads: {client: "GOOGLE_CLIENT_ID", secret: "GOOGLE_CLIENT_SECRET", scope: "https://www.googleapis.com/auth/adwords", authorize: "https://accounts.google.com/o/oauth2/v2/auth"},
  klaviyo: {client: "KLAVIYO_CLIENT_ID", secret: "KLAVIYO_CLIENT_SECRET", scope: "accounts:read campaigns:read events:read metrics:read", authorize: "https://www.klaviyo.com/oauth/authorize"},
  tiktok: {client: "TIKTOK_CLIENT_ID", secret: "TIKTOK_CLIENT_SECRET", scope: "", authorize: "https://business-api.tiktok.com/portal/auth"},
  pinterest: {client: "PINTEREST_CLIENT_ID", secret: "PINTEREST_CLIENT_SECRET", scope: "ads:read,user_accounts:read", authorize: "https://www.pinterest.com/oauth/"},
});

function value(env, name) {
  return typeof env[name] === "string" ? env[name].trim() : "";
}

function createPkce() {
  const verifier = crypto.randomBytes(32).toString("base64url");
  const challenge = crypto.createHash("sha256").update(verifier).digest("base64url");
  return Object.freeze({verifier, challenge});
}

function createEmbeddedProviderStrategies({env = process.env, appUrl, exchangeCodeByProvider} = {}) {
  if (!appUrl || typeof appUrl !== "string") throw new TypeError("appUrl is required");
  if (!exchangeCodeByProvider || typeof exchangeCodeByProvider !== "object") throw new TypeError("exchangeCodeByProvider is required");
  const strategies = {};
  for (const [provider, spec] of Object.entries(SPECS)) {
    const clientId = value(env, spec.client);
    const clientSecret = value(env, spec.secret);
    const exchange = exchangeCodeByProvider[provider];
    if (!clientId || !clientSecret || typeof exchange !== "function") throw new Error(`EMBEDDED_${provider.toUpperCase()}_OAUTH_CONFIG_INCOMPLETE`);
    const redirectUri = `${appUrl}/api/shopify/providers/${provider}/oauth/callback`;
    strategies[provider] = Object.freeze({
      redirectUri,
      async buildAuthorizationUrl({state, pkceChallenge = null}) {
        const params = new URLSearchParams({client_id: clientId, redirect_uri: redirectUri, state, response_type: "code"});
        if (spec.scope) params.set("scope", spec.scope);
        if (provider === "google_ads") { params.set("access_type", "offline"); params.set("prompt", "consent"); }
        if (provider === "tiktok") { params.delete("client_id"); params.delete("response_type"); params.set("app_id", clientId); }
        if (provider === "klaviyo") {
          if (!pkceChallenge) throw new Error("KLAVIYO_PKCE_REQUIRED");
          params.set("code_challenge_method", "S256");
          params.set("code_challenge", pkceChallenge);
        }
        return `${spec.authorize}?${params}`;
      },
      exchangeCode: input => exchange({...input, clientId, clientSecret}),
      ...(provider === "klaviyo" ? {createPkce} : {}),
    });
  }
  return Object.freeze(strategies);
}

module.exports = Object.freeze({createEmbeddedProviderStrategies, SPECS});

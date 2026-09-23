"use strict";

const {bearerToken} = require("./shopify-auth-routes");
const SAFE_ERRORS = new Set(["INVALID_PLAN_COST", "INVALID_ACCOUNT", "KLAVIYO_REAUTHORIZE", "KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED", "KLAVIYO_UNAVAILABLE", "KLAVIYO_RESET_CONFIRMATION_REQUIRED", "KLAVIYO_REAUTHORIZATION_REQUIRED", "KLAVIYO_REVOKE_FAILED", "CONNECTION_CHANGED"]);

function registerShopifyKlaviyoAccountRoutes(app, {authenticateEmbedded, selection, reset}) {
  const handler = action => async (req, res) => {
    res.set("Cache-Control", "no-store");
    let authority;
    try {
      authority = await authenticateEmbedded({session_token: bearerToken(req.get("authorization"))});
    } catch {
      return res.status(401).json({code: "SHOPIFY_SESSION_REQUIRED"});
    }
    try {
      return res.status(200).json(await action(authority, req.body));
    } catch (error) {
      const known = SAFE_ERRORS.has(error.code);
      return res.status(known ? error.status || 503 : 503).json({code: known ? error.code : "KLAVIYO_UNAVAILABLE"});
    }
  };
  app.get("/api/shopify/providers/klaviyo/accounts/status", handler(authority => selection.status(authority)));
  app.get("/api/shopify/providers/klaviyo/accounts/verify", handler(authority => selection.verifyReadOnly(authority)));
  app.get("/api/shopify/providers/klaviyo/accounts", handler(authority => selection.list(authority)));
  app.post("/api/shopify/providers/klaviyo/accounts/select", handler((authority, body) => selection.complete(authority, body)));
  if (reset) app.post("/api/shopify/providers/klaviyo/accounts/reset", handler((authority, body) => reset.execute(authority, body?.confirmation)));
}

module.exports = {registerShopifyKlaviyoAccountRoutes};

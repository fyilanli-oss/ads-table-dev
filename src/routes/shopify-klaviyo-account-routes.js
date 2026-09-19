"use strict";

const {bearerToken} = require("./shopify-auth-routes");
const SAFE_ERRORS = new Set(["INVALID_PLAN_COST", "INVALID_ACCOUNT", "KLAVIYO_REAUTHORIZE", "KLAVIYO_UNAVAILABLE", "CONNECTION_CHANGED"]);

function registerShopifyKlaviyoAccountRoutes(app, {authenticateEmbedded, selection}) {
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
  app.get("/api/shopify/providers/klaviyo/accounts", handler(authority => selection.list(authority)));
  app.post("/api/shopify/providers/klaviyo/accounts/select", handler((authority, body) => selection.complete(authority, body)));
}

module.exports = {registerShopifyKlaviyoAccountRoutes};

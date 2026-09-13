"use strict";

const {bearerToken} = require("./shopify-auth-routes");
const {RETURN_TARGET} = require("../shopify/embedded-provider-oauth");

const PROVIDERS = Object.freeze(["meta", "google_ads", "klaviyo", "tiktok", "pinterest"]);

function registerShopifyProviderOAuthRoutes(app, {adapters} = {}) {
  if (!app || typeof app.get !== "function" || typeof app.post !== "function") throw new TypeError("Express app is required");
  if (!adapters || typeof adapters !== "object" || Array.isArray(adapters)) throw new TypeError("adapters are required");
  for (const provider of PROVIDERS) {
    const adapter = adapters[provider];
    if (!adapter || typeof adapter.start !== "function" || typeof adapter.callback !== "function") {
      throw new TypeError(`adapter.${provider} is required`);
    }
  }

  app.post("/api/shopify/providers/:provider/oauth/start", async (req, res, next) => {
    try {
      const adapter = PROVIDERS.includes(req.params.provider) ? adapters[req.params.provider] : null;
      if (!adapter) return res.status(404).json({code: "SHOPIFY_PROVIDER_NOT_FOUND"});
      const result = await adapter.start({sessionToken: bearerToken(req.get("authorization"))});
      return res.status(200).json(result);
    } catch (error) { return next(error); }
  });

  app.get("/api/shopify/providers/:provider/oauth/callback", async (req, res) => {
    const adapter = PROVIDERS.includes(req.params.provider) ? adapters[req.params.provider] : null;
    if (!adapter) return res.redirect(`${RETURN_TARGET}?oauth_error=provider_not_found`);
    try {
      await adapter.callback({state: req.query.state, code: req.query.code});
      return res.redirect(`${RETURN_TARGET}?oauth_connected=${encodeURIComponent(req.params.provider)}&account_selection_required=1`);
    } catch {
      return res.redirect(`${RETURN_TARGET}?oauth_error=connection_failed`);
    }
  });
}

module.exports = Object.freeze({registerShopifyProviderOAuthRoutes, PROVIDERS});

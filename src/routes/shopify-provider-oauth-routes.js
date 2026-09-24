"use strict";

const {bearerToken} = require("./shopify-auth-routes");
const {RETURN_TARGET} = require("../shopify/embedded-provider-oauth");

const PROVIDERS = Object.freeze(["meta", "google_ads", "klaviyo"]);

function verifiedAdminTarget(value) {
  const target = new URL(value);
  if (target.protocol !== "https:" || !/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(target.hostname) ||
    target.port || target.username || target.password || !/^\/admin\/apps\/[A-Za-z0-9_-]+$/.test(target.pathname) || target.search || target.hash) {
    throw new Error("INVALID_EMBEDDED_RETURN_TARGET");
  }
  return target;
}

function safeErrorCode(error) {
  const code = error?.code || error?.message;
  return typeof code === "string" && /^[A-Z0-9_]{1,64}$/.test(code) ? code : "EMBEDDED_OAUTH_CALLBACK_FAILED";
}

function registerShopifyProviderOAuthRoutes(app, {adapters} = {}) {
  if (!app || typeof app.get !== "function" || typeof app.post !== "function") throw new TypeError("Express app is required");
  if (!adapters || typeof adapters !== "object" || Array.isArray(adapters)) throw new TypeError("adapters are required");
  for (const [provider, adapter] of Object.entries(adapters)) {
    if (!PROVIDERS.includes(provider) || !adapter || typeof adapter.start !== "function" || typeof adapter.callback !== "function") throw new TypeError(`adapter.${provider} is invalid`);
  }

  app.post("/api/shopify/providers/:provider/oauth/start", async (req, res, next) => {
    try {
      if (!PROVIDERS.includes(req.params.provider)) return res.status(404).json({code: "SHOPIFY_PROVIDER_NOT_FOUND"});
      const adapter = adapters[req.params.provider];
      if (!adapter) return res.status(503).json({code: "SHOPIFY_PROVIDER_NOT_CONFIGURED"});
      const result = await adapter.start({sessionToken: bearerToken(req.get("authorization"))});
      return res.status(200).json(result);
    } catch (error) { return next(error); }
  });

  app.get("/api/shopify/providers/:provider/oauth/callback", async (req, res) => {
    if (!PROVIDERS.includes(req.params.provider)) return res.redirect(`${RETURN_TARGET}?oauth_error=provider_not_found`);
    const adapter = adapters[req.params.provider];
    if (!adapter) return res.redirect(`${RETURN_TARGET}?oauth_error=provider_not_configured`);
    try {
      const result = await adapter.callback({state: req.query.state, code: req.query.code});
      if (result?.redirect_to && result.redirect_to !== RETURN_TARGET) {
        const target = verifiedAdminTarget(result.redirect_to);
        return res.redirect(target.href);
      }
      return res.redirect(`${RETURN_TARGET}?oauth_connected=${encodeURIComponent(req.params.provider)}&account_selection_required=1`);
    } catch (error) {
      console.error(JSON.stringify({event: "embedded_provider_oauth_callback_failed", provider: req.params.provider, code: safeErrorCode(error)}));
      if (error?.embedded_return_target) {
        try {
          const target = verifiedAdminTarget(error.embedded_return_target);
          target.searchParams.set("oauth_error", "connection_failed");
          return res.redirect(target.href);
        } catch {}
      }
      return res.redirect(`${RETURN_TARGET}?oauth_error=connection_failed`);
    }
  });
}

module.exports = Object.freeze({registerShopifyProviderOAuthRoutes, PROVIDERS});

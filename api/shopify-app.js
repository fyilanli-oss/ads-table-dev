"use strict";

const {
  EMBEDDED_HOME_RELEASE,
  renderEmbeddedAppHome,
  renderEmbeddedPlatforms,
  renderEmbeddedPlaceholder,
} = require("../src/shopify/embedded-app-home");
const {configuredOAuthProviders} = require("../src/shopify/embedded-provider-strategies");

function handler(req, res) {
  if (req.method && req.method !== "GET" && req.method !== "HEAD") {
    res.statusCode = 405;
    res.setHeader("Allow", "GET, HEAD");
    return res.end();
  }

  const path = new URL(req.url, "https://dev.adstable.app").pathname;
  const allowedPaths = new Set(["/", "/shopify/app", "/shopify/app/settings", "/shopify/app/platforms", "/shopify/app/funnel", "/shopify/app/analysis"]);
  if (!allowedPaths.has(path)) {
    res.statusCode = 404;
    return res.end();
  }

  const clientId = String(process.env.SHOPIFY_API_KEY || "").trim();
  if (!clientId) {
    res.statusCode = 503;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.end("Shopify application configuration is unavailable.");
  }

  const settings = path === "/shopify/app/settings" || path === "/shopify/app/platforms";
  const placeholderPage = path === "/shopify/app/funnel" ? "Funnel" : path === "/shopify/app/analysis" ? "Analysis" : null;
  const providerOAuthEnabled = process.env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED === "true";
  const configuredProviders = new Set(configuredOAuthProviders(process.env));
  const html = settings
    ? renderEmbeddedPlatforms({
        clientId,
        providerOAuthEnabled,
        providerAvailability: {
          meta: providerOAuthEnabled && configuredProviders.has("meta"),
          google_ads: providerOAuthEnabled && configuredProviders.has("google_ads") && Boolean(String(process.env.GOOGLE_ADS_DEVELOPER_TOKEN || "").trim()),
          klaviyo: providerOAuthEnabled && configuredProviders.has("klaviyo"),
        },
      })
    : placeholderPage
      ? renderEmbeddedPlaceholder({clientId, page: placeholderPage})
      : renderEmbeddedAppHome({clientId});

  res.statusCode = 200;
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0");
  res.setHeader("CDN-Cache-Control", "no-store");
  res.setHeader("Vercel-CDN-Cache-Control", "no-store");
  res.setHeader("Surrogate-Control", "no-store");
  res.setHeader("X-AdsTable-Release", EMBEDDED_HOME_RELEASE);
  res.setHeader("Content-Security-Policy", "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
  if (req.method === "HEAD") return res.end();
  return res.end(html);
}

module.exports = handler;

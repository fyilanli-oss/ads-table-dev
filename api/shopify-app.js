"use strict";

const {
  EMBEDDED_HOME_RELEASE,
  renderEmbeddedAppHome,
  renderEmbeddedPlatforms,
} = require("../src/shopify/embedded-app-home");

function handler(req, res) {
  if (req.method && req.method !== "GET" && req.method !== "HEAD") {
    res.statusCode = 405;
    res.setHeader("Allow", "GET, HEAD");
    return res.end();
  }

  const path = new URL(req.url, "https://dev.adstable.app").pathname;
  if (path !== "/" && path !== "/shopify/app" && path !== "/shopify/app/platforms") {
    res.statusCode = 404;
    return res.end();
  }

  const clientId = String(process.env.SHOPIFY_API_KEY || "").trim();
  if (!clientId) {
    res.statusCode = 503;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.end("Shopify application configuration is unavailable.");
  }

  const platforms = path === "/shopify/app/platforms";
  const html = platforms
    ? renderEmbeddedPlatforms({
        clientId,
        providerOAuthEnabled: process.env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED === "true",
      })
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

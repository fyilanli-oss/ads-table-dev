"use strict";

const REQUIRED_ENV = Object.freeze([
  "SHOPIFY_API_KEY",
  "SHOPIFY_API_SECRET",
  "SHOPIFY_APP_URL",
  "SHOPIFY_DEV_STORE",
]);
const ALLOWED_SCOPES = Object.freeze(["read_reports"]);

function normalized(value) {
  return String(value || "").trim();
}

function validateHttpsUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError("SHOPIFY_APP_URL must be an absolute HTTPS URL");
  }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    throw new TypeError("SHOPIFY_APP_URL must be an origin-only HTTPS URL");
  }
  return url.origin;
}

function validateShop(value) {
  const shop = value.toLowerCase();
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(shop)) {
    throw new TypeError("SHOPIFY_DEV_STORE must be a canonical myshopify.com domain");
  }
  return shop;
}

function parseScopes(value) {
  const scopes = [...new Set(normalized(value).split(",").map(scope => scope.trim()).filter(Boolean))].sort();
  const unsupported = scopes.filter(scope => !ALLOWED_SCOPES.includes(scope));
  if (unsupported.length) throw new Error("SHOPIFY_SCOPE_NOT_APPROVED");
  return scopes;
}

function inspectDevelopmentBootstrap({env = process.env, cliAvailable = false} = {}) {
  const missing = REQUIRED_ENV.filter(name => !normalized(env[name]));
  if (missing.length || !cliAvailable) {
    return Object.freeze({
      contract_version: "e10-t6b-v1",
      status: "BLOCKED",
      safe_code: missing.length ? "BLOCKED_BOOTSTRAP_ENV" : "BLOCKED_SHOPIFY_CLI",
      missing,
      cli_available: Boolean(cliAvailable),
      shopify_contact: false,
      production_contact: false,
    });
  }

  const appUrl = validateHttpsUrl(normalized(env.SHOPIFY_APP_URL));
  const scopes = parseScopes(env.SHOPIFY_SCOPES);
  return Object.freeze({
    contract_version: "e10-t6b-v1",
    status: "READY_FOR_MANUAL_BOOTSTRAP",
    safe_code: "BOOTSTRAP_PREFLIGHT_PASS",
    cli_available: true,
    shopify_contact: false,
    production_contact: false,
    app_url: appUrl,
    callback_url: `${appUrl}/auth/shopify/callback`,
    development_store: validateShop(normalized(env.SHOPIFY_DEV_STORE)),
    scopes,
  });
}

module.exports = Object.freeze({ALLOWED_SCOPES, REQUIRED_ENV, inspectDevelopmentBootstrap});

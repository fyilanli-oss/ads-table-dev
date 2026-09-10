"use strict";

const {normalizeShopDomain} = require("../shopify/tenant-model");

const VARIABLES = Object.freeze([
  "SHOPIFY_API_KEY",
  "SHOPIFY_API_SECRET",
  "SHOPIFY_APP_URL",
  "SHOPIFY_DEV_STORE_DOMAIN",
]);

class ShopifyConfigError extends Error {
  constructor(code, missing = []) {
    super(code);
    this.name = "ShopifyConfigError";
    this.code = code;
    this.missing = Object.freeze([...missing]);
  }
}

function value(env, name) {
  return typeof env[name] === "string" ? env[name].trim() : "";
}

function loadShopifyConfig(env = process.env) {
  if (!env || typeof env !== "object" || Array.isArray(env)) throw new TypeError("env must be an object");
  const present = VARIABLES.filter((name) => value(env, name));
  if (present.length === 0) return Object.freeze({enabled: false});
  const missing = VARIABLES.filter((name) => !value(env, name));
  if (missing.length) throw new ShopifyConfigError("SHOPIFY_CONFIG_INCOMPLETE", missing);

  let appUrl;
  try { appUrl = new URL(value(env, "SHOPIFY_APP_URL")); } catch { throw new ShopifyConfigError("SHOPIFY_APP_URL_INVALID"); }
  if (appUrl.protocol !== "https:" || appUrl.username || appUrl.password || appUrl.search || appUrl.hash) {
    throw new ShopifyConfigError("SHOPIFY_APP_URL_INVALID");
  }
  return Object.freeze({
    enabled: true,
    clientId: value(env, "SHOPIFY_API_KEY"),
    clientSecret: value(env, "SHOPIFY_API_SECRET"),
    appUrl: appUrl.toString().replace(/\/$/, ""),
    developmentStoreDomain: normalizeShopDomain(value(env, "SHOPIFY_DEV_STORE_DOMAIN")),
  });
}

function shopifyConfigStatus(env = process.env) {
  const present = VARIABLES.filter((name) => value(env, name));
  return Object.freeze({
    configured: present.length === VARIABLES.length,
    visible_count: present.length,
    required_count: VARIABLES.length,
  });
}

module.exports = Object.freeze({VARIABLES, ShopifyConfigError, loadShopifyConfig, shopifyConfigStatus});

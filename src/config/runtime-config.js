"use strict";

const path = require("node:path");
const { loadProductionConfig } = require("../../security/production-config");

function parsePort(value) {
  const candidate = value === undefined || value === null || value === "" ? "3000" : String(value);
  if (!/^\d+$/.test(candidate)) throw new TypeError("PORT must be an integer");
  const port = Number(candidate);
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535) {
    throw new TypeError("PORT must be between 0 and 65535");
  }
  return port;
}

function loadRuntimeConfig({
  env = process.env,
  rootDirectory,
  logger = console,
  loadSecurityConfig = loadProductionConfig,
} = {}) {
  if (!rootDirectory || !path.isAbsolute(rootDirectory)) {
    throw new TypeError("rootDirectory must be an absolute path");
  }
  if (typeof loadSecurityConfig !== "function") {
    throw new TypeError("loadSecurityConfig must be a function");
  }

  return Object.freeze({
    port: parsePort(env.PORT),
    publicDirectory: path.join(rootDirectory, "public"),
    production: loadSecurityConfig(env, logger),
  });
}

module.exports = { loadRuntimeConfig, parsePort };


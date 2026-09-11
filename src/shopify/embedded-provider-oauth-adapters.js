"use strict";

const {createEmbeddedProviderOAuth} = require("./embedded-provider-oauth");
const {PROVIDERS} = require("../routes/shopify-provider-oauth-routes");

function requiredFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} is required`);
  return value;
}

function createEmbeddedProviderOAuthAdapters({
  authenticateEmbedded,
  createEmbeddedTransaction,
  consumeTransaction,
  connectionStore,
  providerStrategies,
} = {}) {
  requiredFunction(authenticateEmbedded, "authenticateEmbedded");
  requiredFunction(createEmbeddedTransaction, "createEmbeddedTransaction");
  requiredFunction(consumeTransaction, "consumeTransaction");
  if (!connectionStore || typeof connectionStore.writeFromOAuthTransaction !== "function") throw new TypeError("connectionStore is required");
  if (!providerStrategies || typeof providerStrategies !== "object" || Array.isArray(providerStrategies)) throw new TypeError("providerStrategies are required");

  const adapters = {};
  for (const provider of PROVIDERS) {
    const strategy = providerStrategies[provider];
    if (!strategy || typeof strategy.redirectUri !== "string" || !strategy.redirectUri) throw new TypeError(`providerStrategies.${provider}.redirectUri is required`);
    requiredFunction(strategy.buildAuthorizationUrl, `providerStrategies.${provider}.buildAuthorizationUrl`);
    requiredFunction(strategy.exchangeCode, `providerStrategies.${provider}.exchangeCode`);
    adapters[provider] = createEmbeddedProviderOAuth({
      provider,
      redirectUri: strategy.redirectUri,
      authenticateEmbedded,
      createEmbeddedTransaction,
      consumeTransaction,
      buildAuthorizationUrl: strategy.buildAuthorizationUrl,
      exchangeCode: strategy.exchangeCode,
      createPkce: strategy.createPkce || null,
      connectionStore,
    });
  }
  return Object.freeze(adapters);
}

module.exports = Object.freeze({createEmbeddedProviderOAuthAdapters});

"use strict";

const {shopifyConfigStatus, loadShopifyConfig} = require("../src/config/shopify-config");
const {parseKeyring} = require("../security/provider-token-vault");
const {SPECS} = require("../src/shopify/embedded-provider-strategies");

function present(env, name) { return typeof env[name] === "string" && Boolean(env[name].trim()); }

function activationPreflight(env = process.env) {
  const shopify = shopifyConfigStatus(env);
  let shopifyValid = false;
  let keyringValid = false;
  try { shopifyValid = loadShopifyConfig(env).enabled; } catch {}
  try { parseKeyring(env); keyringValid = true; } catch {}
  const providers = Object.fromEntries(Object.entries(SPECS).map(([provider, spec]) => [provider, Object.freeze({
    configured: present(env, spec.client) && present(env, spec.secret),
    required_input_count: 2,
    visible_input_count: [spec.client, spec.secret].filter(name => present(env, name)).length,
  })]));
  const databaseVisible = present(env, "SUPABASE_URL") && present(env, "SUPABASE_SERVICE_ROLE_KEY");
  const featureFlagDisabled = !present(env, "SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED") || env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED === "false";
  const ready = shopifyValid && keyringValid && databaseVisible && featureFlagDisabled && Object.values(providers).every(item => item.configured);
  return Object.freeze({contract_version: "e10-t6c-activation-preflight-v1", ready, shopify: {...shopify, contract_valid: shopifyValid}, token_keyring_valid: keyringValid, remote_database_credentials_visible: databaseVisible, feature_flag_safely_disabled: featureFlagDisabled, providers, values_or_lengths_exposed: false, provider_contact: false, production_contact: false});
}

if (require.main === module) {
  const result = activationPreflight();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (!result.ready) process.exitCode = 2;
}

module.exports = Object.freeze({activationPreflight});

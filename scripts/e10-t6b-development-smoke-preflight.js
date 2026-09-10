"use strict";

const {loadShopifyConfig, shopifyConfigStatus} = require("../src/config/shopify-config");
const {parseKeyring} = require("../security/provider-token-vault");

function developmentSmokePreflight(env = process.env) {
  const shopify = shopifyConfigStatus(env);
  const keyringInputs = ["PROVIDER_TOKEN_ACTIVE_KEY_ID", "PROVIDER_TOKEN_ENCRYPTION_KEYS"];
  const visibleKeyringInputCount = keyringInputs.filter((name) => (
    typeof env[name] === "string" && env[name].trim()
  )).length;
  let shopifyContractValid = false;
  let tokenKeyringValid = false;

  try { loadShopifyConfig(env); shopifyContractValid = shopify.configured; } catch {}
  try { parseKeyring(env); tokenKeyringValid = true; } catch {}

  const remoteDatabaseCredentialsVisible = Boolean(
    typeof env.SUPABASE_URL === "string" && env.SUPABASE_URL.trim()
    && typeof env.SUPABASE_SERVICE_ROLE_KEY === "string" && env.SUPABASE_SERVICE_ROLE_KEY.trim(),
  );
  const ready = shopifyContractValid && tokenKeyringValid && remoteDatabaseCredentialsVisible;

  return Object.freeze({
    contract_version: "e10-t6b-development-smoke-preflight-v1",
    ready,
    shopify: Object.freeze({...shopify, contract_valid: shopifyContractValid}),
    token_keyring: Object.freeze({
      configured: tokenKeyringValid,
      visible_count: visibleKeyringInputCount,
      required_count: keyringInputs.length,
    }),
    remote_database: Object.freeze({credentials_visible: remoteDatabaseCredentialsVisible}),
    values_or_lengths_exposed: false,
  });
}

if (require.main === module) {
  const result = developmentSmokePreflight(process.env);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (!result.ready) process.exitCode = 2;
}

module.exports = Object.freeze({developmentSmokePreflight});

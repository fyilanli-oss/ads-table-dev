"use strict";

const {loadShopifyConfig} = require("../config/shopify-config");
const {createProviderTokenVaultFromEnv} = require("../../security/provider-token-vault");
const {createShopifyExchangeClient, createShopifyAdminClient} = require("./admin-api-client");
const {bootstrapManagedInstallation} = require("./managed-installation");
const {authenticateEmbeddedRequest} = require("./embedded-auth");
const {createShopifyInstallService, createShopifyTenantResolver} = require("./install-service");
const {registerShopifyAuthRoutes} = require("../routes/shopify-auth-routes");

function registerShopifyRuntime({app, env = process.env, supabaseAdmin}) {
  const config = loadShopifyConfig(env);
  if (!config.enabled) return Object.freeze({enabled: false});
  if (!supabaseAdmin) throw new Error("Shopify managed installation requires Supabase service-role configuration");

  const vault = createProviderTokenVaultFromEnv(env);
  const exchangeClient = createShopifyExchangeClient({clientId: config.clientId, clientSecret: config.clientSecret});
  const adminClient = createShopifyAdminClient();
  const installService = createShopifyInstallService({client: supabaseAdmin, vault});
  const tenantResolver = createShopifyTenantResolver({client: supabaseAdmin});
  const authConfig = {client_id: config.clientId, client_secret: config.clientSecret};
  registerShopifyAuthRoutes(app, {
    bootstrap_managed_install: ({session_token}) => bootstrapManagedInstallation({
      session_token,
      auth_config: authConfig,
      exchange_client: exchangeClient,
      admin_client: adminClient,
      install_service: installService,
    }),
    authenticate_embedded: ({session_token}) => authenticateEmbeddedRequest({
      session_token,
      config: authConfig,
      tenant_resolver: tenantResolver,
    }),
  });
  return Object.freeze({enabled: true});
}

module.exports = Object.freeze({registerShopifyRuntime});

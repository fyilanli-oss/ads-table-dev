"use strict";

const {loadShopifyConfig} = require("../config/shopify-config");
const {createProviderTokenVaultFromEnv} = require("../../security/provider-token-vault");
const {createShopifyExchangeClient, createShopifyAdminClient} = require("./admin-api-client");
const {bootstrapManagedInstallation} = require("./managed-installation");
const {authenticateEmbeddedRequest} = require("./embedded-auth");
const {createShopifyInstallService, createShopifyTenantResolver} = require("./install-service");
const {registerShopifyAuthRoutes} = require("../routes/shopify-auth-routes");
const {registerEmbeddedAppHome} = require("./embedded-app-home");
const {registerShopifyProviderOAuthRoutes} = require("../routes/shopify-provider-oauth-routes");

function enabled(value) {
  if (value === undefined || value === "") return false;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error("SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED must be true or false");
}

function registerShopifyRuntime({app, env = process.env, supabaseAdmin, embeddedProviderOAuthAdapters}) {
  const config = loadShopifyConfig(env);
  if (!config.enabled) return Object.freeze({enabled: false});
  if (!supabaseAdmin) throw new Error("Shopify managed installation requires Supabase service-role configuration");

  const exchangeClient = createShopifyExchangeClient({clientId: config.clientId, clientSecret: config.clientSecret});
  const adminClient = createShopifyAdminClient();
  const tenantResolver = createShopifyTenantResolver({client: supabaseAdmin});
  const authConfig = {client_id: config.clientId, client_secret: config.clientSecret};
  registerEmbeddedAppHome(app, {clientId: config.clientId});
  registerShopifyAuthRoutes(app, {
    bootstrap_managed_install: ({session_token}) => {
      const vault = createProviderTokenVaultFromEnv(env);
      const installService = createShopifyInstallService({client: supabaseAdmin, vault});
      return bootstrapManagedInstallation({
        session_token,
        auth_config: authConfig,
        exchange_client: exchangeClient,
        admin_client: adminClient,
        install_service: installService,
      });
    },
    authenticate_embedded: ({session_token}) => authenticateEmbeddedRequest({
      session_token,
      config: authConfig,
      tenant_resolver: tenantResolver,
    }),
  });
  const providerOAuthEnabled = enabled(env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED);
  if (providerOAuthEnabled) registerShopifyProviderOAuthRoutes(app, {adapters: embeddedProviderOAuthAdapters});
  return Object.freeze({enabled: true, providerOAuthEnabled});
}

module.exports = Object.freeze({registerShopifyRuntime, enabled});

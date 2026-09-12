"use strict";

const {loadShopifyConfig} = require("../config/shopify-config");
const {createProviderTokenVaultFromEnv} = require("../../security/provider-token-vault");
const {createShopifyExchangeClient, createShopifyAdminClient} = require("./admin-api-client");
const {bootstrapManagedInstallation} = require("./managed-installation");
const {authenticateEmbeddedRequest} = require("./embedded-auth");
const {createShopifyInstallService, createShopifyTenantResolver} = require("./install-service");
const {registerShopifyAuthRoutes} = require("../routes/shopify-auth-routes");
const {registerEmbeddedAppHome} = require("./embedded-app-home");
const {registerEmbeddedPlatforms} = require("./embedded-app-home");
const {registerShopifyProviderOAuthRoutes} = require("../routes/shopify-provider-oauth-routes");
const {createEmbeddedProviderStrategies} = require("./embedded-provider-strategies");
const {createEmbeddedProviderOAuthAdapters} = require("./embedded-provider-oauth-adapters");
const {createEmbeddedProviderTokenExchanges} = require("./embedded-provider-token-exchange");
const {createWorkspaceProviderConnectionStore} = require("./workspace-provider-connection-store");

function enabled(value, env = process.env) {
  if (value === undefined || value === "") {
    return Boolean(env.PROVIDER_TOKEN_ACTIVE_KEY_ID && env.PROVIDER_TOKEN_ENCRYPTION_KEYS);
  }
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error("SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED must be true or false");
}

function registerShopifyRuntime({app, env = process.env, supabaseAdmin, oauthTransactionStore, fetchImpl = fetch, embeddedProviderOAuthAdapters}) {
  const config = loadShopifyConfig(env);
  if (!config.enabled) return Object.freeze({enabled: false});
  if (!supabaseAdmin) throw new Error("Shopify managed installation requires Supabase service-role configuration");

  const exchangeClient = createShopifyExchangeClient({clientId: config.clientId, clientSecret: config.clientSecret});
  const adminClient = createShopifyAdminClient();
  const tenantResolver = createShopifyTenantResolver({client: supabaseAdmin});
  const authConfig = {client_id: config.clientId, client_secret: config.clientSecret};
  const providerOAuthEnabled = enabled(env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED, env);
  registerEmbeddedAppHome(app, {clientId: config.clientId});
  registerEmbeddedPlatforms(app, {clientId: config.clientId, providerOAuthEnabled});
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
  if (providerOAuthEnabled) {
    if (!oauthTransactionStore) throw new Error("Embedded provider OAuth requires transaction storage");
    const vault = createProviderTokenVaultFromEnv(env);
    const adapters = embeddedProviderOAuthAdapters || createEmbeddedProviderOAuthAdapters({
      authenticateEmbedded: ({session_token}) => authenticateEmbeddedRequest({session_token, config: authConfig, tenant_resolver: tenantResolver}),
      createEmbeddedTransaction: (authority, provider, redirectUri, pkceVerifier) => oauthTransactionStore.createEmbedded({authority, provider, redirectUri, pkceVerifier, surface: "shopify_embedded", returnTarget: "/shopify/app/platforms"}),
      consumeTransaction: (state, provider, redirectUri) => oauthTransactionStore.consume({state, provider, redirectUri}),
      connectionStore: createWorkspaceProviderConnectionStore({client: supabaseAdmin, vault}),
      providerStrategies: createEmbeddedProviderStrategies({env, appUrl: config.appUrl, exchangeCodeByProvider: createEmbeddedProviderTokenExchanges({fetchImpl})}),
    });
    registerShopifyProviderOAuthRoutes(app, {adapters});
  }
  return Object.freeze({enabled: true, providerOAuthEnabled});
}

module.exports = Object.freeze({registerShopifyRuntime, enabled});

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
const {createEmbeddedProviderStrategies, configuredOAuthProviders} = require("./embedded-provider-strategies");
const {createEmbeddedProviderOAuthAdapters} = require("./embedded-provider-oauth-adapters");
const {createEmbeddedProviderTokenExchanges} = require("./embedded-provider-token-exchange");
const {createCanonicalWorkspaceProviderConnectionStore} = require("../providers/workspace-provider-connection-store");
const {createWorkspaceSettingsStore} = require("../providers/workspace-settings-store");
const {createKlaviyoAccountSelection} = require("./klaviyo-account-selection");
const {createKlaviyoDisconnect} = require("./klaviyo-disconnect");
const {createMetaDisconnect} = require("./meta-disconnect");
const {registerShopifyKlaviyoAccountRoutes} = require("../routes/shopify-klaviyo-account-routes");
const {registerShopifyWorkspaceSettingsRoutes} = require("../routes/shopify-workspace-settings-routes");
const {registerShopifyAdAccountRoutes} = require("../routes/shopify-ad-account-routes");
const {createAdAccountSelection, createMetaAccountDiscovery, createGoogleAdsAccountDiscovery} = require("./ad-account-selection");
const {createEmbeddedOAuthReturn} = require("./embedded-oauth-return");
const {createKlaviyoProviderClient} = require("../providers/klaviyo/provider-client");
const {createKlaviyoReadOnlyPreflight} = require("../providers/klaviyo/read-only-preflight");
const {createKlaviyoMetricBinding} = require("../providers/klaviyo/metric-binding");
const {createKlaviyoControlledDatasetAcceptance} = require("../providers/klaviyo/controlled-dataset-acceptance");
const {createKlaviyoTokenLifecycle} = require("../providers/klaviyo/token-lifecycle");
const {createMetaReadOnlyPreflight} = require("../providers/meta/read-only-preflight");
const {createMetaControlledDatasetAcceptance} = require("../providers/meta/controlled-dataset-acceptance");
const {createGoogleAdsTokenLifecycle} = require("../providers/google/token-lifecycle");
const {createGoogleAdsSearchClient} = require("../providers/google/search-client");
const {createGoogleReadOnlyPreflight} = require("../providers/google/read-only-preflight");
const {createGoogleControlledDatasetAcceptance} = require("../providers/google/controlled-dataset-acceptance");
const {WorkspaceSupabaseDatasetRepository} = require("../../funnel-core/workspace-supabase-dataset-repository");

function enabled(value) {
  if (value === undefined || value === "") return false;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error("SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED must be true or false");
}

function providerRuntimeReady({env, oauthTransactionStore, embeddedProviderOAuthAdapters}) {
  if (embeddedProviderOAuthAdapters) return true;
  if (!oauthTransactionStore) return false;
  try {
    createProviderTokenVaultFromEnv(env);
  } catch {
    return false;
  }
  return true;
}

function unavailableAccountDiscovery() {
  const error = new Error("PROVIDER_ACCOUNTS_UNAVAILABLE");
  error.code = "PROVIDER_ACCOUNTS_UNAVAILABLE";
  error.status = 503;
  throw error;
}

function registerShopifyRuntime({app, env = process.env, supabaseAdmin, oauthTransactionStore, fetchImpl = fetch, embeddedProviderOAuthAdapters, resolveFxRate}) {
  const config = loadShopifyConfig(env);
  if (!config.enabled) return Object.freeze({enabled: false});
  if (!supabaseAdmin) throw new Error("Shopify managed installation requires Supabase service-role configuration");

  const exchangeClient = createShopifyExchangeClient({clientId: config.clientId, clientSecret: config.clientSecret});
  const adminClient = createShopifyAdminClient();
  const tenantResolver = createShopifyTenantResolver({client: supabaseAdmin});
  const authConfig = {client_id: config.clientId, client_secret: config.clientSecret};
  const providerOAuthRequested = enabled(env.SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED);
  const runtimeReady = providerRuntimeReady({env, oauthTransactionStore, embeddedProviderOAuthAdapters});
  const oauthProviders = embeddedProviderOAuthAdapters ? Object.keys(embeddedProviderOAuthAdapters) : configuredOAuthProviders(env);
  const providerOAuthEnabled = providerOAuthRequested && runtimeReady && oauthProviders.length > 0;
  const googleDeveloperToken = String(env.GOOGLE_ADS_DEVELOPER_TOKEN || env.GOOGLE_DEVELOPER_TOKEN || "").trim();
  const providerAvailability = Object.freeze({
    meta: providerOAuthEnabled && oauthProviders.includes("meta"),
    google_ads: providerOAuthEnabled && oauthProviders.includes("google_ads") && Boolean(googleDeveloperToken),
    klaviyo: providerOAuthEnabled && oauthProviders.includes("klaviyo"),
  });
  const authenticateEmbedded = ({session_token}) => authenticateEmbeddedRequest({session_token, config: authConfig, tenant_resolver: tenantResolver});
  const serverWorkspaceAuthority = verified => Object.freeze({
    authority: "server_resolved_workspace",
    workspace_id: verified.workspace_id,
    source: "shopify_verified_session",
  });
  const settingsStore = createWorkspaceSettingsStore({client: supabaseAdmin});
  registerEmbeddedAppHome(app, {clientId: config.clientId});
  registerEmbeddedPlatforms(app, {clientId: config.clientId, providerOAuthEnabled, providerAvailability});
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
  registerShopifyWorkspaceSettingsRoutes(app, {
    authenticateEmbedded: async input => serverWorkspaceAuthority(await authenticateEmbedded(input)),
    settings: settingsStore,
  });
  if (providerOAuthEnabled) {
    if (!oauthTransactionStore) throw new Error("Embedded provider OAuth requires transaction storage");
    const vault = createProviderTokenVaultFromEnv(env);
    const connectionStore = createCanonicalWorkspaceProviderConnectionStore({client: supabaseAdmin, vault});
    const adapters = embeddedProviderOAuthAdapters || createEmbeddedProviderOAuthAdapters({
      authenticateEmbedded: async input => {
        const verified = await authenticateEmbedded(input);
        await settingsStore.resolveReportingCurrency(serverWorkspaceAuthority(verified));
        return verified;
      },
      createEmbeddedTransaction: (authority, provider, redirectUri, pkceVerifier) => oauthTransactionStore.createEmbedded({authority, provider, redirectUri, pkceVerifier, surface: "shopify_embedded", returnTarget: "/shopify/app/platforms"}),
      consumeTransaction: (state, provider, redirectUri) => oauthTransactionStore.consume({state, provider, redirectUri}),
      connectionStore,
      resolveReturnTarget: createEmbeddedOAuthReturn({client: supabaseAdmin, clientId: config.clientId}),
      providerStrategies: createEmbeddedProviderStrategies({
        env,
        appUrl: config.appUrl,
        exchangeCodeByProvider: createEmbeddedProviderTokenExchanges({fetchImpl, metaGraphVersion: env.META_GRAPH_VERSION || "v20.0"}),
        providers: oauthProviders,
      }),
      providers: oauthProviders,
    });
    registerShopifyProviderOAuthRoutes(app, {adapters});
    if (adapters.klaviyo) {
      const providerClient = createKlaviyoProviderClient({fetchImpl});
      const tokenLifecycle = createKlaviyoTokenLifecycle({
        connectionStore,
        fetchImpl,
        clientId: env.KLAVIYO_CLIENT_ID,
        clientSecret: env.KLAVIYO_CLIENT_SECRET,
      });
      const preflight = typeof resolveFxRate === "function"
        ? createKlaviyoReadOnlyPreflight({
          connectionStore,
          settingsStore,
          providerClient,
          tokenLifecycle,
          resolveFxRate,
        })
        : {execute: async () => {throw Object.assign(new Error("KLAVIYO_PREFLIGHT_NOT_CONFIGURED"), {code: "KLAVIYO_PREFLIGHT_NOT_CONFIGURED", status: 503});}};
      const datasetAcceptance = typeof resolveFxRate === "function"
        ? createKlaviyoControlledDatasetAcceptance({
          connectionStore,
          settingsStore,
          providerClient,
          tokenLifecycle,
          resolveFxRate,
          repository: new WorkspaceSupabaseDatasetRepository(supabaseAdmin),
        })
        : null;
      registerShopifyKlaviyoAccountRoutes(app, {
      authenticateEmbedded: async input => serverWorkspaceAuthority(await authenticateEmbedded(input)),
      selection: createKlaviyoAccountSelection({
        store: connectionStore, fetchImpl, clientId: env.KLAVIYO_CLIENT_ID, clientSecret: env.KLAVIYO_CLIENT_SECRET,
      }),
      disconnect: createKlaviyoDisconnect({
        store: connectionStore, fetchImpl, clientId: env.KLAVIYO_CLIENT_ID, clientSecret: env.KLAVIYO_CLIENT_SECRET,
      }),
      preflight,
      datasetAcceptance,
      metricBinding: createKlaviyoMetricBinding({connectionStore, providerClient, tokenLifecycle}),
      });
    }
    if (adapters.meta || adapters.google_ads) {
      const googleTokenLifecycle = adapters.google_ads ? createGoogleAdsTokenLifecycle({
        connectionStore,
        fetchImpl,
        clientId: env.GOOGLE_CLIENT_ID,
        clientSecret: env.GOOGLE_CLIENT_SECRET,
      }) : null;
      registerShopifyAdAccountRoutes(app, {
      authenticateEmbedded: async input => serverWorkspaceAuthority(await authenticateEmbedded(input)),
      selection: createAdAccountSelection({
        store: connectionStore,
        discoverByProvider: {
          meta: adapters.meta ? createMetaAccountDiscovery({fetchImpl, graphVersion: env.META_GRAPH_VERSION || "v20.0"}) : unavailableAccountDiscovery,
          google_ads: adapters.google_ads && providerAvailability.google_ads ? createGoogleAdsAccountDiscovery({
            fetchImpl,
            developerToken: googleDeveloperToken,
            apiVersion: env.GOOGLE_ADS_API_VERSION || "v25",
          }) : unavailableAccountDiscovery,
        },
        tokenLifecycleByProvider: googleTokenLifecycle ? {google_ads: googleTokenLifecycle} : {},
      }),
      metaPreflight: adapters.meta && typeof resolveFxRate === "function"
        ? createMetaReadOnlyPreflight({
          connectionStore,
          settingsStore,
          transport: fetchImpl,
          graphVersion: env.META_GRAPH_VERSION || "v20.0",
          resolveFxRate,
        })
        : adapters.meta
          ? {execute: async () => {throw Object.assign(new Error("META_PREFLIGHT_NOT_CONFIGURED"), {code: "META_PREFLIGHT_NOT_CONFIGURED", status: 503});}}
          : null,
      metaDatasetAcceptance: adapters.meta && typeof resolveFxRate === "function"
        ? createMetaControlledDatasetAcceptance({
          connectionStore,
          settingsStore,
          transport: fetchImpl,
          graphVersion: env.META_GRAPH_VERSION || "v20.0",
          resolveFxRate,
          repository: new WorkspaceSupabaseDatasetRepository(supabaseAdmin),
        })
        : null,
      metaDisconnect: adapters.meta
        ? createMetaDisconnect({
          store: connectionStore,
          fetchImpl,
          graphVersion: env.META_GRAPH_VERSION || "v20.0",
        })
        : null,
      googlePreflight: adapters.google_ads && providerAvailability.google_ads && typeof resolveFxRate === "function"
        ? createGoogleReadOnlyPreflight({
          connectionStore,
          settingsStore,
          tokenLifecycle: googleTokenLifecycle,
          search: createGoogleAdsSearchClient({
            fetchImpl,
            developerToken: googleDeveloperToken,
            apiVersion: env.GOOGLE_ADS_API_VERSION || "v25",
          }),
          resolveFxRate,
        })
        : adapters.google_ads
          ? {execute: async () => {throw Object.assign(new Error("GOOGLE_PREFLIGHT_NOT_CONFIGURED"), {code: "GOOGLE_PREFLIGHT_NOT_CONFIGURED", status: 503});}}
          : null,
      googleDatasetAcceptance: adapters.google_ads && providerAvailability.google_ads && typeof resolveFxRate === "function"
        ? createGoogleControlledDatasetAcceptance({
          connectionStore,
          settingsStore,
          tokenLifecycle: googleTokenLifecycle,
          search: createGoogleAdsSearchClient({
            fetchImpl,
            developerToken: googleDeveloperToken,
            apiVersion: env.GOOGLE_ADS_API_VERSION || "v25",
          }),
          resolveFxRate,
          repository: new WorkspaceSupabaseDatasetRepository(supabaseAdmin),
        })
        : null,
      });
    }
  }
  return Object.freeze({enabled: true, providerOAuthEnabled, providerOAuthRequested, providerAvailability});
}

module.exports = Object.freeze({registerShopifyRuntime, enabled, providerRuntimeReady});


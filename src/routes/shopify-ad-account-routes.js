'use strict';

const {bearerToken} = require('./shopify-auth-routes');
const {PROVIDERS} = require('../shopify/ad-account-selection');
const SAFE = new Set(['INVALID_ACCOUNT','ACCOUNT_SELECTION_LIMIT','PROVIDER_REAUTHORIZE','PROVIDER_ACCOUNTS_UNAVAILABLE','CONNECTION_CHANGED','META_PREFLIGHT_NOT_CONFIGURED','META_PREFLIGHT_CONNECTION_REQUIRED','META_PREFLIGHT_CURRENCY_REQUIRED','META_PREFLIGHT_REAUTHORIZE','META_PREFLIGHT_FAILED','META_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED','META_DATASET_ACCEPTANCE_ALREADY_EXECUTED','META_DATASET_ACCEPTANCE_REAUTHORIZE','META_DISCONNECT_CONFIRMATION_REQUIRED','META_REAUTHORIZE','META_REVOKE_FAILED']);

function registerShopifyAdAccountRoutes(app, {authenticateEmbedded, selection, metaPreflight = null, metaDatasetAcceptance = null, metaDisconnect = null} = {}) {
  const handler = (provider, operation, action) => async (req, res) => {
    res.set('Cache-Control', 'no-store');
    let authority;
    try { authority = await authenticateEmbedded({session_token: bearerToken(req.get('authorization'))}); }
    catch { return res.status(401).json({code: 'SHOPIFY_SESSION_REQUIRED'}); }
    try { return res.status(200).json(await action(authority, provider, req.body)); }
    catch (error) {
      const known = SAFE.has(error.code) || /^META_DATASET_ACCEPTANCE_FAILED_(CONNECTION|CURRENCY|ACCOUNT_SELECTION|DATASET_GUARD|PROVIDER_ACCOUNT|PROVIDER_FACTS|FX_RESOLUTION|RESULT_VERIFICATION|DATASET_PERSISTENCE|PERSISTENCE_CARDINALITY)$/.test(error.code);
      if (provider === 'google_ads') console.error({
        event: 'shopify_google_ads_account_operation_failed',
        operation,
        code: known ? error.code : 'PROVIDER_ACCOUNTS_UNAVAILABLE',
        stage: error.providerStage || null,
        upstreamStatus: error.upstreamStatus ?? null,
        upstreamCode: error.upstreamCode || null,
        upstreamRequestId: error.upstreamRequestId || null,
      });
      return res.status(known ? error.status || 503 : 503).json({code: known ? error.code : 'PROVIDER_ACCOUNTS_UNAVAILABLE'});
    }
  };
  for (const provider of PROVIDERS) {
    app.get(`/api/shopify/providers/${provider}/accounts/status`, handler(provider, 'status', (authority, value) => selection.status(authority, value)));
    app.get(`/api/shopify/providers/${provider}/accounts`, handler(provider, 'list', (authority, value) => selection.list(authority, value)));
    app.post(`/api/shopify/providers/${provider}/accounts/select`, handler(provider, 'select', (authority, value, body) => selection.complete(authority, value, body)));
  }
  if (metaPreflight) app.post('/api/shopify/providers/meta/runtime/preflight', handler('meta', 'preflight', authority => metaPreflight.execute(authority)));
  if (metaDatasetAcceptance) app.post('/api/shopify/providers/meta/runtime/acceptance', handler('meta', 'acceptance', (authority, _provider, body) => metaDatasetAcceptance.execute(authority, body?.confirmation)));
  if (metaDisconnect) app.post('/api/shopify/providers/meta/accounts/disconnect', handler('meta', 'disconnect', (authority, _provider, body) => metaDisconnect.execute(authority, body?.confirmation)));
}

module.exports = Object.freeze({registerShopifyAdAccountRoutes});


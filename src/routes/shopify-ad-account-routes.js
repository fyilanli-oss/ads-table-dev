'use strict';

const {bearerToken} = require('./shopify-auth-routes');
const {PROVIDERS} = require('../shopify/ad-account-selection');
const SAFE = new Set(['INVALID_ACCOUNT','PROVIDER_REAUTHORIZE','PROVIDER_ACCOUNTS_UNAVAILABLE','CONNECTION_CHANGED']);

function registerShopifyAdAccountRoutes(app, {authenticateEmbedded, selection} = {}) {
  const handler = (provider, action) => async (req, res) => {
    res.set('Cache-Control', 'no-store');
    let authority;
    try { authority = await authenticateEmbedded({session_token: bearerToken(req.get('authorization'))}); }
    catch { return res.status(401).json({code: 'SHOPIFY_SESSION_REQUIRED'}); }
    try { return res.status(200).json(await action(authority, provider, req.body)); }
    catch (error) { return res.status(SAFE.has(error.code) ? error.status || 503 : 503).json({code: SAFE.has(error.code) ? error.code : 'PROVIDER_ACCOUNTS_UNAVAILABLE'}); }
  };
  for (const provider of PROVIDERS) {
    app.get(`/api/shopify/providers/${provider}/accounts/status`, handler(provider, (authority, value) => selection.status(authority, value)));
    app.get(`/api/shopify/providers/${provider}/accounts`, handler(provider, (authority, value) => selection.list(authority, value)));
    app.post(`/api/shopify/providers/${provider}/accounts/select`, handler(provider, (authority, value, body) => selection.complete(authority, value, body)));
  }
}

module.exports = Object.freeze({registerShopifyAdAccountRoutes});

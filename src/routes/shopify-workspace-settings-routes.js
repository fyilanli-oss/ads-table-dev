'use strict';

const { bearerToken } = require('./shopify-auth-routes');

const SAFE_ERRORS = new Map([
  ['REPORTING_CURRENCY_NOT_SUPPORTED', 400],
  ['REPORTING_CURRENCY_ALREADY_CONFIGURED', 409],
]);

function registerShopifyWorkspaceSettingsRoutes(app, { authenticateEmbedded, settings } = {}) {
  if (!app || typeof app.get !== 'function' || typeof app.post !== 'function') throw new TypeError('Express app is required');
  if (typeof authenticateEmbedded !== 'function') throw new TypeError('authenticateEmbedded is required');
  if (!settings || typeof settings.readStatus !== 'function' || typeof settings.selectReportingCurrency !== 'function') {
    throw new TypeError('settings store is required');
  }
  const authenticate = async (req, res) => {
    try {
      return await authenticateEmbedded({ session_token: bearerToken(req.get('authorization')) });
    } catch {
      res.status(401).json({ code: 'SHOPIFY_SESSION_REQUIRED' });
      return null;
    }
  };
  app.get('/api/shopify/workspace/settings', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    const authority = await authenticate(req, res);
    if (!authority) return;
    try { return res.status(200).json(await settings.readStatus(authority)); }
    catch { return res.status(503).json({ code: 'WORKSPACE_SETTINGS_UNAVAILABLE' }); }
  });
  app.post('/api/shopify/workspace/reporting-currency', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    const authority = await authenticate(req, res);
    if (!authority) return;
    try {
      return res.status(200).json(await settings.selectReportingCurrency({ authority, currency: req.body?.currency }));
    } catch (error) {
      const status = SAFE_ERRORS.get(error.message);
      return res.status(status || 503).json({ code: status ? error.message : 'WORKSPACE_SETTINGS_UNAVAILABLE' });
    }
  });
}

module.exports = Object.freeze({ registerShopifyWorkspaceSettingsRoutes });

"use strict";

function frozenRoute(provider) {
  return (_req, res) => res.status(409).json({
    ok: false,
    code: 'LEGACY_PROVIDER_RUNTIME_FROZEN',
    provider,
    message: 'This legacy provider connection route is frozen while AdsTable consolidates workspace connections.'
  });
}

function registerOAuthProviderRoutes({ app, provider, startHandler, callbackHandler, frozen = false } = {}) {
  if (!app || typeof app.get !== "function") throw new TypeError("Express application is required");
  if (!provider || !/^[a-z0-9-]+$/.test(provider)) throw new TypeError("provider must be canonical");
  if (typeof startHandler !== "function") throw new TypeError("startHandler must be a function");
  if (typeof callbackHandler !== "function") throw new TypeError("callbackHandler must be a function");
  const start = frozen ? frozenRoute(provider) : startHandler;
  const callback = frozen ? frozenRoute(provider) : callbackHandler;
  app.get(`/auth/${provider}`, start);
  app.get(`/auth/${provider}/callback`, callback);
  return app;
}

module.exports = { registerOAuthProviderRoutes };

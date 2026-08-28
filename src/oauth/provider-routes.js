"use strict";

function registerOAuthProviderRoutes({ app, provider, startHandler, callbackHandler } = {}) {
  if (!app || typeof app.get !== "function") throw new TypeError("Express application is required");
  if (!provider || !/^[a-z0-9-]+$/.test(provider)) throw new TypeError("provider must be canonical");
  if (typeof startHandler !== "function") throw new TypeError("startHandler must be a function");
  if (typeof callbackHandler !== "function") throw new TypeError("callbackHandler must be a function");
  app.get(`/auth/${provider}`, startHandler);
  app.get(`/auth/${provider}/callback`, callbackHandler);
  return app;
}

module.exports = { registerOAuthProviderRoutes };

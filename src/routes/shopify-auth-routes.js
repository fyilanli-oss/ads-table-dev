"use strict";

function requireFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} is required`);
  return value;
}

function bearerToken(header) {
  if (typeof header !== "string" || !/^Bearer [A-Za-z0-9._~-]+$/.test(header)) throw new Error("SHOPIFY_SESSION_REQUIRED");
  return header.slice(7);
}

function registerShopifyAuthRoutes(app, {verify_install_callback, consume_install_state, complete_install, authenticate_embedded}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  requireFunction(verify_install_callback, "verify_install_callback");
  requireFunction(consume_install_state, "consume_install_state");
  requireFunction(complete_install, "complete_install");
  requireFunction(authenticate_embedded, "authenticate_embedded");

  app.get("/auth/shopify/callback", async (req, res, next) => {
    try {
      const callback = verify_install_callback(req.query);
      const state = await consume_install_state(callback);
      await complete_install({callback, state});
      res.redirect(303, "/dashboard?shopify=installed");
    } catch (error) { next(error); }
  });

  app.get("/api/shopify/session", async (req, res, next) => {
    try {
      const context = await authenticate_embedded({session_token: bearerToken(req.get("authorization"))});
      res.status(200).json({status: "active", workspace_ready: Boolean(context.workspace_id), reauthorization_required: false});
    } catch (error) { next(error); }
  });
}

module.exports = Object.freeze({registerShopifyAuthRoutes, bearerToken});

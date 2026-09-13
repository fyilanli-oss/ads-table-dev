"use strict";

const path = require("node:path");

const PUBLIC_PAGE_ROUTES = Object.freeze([
  Object.freeze({ route: "/", file: "landing.html" }),
  Object.freeze({ route: "/dashboard-demo", file: "dashboard-demo.html" }),
  Object.freeze({ route: "/login", file: "login.html" }),
  Object.freeze({ route: "/signup", file: "signup.html" }),
  Object.freeze({ route: "/dashboard", file: "dashboard.html" }),
  Object.freeze({ route: "/demo", file: "dashboard-demo.html" }),
  Object.freeze({ route: "/privacy", file: "privacy.html" }),
  Object.freeze({ route: "/terms", file: "terms.html" }),
  Object.freeze({ route: "/data-deletion", file: "data-deletion.html" }),
]);

function registerPublicRoutes({
  app,
  publicDirectory,
  publicConfig = {},
  tiktokTestPageEnabled = false,
} = {}) {
  if (!app || typeof app.get !== "function") throw new TypeError("Express application is required");
  if (!path.isAbsolute(publicDirectory || "")) throw new TypeError("publicDirectory must be absolute");
  if (!publicConfig || typeof publicConfig !== "object" || Array.isArray(publicConfig)) {
    throw new TypeError("publicConfig must be an object");
  }

  const sendFile = (res, file) => res.sendFile(path.join(publicDirectory, file));
  for (const { route, file } of PUBLIC_PAGE_ROUTES) {
    app.get(route, (_req, res) => sendFile(res, file));
  }
  app.get("/tiktok-test", (_req, res) => (
    tiktokTestPageEnabled ? sendFile(res, "tiktok-test.html") : res.sendStatus(404)
  ));
  app.get("/api/public-config", (_req, res) => res.json(publicConfig));
  return app;
}

module.exports = { PUBLIC_PAGE_ROUTES, registerPublicRoutes };

"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { test } = require("node:test");
const { PUBLIC_PAGE_ROUTES, registerPublicRoutes } = require("../src/routes/public-routes");

function fakeApp() {
  const routes = [];
  return { routes, get(route, handler) { routes.push({ route, handler }); return this; } };
}
function response() {
  return {
    sentFile: null,
    statusCode: 200,
    body: null,
    sendFile(file) { this.sentFile = file; return this; },
    sendStatus(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
  };
}

test("registers each public route exactly once with thin static handlers", () => {
  const app = fakeApp();
  const publicDirectory = path.resolve("public");
  registerPublicRoutes({ app, publicDirectory, publicConfig: Object.freeze({ supabaseUrl: "public" }) });

  assert.equal(new Set(app.routes.map(({ route }) => route)).size, app.routes.length);
  assert.deepEqual(app.routes.slice(0, PUBLIC_PAGE_ROUTES.length).map(({ route }) => route), PUBLIC_PAGE_ROUTES.map(({ route }) => route));
  for (const { route, file } of PUBLIC_PAGE_ROUTES) {
    const res = response();
    app.routes.find((entry) => entry.route === route).handler({}, res);
    assert.equal(res.sentFile, path.join(publicDirectory, file));
  }
});

test("preserves public config and disabled TikTok response contracts", () => {
  const app = fakeApp();
  const publicConfig = Object.freeze({ supabaseUrl: "url", supabaseAnonKey: "key" });
  registerPublicRoutes({ app, publicDirectory: path.resolve("public"), publicConfig });

  const configResponse = response();
  app.routes.find(({ route }) => route === "/api/public-config").handler({}, configResponse);
  assert.equal(configResponse.body, publicConfig);

  const tiktokResponse = response();
  app.routes.find(({ route }) => route === "/tiktok-test").handler({}, tiktokResponse);
  assert.equal(tiktokResponse.statusCode, 404);
});

test("serves TikTok test file only when explicitly enabled", () => {
  const app = fakeApp();
  const publicDirectory = path.resolve("public");
  registerPublicRoutes({ app, publicDirectory, tiktokTestPageEnabled: true });
  const res = response();
  app.routes.find(({ route }) => route === "/tiktok-test").handler({}, res);
  assert.equal(res.sentFile, path.join(publicDirectory, "tiktok-test.html"));
});

test("fails closed for missing route-registration dependencies", () => {
  assert.throws(() => registerPublicRoutes(), /Express application is required/);
  assert.throws(() => registerPublicRoutes({ app: fakeApp(), publicDirectory: "relative" }), /publicDirectory must be absolute/);
  assert.throws(() => registerPublicRoutes({ app: fakeApp(), publicDirectory: path.resolve("public"), publicConfig: null }), /publicConfig must be an object/);
});

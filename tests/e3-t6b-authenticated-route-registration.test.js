"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const { registerAccountStatusRoutes } = require("../src/routes/account-status-routes");

function fakeApp() {
  const routes = [];
  return { routes, get(route, handler) { routes.push({ route, handler }); return this; } };
}
function response() {
  return { body: null, statusCode: 200, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
}

async function invoke(handler, { req = {}, res = response() } = {}) {
  let forwarded = null;
  await handler(req, res, (error) => { forwarded = error; });
  return { res, forwarded };
}

test("registers one thin authenticated account-status route", async () => {
  const app = fakeApp();
  const calls = [];
  registerAccountStatusRoutes({
    app,
    requireUser: async () => (calls.push("auth"), { id: "verified-user" }),
    getSubscription: async (userId) => (calls.push(["subscription", userId]), { status: "active" }),
    getLifecycleAccess: (status) => (calls.push(["access", status]), { status, dashboard: true, blocked: false }),
  });

  assert.equal(app.routes.length, 1);
  assert.equal(app.routes[0].route, "/api/account/status");
  const { res, forwarded } = await invoke(app.routes[0].handler);
  assert.equal(forwarded, null);
  assert.deepEqual(calls, ["auth", ["subscription", "verified-user"], ["access", "active"]]);
  assert.deepEqual(res.body, {
    status: "active",
    access: { status: "active", dashboard: true, blocked: false },
    deleted_at: null,
    hard_delete_at: null,
  });
});

test("stops before subscription access when authentication rejects", async () => {
  const app = fakeApp();
  registerAccountStatusRoutes({
    app,
    requireUser: async (_req, res) => (res.status(401).json({ error: "Not authenticated" }), null),
    getSubscription: async () => assert.fail("subscription must not be read"),
    getLifecycleAccess: () => assert.fail("access must not be calculated"),
  });
  const { res } = await invoke(app.routes[0].handler);
  assert.equal(res.statusCode, 401);
  assert.deepEqual(res.body, { error: "Not authenticated" });
});

test("forwards failures to the canonical error boundary", async () => {
  const app = fakeApp();
  const failure = new Error("subscription unavailable");
  registerAccountStatusRoutes({
    app,
    requireUser: async () => ({ id: "verified-user" }),
    getSubscription: async () => { throw failure; },
    getLifecycleAccess: () => assert.fail("access must not be calculated"),
  });
  const { res, forwarded } = await invoke(app.routes[0].handler);
  assert.equal(forwarded, failure);
  assert.equal(res.body, null);
});

test("fails closed when registration dependencies are missing", () => {
  const valid = { app: fakeApp(), requireUser() {}, getSubscription() {}, getLifecycleAccess() {} };
  assert.throws(() => registerAccountStatusRoutes(), /Express application is required/);
  for (const dependency of ["requireUser", "getSubscription", "getLifecycleAccess"]) {
    assert.throws(() => registerAccountStatusRoutes({ ...valid, [dependency]: null }), new RegExp(`${dependency} must be a function`));
  }
});

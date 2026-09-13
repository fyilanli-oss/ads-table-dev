"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createAccessBoundary } = require("../src/middleware/access-boundary");

function response() {
  return { statusCode: 200, body: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
}
function boundary(overrides = {}) {
  return createAccessBoundary({
    getUserFromRequest: async () => ({ id: "user-1" }),
    getUserSubscription: async () => ({ status: "active" }),
    getSubscriptionForLifecycle: async () => ({ status: "active" }),
    getAccessByStatus: () => ({ manualRefresh: true, blocked: false }),
    getLifecycleAccess: (status) => ({ status, dashboard: true, blocked: false }),
    getConnection: async () => ({ id: "connection-1" }),
    getOwnership: async () => ({ owner_user_id: "user-1", status: "active" }),
    activeOwnershipStatuses: () => ["connected", "active"],
    ...overrides,
  });
}

test("uses verified request identity and preserves unauthenticated contract", async () => {
  const access = boundary({ getUserFromRequest: async () => null });
  const res = response();
  assert.equal(await access.requireUser({}, res), null);
  assert.equal(res.statusCode, 401);
  assert.deepEqual(res.body, { error: "Not authenticated" });
});

test("enforces lifecycle capability before returning user context", async () => {
  const denied = boundary({ getLifecycleAccess: (status) => ({ status, dashboard: false, blocked: false }) });
  const deniedRes = response();
  assert.equal(await denied.requireLifecycleAccess({}, deniedRes, "dashboard"), null);
  assert.deepEqual(deniedRes.body, { error: "Account access blocked", status: "active", capability: "dashboard" });

  const allowed = await boundary().requireLifecycleAccess({}, response(), "dashboard");
  assert.equal(allowed.user.id, "user-1");
  assert.equal(allowed.access.dashboard, true);
});

test("requires account access and a connected provider", async () => {
  const blocked = boundary({ getLifecycleAccess: () => ({ status: "suspended", blocked: true }) });
  const blockedRes = response();
  assert.equal(await blocked.requireConnection({}, blockedRes, "meta"), null);
  assert.equal(blockedRes.statusCode, 403);

  const missing = boundary({ getConnection: async () => null });
  const missingRes = response();
  assert.equal(await missing.requireConnection({}, missingRes, "meta"), null);
  assert.deepEqual(missingRes.body, { error: "meta not connected" });
});

test("binds active ownership to the authenticated server-side user id", async () => {
  const access = boundary();
  assert.equal((await access.requireActiveOwnership("user-1", "meta", "account-1")).status, "active");
  await assert.rejects(() => access.requireActiveOwnership("attacker", "meta", "account-1"), (error) => error.status === 403);
});

test("fails closed when a canonical dependency is missing", () => {
  assert.throws(() => boundary({ getOwnership: null }), /getOwnership must be a function/);
});

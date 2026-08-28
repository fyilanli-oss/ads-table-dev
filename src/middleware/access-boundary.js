"use strict";

function requireFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} must be a function`);
}

function createAccessBoundary({
  getUserFromRequest,
  getUserSubscription,
  getSubscriptionForLifecycle,
  getAccessByStatus,
  getLifecycleAccess,
  getConnection,
  getOwnership,
  activeOwnershipStatuses,
} = {}) {
  for (const [name, dependency] of Object.entries({
    getUserFromRequest,
    getUserSubscription,
    getSubscriptionForLifecycle,
    getAccessByStatus,
    getLifecycleAccess,
    getConnection,
    getOwnership,
    activeOwnershipStatuses,
  })) requireFunction(dependency, name);

  async function requireUser(req, res) {
    const user = await getUserFromRequest(req);
    if (!user) {
      res.status(401).json({ error: "Not authenticated" });
      return null;
    }
    return user;
  }

  async function requireAccess(req, res, userId, capability) {
    const sub = await getUserSubscription(userId);
    const access = getAccessByStatus(sub?.status);
    if (access.blocked || !access[capability]) {
      res.status(403).json({ error: "Subscription inactive", status: sub?.status || null });
      return null;
    }
    return { sub, access };
  }

  async function requireLifecycleAccess(req, res, capability) {
    const user = await requireUser(req, res);
    if (!user) return null;
    const sub = await getSubscriptionForLifecycle(user.id);
    const access = getLifecycleAccess(sub?.status);
    if (access.blocked || !access[capability]) {
      res.status(403).json({ error: "Account access blocked", status: access.status, capability });
      return null;
    }
    return { user, sub, access };
  }

  async function requireConnection(req, res, platform) {
    const user = await requireUser(req, res);
    if (!user) return null;
    const sub = await getSubscriptionForLifecycle(user.id);
    const access = getLifecycleAccess(sub?.status);
    if (access.blocked) {
      res.status(403).json({ error: "Account access blocked", status: access.status });
      return null;
    }
    const conn = await getConnection(user.id, platform);
    if (!conn) {
      res.status(404).json({ error: `${platform} not connected` });
      return null;
    }
    return { user, conn };
  }

  async function requireRefreshConnection(req, res, platform) {
    const user = await requireUser(req, res);
    if (!user) return null;
    const accessCheck = await requireAccess(req, res, user.id, "manualRefresh");
    if (!accessCheck) return null;
    const conn = await getConnection(user.id, platform);
    if (!conn) {
      res.status(404).json({ error: `${platform} not connected` });
      return null;
    }
    return { user, conn, sub: accessCheck.sub, access: accessCheck.access };
  }

  async function requireActiveOwnership(userId, platform, platformAccountId) {
    const ownership = await getOwnership(platform, platformAccountId);
    if (!ownership || ownership.owner_user_id !== userId || !activeOwnershipStatuses().includes(ownership.status)) {
      const error = new Error("Platform account ownership is not active");
      error.status = 403;
      throw error;
    }
    return ownership;
  }

  return Object.freeze({
    requireUser,
    requireAccess,
    requireLifecycleAccess,
    requireConnection,
    requireRefreshConnection,
    requireActiveOwnership,
  });
}

module.exports = { createAccessBoundary };

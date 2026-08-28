"use strict";

function requireFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} must be a function`);
}

function registerAccountStatusRoutes({
  app,
  requireUser,
  getSubscription,
  getLifecycleAccess,
} = {}) {
  if (!app || typeof app.get !== "function") throw new TypeError("Express application is required");
  requireFunction(requireUser, "requireUser");
  requireFunction(getSubscription, "getSubscription");
  requireFunction(getLifecycleAccess, "getLifecycleAccess");

  app.get("/api/account/status", async (req, res, next) => {
    try {
      const user = await requireUser(req, res);
      if (!user) return;
      const subscription = await getSubscription(user.id);
      const access = getLifecycleAccess(subscription?.status);
      res.json({ status: access.status, access, deleted_at: null, hard_delete_at: null });
    } catch (error) {
      next(error);
    }
  });
  return app;
}

module.exports = { registerAccountStatusRoutes };

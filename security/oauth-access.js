'use strict';

function createRequireConnectAccessForOAuth({requireUser, getUserSubscription, getAccessByStatus}) {
  if (typeof requireUser !== 'function') throw new TypeError('requireUser is required');
  if (typeof getUserSubscription !== 'function') throw new TypeError('getUserSubscription is required');
  if (typeof getAccessByStatus !== 'function') throw new TypeError('getAccessByStatus is required');

  return async function requireConnectAccessForOAuth(req, res) {
    const user = await requireUser(req, res);
    if (!user) return null;

    if (Object.prototype.hasOwnProperty.call(req.query || {}, 'user_id')) {
      res.status(400).json({error: 'OAuth user_id query parameter is not accepted'});
      return null;
    }

    const sub = await getUserSubscription(user.id);
    const access = getAccessByStatus(sub?.status);
    if (access.blocked || !access.connect) {
      res.status(403).json({error: 'Subscription inactive', status: sub?.status || null});
      return null;
    }

    return {userId: user.id, sub, access};
  };
}

module.exports = Object.freeze({createRequireConnectAccessForOAuth});

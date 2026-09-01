'use strict';

function createTikTokSandboxConnectHandler({readiness, requireConnectAccess, saveConnection, accessToken, sandboxBase, fallback} = {}) {
  for (const [name, fn] of Object.entries({ readiness, requireConnectAccess, saveConnection, fallback })) if (typeof fn !== 'function') throw new TypeError(`${name} must be a function`);
  return async function handleTikTokConnect(req, res) {
    if (!readiness().ready) return fallback(req, res);
    const access = await requireConnectAccess(req, res);
    if (!access) return;
    await saveConnection(access.userId, 'tiktok', {
      accessToken, refreshToken: null, tokenExpiresAt: null, accountId: null, accountName: null,
      metadata: { selectedPlatformAccountId: null, selectedPlatformAccountIds: [], selectedPlatformAccounts: [], lastOwnedPlatformAccountId: null, accountSelectionRequired: true, reconnectSelectionRequired: true, sandbox: true, reportBase: sandboxBase, tokenSource: 'server_sandbox_access_token', accountSelectionGuardVersion: 'v2-explicit-selection' }
    });
    return res.redirect('/dashboard?tiktok_connected=1&account_selection_required=1');
  };
}

module.exports = Object.freeze({ createTikTokSandboxConnectHandler });

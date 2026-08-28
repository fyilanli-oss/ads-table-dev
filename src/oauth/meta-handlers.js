"use strict";

function createMetaOAuthHandlers({
  config,
  requireConnectAccess,
  createTransaction,
  consumeTransaction,
  sendAuthorizationResponse,
  exchangeToken,
  saveConnection,
  getConnection,
  discoverAccounts,
  upsertAdAccount,
  parseExpiry,
} = {}) {
  if (!config || typeof config !== "object") throw new TypeError("config is required");
  for (const [name, dependency] of Object.entries({ requireConnectAccess, createTransaction, consumeTransaction, sendAuthorizationResponse, exchangeToken, saveConnection, getConnection, discoverAccounts, upsertAdAccount, parseExpiry })) {
    if (typeof dependency !== "function") throw new TypeError(`${name} must be a function`);
  }

  async function start(req, res) {
    try {
      const access = await requireConnectAccess(req, res);
      if (!access) return;
      if (!config.appId || !config.redirectUri) throw new Error("Missing Meta env");
      const { state } = await createTransaction(access.userId, "meta", config.redirectUri);
      const params = new URLSearchParams({ client_id: config.appId, redirect_uri: config.redirectUri, state, response_type: "code", scope: "ads_read" });
      return sendAuthorizationResponse(req, res, `https://www.facebook.com/${config.graphVersion}/dialog/oauth?${params}`);
    } catch (error) {
      return res.status(500).send(error.message);
    }
  }

  async function callback(req, res) {
    try {
      const { code, state, error, error_description: description } = req.query;
      if (error) return res.redirect(`/dashboard?meta_error=${encodeURIComponent(description || error)}`);
      if (!code) return res.redirect("/dashboard?meta_error=missing_code");
      const transaction = await consumeTransaction(state, "meta", config.redirectUri);
      if (!transaction) return res.redirect("/dashboard?meta_error=invalid_state");
      const userId = transaction.user_id;
      const token = await exchangeToken({ code, appId: config.appId, appSecret: config.appSecret, redirectUri: config.redirectUri, graphVersion: config.graphVersion });
      await saveConnection(userId, "meta", { accessToken: token.access_token, tokenExpiresAt: parseExpiry(token.expires_in), accountId: null, accountName: null, metadata: { expiresIn: token.expires_in || null, selectedPlatformAccountId: null, selectedPlatformAccountIds: [], selectedPlatformAccounts: [], lastOwnedPlatformAccountId: null, accountSelectionRequired: true, reconnectSelectionRequired: true, accountSelectionGuardVersion: "v2-explicit-selection" } });
      if (await getConnection(userId, "meta")) {
        try {
          const accounts = await discoverAccounts(token.access_token);
          for (const account of accounts) await upsertAdAccount(userId, "meta", account);
          await saveConnection(userId, "meta", { metadata: { accountSelectionRequired: true, availableAccountCount: accounts.length, accountSelectionGuardVersion: "v1" } });
        } catch (discoveryError) {
          await saveConnection(userId, "meta", { metadata: { accountSelectionRequired: true, accountDiscoveryError: discoveryError.message, accountSelectionGuardVersion: "v1" } });
        }
      }
      return res.redirect("/dashboard?meta_connected=1&account_selection_required=1");
    } catch (error) {
      return res.redirect(`/dashboard?meta_error=${encodeURIComponent(error.message)}`);
    }
  }
  return Object.freeze({ start, callback });
}

module.exports = { createMetaOAuthHandlers };

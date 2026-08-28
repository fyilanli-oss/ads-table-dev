"use strict";

function createOAuthTransactionBoundary({ transactionStore } = {}) {
  async function createTransaction(userId, provider, redirectUri, pkceVerifier = null) {
    if (!transactionStore) throw new Error("OAuth transaction store is not configured");
    await transactionStore.cleanupExpired();
    return transactionStore.create({ userId, provider, redirectUri, pkceVerifier });
  }

  async function consumeTransaction(state, provider, redirectUri) {
    if (!transactionStore || !state) return null;
    return transactionStore.consume({ state: String(state), provider, redirectUri });
  }

  function sendAuthorizationResponse(req, res, authorizationUrl) {
    if (req.query.response_mode === "json") return res.json({ authorization_url: authorizationUrl });
    return res.redirect(authorizationUrl);
  }

  return Object.freeze({ createTransaction, consumeTransaction, sendAuthorizationResponse });
}

module.exports = { createOAuthTransactionBoundary };

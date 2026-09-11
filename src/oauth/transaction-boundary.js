"use strict";

const EMBEDDED_SURFACE = "shopify_embedded";
const EMBEDDED_RETURN_TARGET = "/shopify/app/platforms";

function createOAuthTransactionBoundary({ transactionStore } = {}) {
  async function createTransaction(userId, provider, redirectUri, pkceVerifier = null) {
    if (!transactionStore) throw new Error("OAuth transaction store is not configured");
    await transactionStore.cleanupExpired();
    return transactionStore.create({ userId, provider, redirectUri, pkceVerifier });
  }

  async function createEmbeddedTransaction(authority, provider, redirectUri, pkceVerifier = null) {
    if (!transactionStore) throw new Error("OAuth transaction store is not configured");
    if (!authority || authority.authority !== "shopify_verified_session") {
      throw new Error("VERIFIED_SHOPIFY_SESSION_REQUIRED");
    }
    await transactionStore.cleanupExpired();
    return transactionStore.createEmbedded({
      authority,
      provider,
      redirectUri,
      pkceVerifier,
      surface: EMBEDDED_SURFACE,
      returnTarget: EMBEDDED_RETURN_TARGET,
    });
  }

  async function consumeTransaction(state, provider, redirectUri) {
    if (!transactionStore || !state) return null;
    return transactionStore.consume({ state: String(state), provider, redirectUri });
  }

  function sendAuthorizationResponse(req, res, authorizationUrl) {
    if (req.query.response_mode === "json") return res.json({ authorization_url: authorizationUrl });
    return res.redirect(authorizationUrl);
  }

  return Object.freeze({ createTransaction, createEmbeddedTransaction, consumeTransaction, sendAuthorizationResponse });
}

module.exports = { createOAuthTransactionBoundary, EMBEDDED_SURFACE, EMBEDDED_RETURN_TARGET };

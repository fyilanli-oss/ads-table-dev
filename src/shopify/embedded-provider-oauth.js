"use strict";

const RETURN_TARGET = "/shopify/app/platforms";

function requiredFunction(value, name) {
  if (typeof value !== "function") throw new TypeError(`${name} is required`);
  return value;
}

function createEmbeddedProviderOAuth({provider, redirectUri, authenticateEmbedded, createEmbeddedTransaction, consumeTransaction, buildAuthorizationUrl, exchangeCode, connectionStore} = {}) {
  if (typeof provider !== "string" || !provider) throw new TypeError("provider is required");
  if (typeof redirectUri !== "string" || !redirectUri) throw new TypeError("redirectUri is required");
  for (const [name, value] of Object.entries({authenticateEmbedded, createEmbeddedTransaction, consumeTransaction, buildAuthorizationUrl, exchangeCode})) requiredFunction(value, name);
  if (!connectionStore || typeof connectionStore.writeFromOAuthTransaction !== "function") throw new TypeError("connectionStore is required");

  async function start({sessionToken}) {
    const authority = await authenticateEmbedded({session_token: sessionToken});
    const transaction = await createEmbeddedTransaction(authority, provider, redirectUri);
    const authorizationUrl = await buildAuthorizationUrl({state: transaction.state, redirectUri});
    return Object.freeze({authorization_url: authorizationUrl, navigation: "top_level"});
  }

  async function callback({state, code}) {
    if (typeof state !== "string" || !state || typeof code !== "string" || !code) throw new Error("INVALID_OAUTH_CALLBACK");
    const transaction = await consumeTransaction(state, provider, redirectUri);
    if (!transaction || transaction.surface !== "shopify_embedded" || transaction.provider !== provider || transaction.return_target !== RETURN_TARGET) {
      throw new Error("INVALID_EMBEDDED_OAUTH_TRANSACTION");
    }
    const tokens = await exchangeCode({code, redirectUri, pkceVerifier: transaction.pkce_verifier || null});
    if (!tokens || typeof tokens.accessToken !== "string" || !tokens.accessToken) throw new Error("INVALID_PROVIDER_TOKEN_RESPONSE");
    await connectionStore.writeFromOAuthTransaction({transaction, accessToken: tokens.accessToken, refreshToken: tokens.refreshToken || null});
    return Object.freeze({redirect_to: RETURN_TARGET, outcome: "account_selection_required"});
  }

  return Object.freeze({start, callback});
}
module.exports = Object.freeze({createEmbeddedProviderOAuth, RETURN_TARGET});

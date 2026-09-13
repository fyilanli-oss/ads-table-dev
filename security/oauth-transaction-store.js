'use strict';

const crypto = require('node:crypto');

const OAUTH_TRANSACTION_TTL_MS = 10 * 60 * 1000;

function stateDigest(state) {
  return crypto.createHash('sha256').update(String(state), 'utf8').digest('hex');
}

function createOAuthTransactionStore({client, now = () => new Date()} = {}) {
  if (!client) throw new TypeError('A service-role Supabase client is required');

  async function create({userId, provider, redirectUri, pkceVerifier = null}) {
    if (!userId || !provider || !redirectUri) throw new TypeError('userId, provider and redirectUri are required');
    const state = crypto.randomBytes(32).toString('base64url');
    const createdAt = now();
    const row = {
      state_hash: stateDigest(state),
      user_id: userId,
      provider,
      redirect_uri: redirectUri,
      pkce_verifier: pkceVerifier,
      expires_at: new Date(createdAt.getTime() + OAUTH_TRANSACTION_TTL_MS).toISOString()
    };
    const {error} = await client.from('oauth_transactions').insert(row);
    if (error) throw new Error(error.message);
    return {state, expiresAt: row.expires_at};
  }

  async function createEmbedded({authority, provider, redirectUri, pkceVerifier = null, surface, returnTarget}) {
    if (!authority || authority.authority !== 'shopify_verified_session') throw new TypeError('verified Shopify authority is required');
    for (const field of ['shop_id', 'workspace_id', 'shopify_user_id']) {
      if (typeof authority[field] !== 'string' || !authority[field]) throw new TypeError(`authority.${field} is required`);
    }
    if (surface !== 'shopify_embedded' || returnTarget !== '/shopify/app/platforms') throw new TypeError('canonical embedded surface and return target are required');
    if (!provider || !redirectUri) throw new TypeError('provider and redirectUri are required');
    const state = crypto.randomBytes(32).toString('base64url');
    const createdAt = now();
    const row = {
      state_hash: stateDigest(state), user_id: null, provider, redirect_uri: redirectUri,
      pkce_verifier: pkceVerifier, surface, return_target: returnTarget,
      shop_id: authority.shop_id, workspace_id: authority.workspace_id,
      shopify_user_id: authority.shopify_user_id,
      expires_at: new Date(createdAt.getTime() + OAUTH_TRANSACTION_TTL_MS).toISOString()
    };
    const {error} = await client.from('oauth_transactions').insert(row);
    if (error) throw new Error(error.message);
    return {state, expiresAt: row.expires_at};
  }

  async function consume({state, provider, redirectUri}) {
    if (!state || !provider || !redirectUri) return null;
    const {data, error} = await client.rpc('consume_oauth_transaction', {
      p_state_hash: stateDigest(state),
      p_provider: provider,
      p_redirect_uri: redirectUri
    });
    if (error) throw new Error(error.message);
    return Array.isArray(data) ? (data[0] || null) : (data || null);
  }

  async function cleanupExpired() {
    const {data, error} = await client.rpc('cleanup_expired_oauth_transactions');
    if (error) throw new Error(error.message);
    return Number(data || 0);
  }

  return Object.freeze({create, createEmbedded, consume, cleanupExpired});
}

module.exports = {createOAuthTransactionStore, stateDigest, OAUTH_TRANSACTION_TTL_MS};

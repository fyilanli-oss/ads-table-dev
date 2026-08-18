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

  return Object.freeze({create, consume, cleanupExpired});
}

module.exports = {createOAuthTransactionStore, stateDigest, OAUTH_TRANSACTION_TTL_MS};

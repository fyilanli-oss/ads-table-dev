'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {createOAuthTransactionStore, stateDigest, OAUTH_TRANSACTION_TTL_MS} = require('../security/oauth-transaction-store');

function memoryClient() {
  const rows = new Map();
  return {
    rows,
    from(table) {
      assert.equal(table, 'oauth_transactions');
      return {insert: async row => { rows.set(row.state_hash, {...row}); return {error: null}; }};
    },
    async rpc(name, args) {
      if (name === 'consume_oauth_transaction') {
        const row = rows.get(args.p_state_hash);
        if (!row || row.provider !== args.p_provider || row.redirect_uri !== args.p_redirect_uri || row.expires_at <= args.p_now) return {data: [], error: null};
        rows.delete(args.p_state_hash);
        return {data: [{user_id: row.user_id, provider: row.provider, redirect_uri: row.redirect_uri, pkce_verifier: row.pkce_verifier}], error: null};
      }
      if (name === 'cleanup_expired_oauth_transactions') {
        let count = 0;
        for (const [key, row] of rows) if (row.expires_at <= args.p_now) { rows.delete(key); count++; }
        return {data: count, error: null};
      }
      throw new Error(`unexpected RPC ${name}`);
    }
  };
}

test('stores only a SHA-256 digest of a cryptographically random 32-byte state', async () => {
  const client = memoryClient();
  const store = createOAuthTransactionStore({client});
  const {state} = await store.create({userId: 'user-1', provider: 'meta', redirectUri: 'https://app/callback'});
  assert.ok(Buffer.from(state, 'base64url').length >= 32);
  const [row] = client.rows.values();
  assert.equal(row.state_hash, stateDigest(state));
  assert.equal(row.state_hash.length, 64);
  assert.equal(JSON.stringify(row).includes(state), false);
});

test('transaction is bound to a ten-minute TTL and expires', async () => {
  let clock = new Date('2026-08-18T09:00:00.000Z');
  const client = memoryClient();
  const store = createOAuthTransactionStore({client, now: () => clock});
  const created = await store.create({userId: 'user-1', provider: 'meta', redirectUri: 'https://app/callback'});
  assert.equal(new Date(created.expiresAt).getTime() - clock.getTime(), OAUTH_TRANSACTION_TTL_MS);
  clock = new Date(clock.getTime() + OAUTH_TRANSACTION_TTL_MS);
  assert.equal(await store.consume({state: created.state, provider: 'meta', redirectUri: 'https://app/callback'}), null);
});

test('atomic consume permits one use and rejects replay', async () => {
  const store = createOAuthTransactionStore({client: memoryClient()});
  const created = await store.create({userId: 'user-1', provider: 'meta', redirectUri: 'https://app/callback'});
  assert.equal((await store.consume({state: created.state, provider: 'meta', redirectUri: 'https://app/callback'})).user_id, 'user-1');
  assert.equal(await store.consume({state: created.state, provider: 'meta', redirectUri: 'https://app/callback'}), null);
});

test('provider and exact redirect URI mismatches are rejected without consuming', async () => {
  const store = createOAuthTransactionStore({client: memoryClient()});
  const created = await store.create({userId: 'user-1', provider: 'meta', redirectUri: 'https://app/callback'});
  assert.equal(await store.consume({state: created.state, provider: 'tiktok', redirectUri: 'https://app/callback'}), null);
  assert.equal(await store.consume({state: created.state, provider: 'meta', redirectUri: 'https://app/callback/'}), null);
  assert.ok(await store.consume({state: created.state, provider: 'meta', redirectUri: 'https://app/callback'}));
});

test('Klaviyo PKCE verifier round-trips in the same transaction', async () => {
  const store = createOAuthTransactionStore({client: memoryClient()});
  const created = await store.create({userId: 'user-1', provider: 'klaviyo', redirectUri: 'https://app/klaviyo', pkceVerifier: 'secret-verifier'});
  const consumed = await store.consume({state: created.state, provider: 'klaviyo', redirectUri: 'https://app/klaviyo'});
  assert.equal(consumed.pkce_verifier, 'secret-verifier');
});

test('cleanup deletes expired abandoned transactions only', async () => {
  let clock = new Date('2026-08-18T09:00:00.000Z');
  const client = memoryClient();
  const store = createOAuthTransactionStore({client, now: () => clock});
  await store.create({userId: 'old', provider: 'meta', redirectUri: 'https://app/callback'});
  clock = new Date(clock.getTime() + OAUTH_TRANSACTION_TTL_MS);
  await store.create({userId: 'new', provider: 'meta', redirectUri: 'https://app/callback'});
  assert.equal(await store.cleanupExpired(), 1);
  assert.equal(client.rows.size, 1);
});

test('migration enforces service-role-only RLS and atomic DELETE RETURNING RPC', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'supabase', 'migrations', '20260818090000_create_oauth_transactions.sql'), 'utf8');
  assert.match(sql, /enable row level security/i);
  assert.match(sql, /revoke all on table public\.oauth_transactions from anon, authenticated/i);
  assert.match(sql, /grant select, insert, delete on table public\.oauth_transactions to service_role/i);
  assert.match(sql, /delete from public\.oauth_transactions[\s\S]*returning oauth_transactions\.user_id/i);
  assert.match(sql, /revoke all on function public\.consume_oauth_transaction[\s\S]*from public, anon, authenticated/i);
  assert.match(sql, /grant execute on function public\.consume_oauth_transaction[\s\S]*to service_role/i);
});

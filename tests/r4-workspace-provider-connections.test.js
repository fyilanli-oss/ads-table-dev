'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  TABLE,
  ACTIVE_PROVIDERS,
  createCanonicalWorkspaceProviderConnectionStore
} = require('../src/providers/workspace-provider-connection-store');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const contract = JSON.parse(read('contracts/r4-workspace-provider-connections-v1.json'));
const migration = read(contract.migration);
const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session'
});

function queryResult(result, calls) {
  const query = {
    insert(row) { calls.push(['insert', row]); return query; },
    select(columns) { calls.push(['select', columns]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    async maybeSingle() { return result; }
  };
  return query;
}

function fixture(result = { data: null, error: null }) {
  const calls = [];
  const client = {
    from(table) {
      calls.push(['from', table]);
      return queryResult(result, calls);
    }
  };
  const vault = {
    encrypt(value, context) { return value ? { version: 1, keyId: 'k', iv: 'i', tag: 't', ciphertext: `enc:${value}`, context } : null; },
    decrypt(envelope) { return envelope ? envelope.ciphertext.replace(/^enc:/, '') : null; }
  };
  return { calls, store: createCanonicalWorkspaceProviderConnectionStore({ client, vault, now: () => new Date('2026-09-23T09:00:00.000Z') }) };
}

test('R4 contract makes AdsTable workspace the owner, not Shopify', () => {
  assert.equal(contract.status, 'R4_COMPLETE_R5_CONSOLIDATION_APPROVAL_PENDING');
  assert.equal(contract.production_gate.live_migration_version, '20260923091731');
  assert.equal(contract.production_gate.result, 'PASS');
  assert.equal(contract.r4c_write_freeze.live_migration_version, '20260923093756');
  assert.equal(contract.r4c_write_freeze.result, 'PASS');
  assert.equal(contract.canonical_owner, 'workspace_id');
  assert.deepEqual(contract.primary_key, ['workspace_id', 'provider']);
  assert.equal(contract.commerce_adapter_role.owns_provider_connection, false);
  assert.equal(contract.commerce_adapter_role.required_in_primary_key, false);
  assert.equal(contract.forbidden_until_later_gate.includes('copy_existing_connections'), true);
  assert.equal(fs.existsSync(path.join(root, contract.production_gate.evidence)), true);
  assert.equal(fs.existsSync(path.join(root, contract.r4c_write_freeze.evidence)), true);
});

test('migration creates one server-only canonical authority without Shopify identity', () => {
  assert.match(migration, /create table public\.workspace_provider_connections/);
  assert.match(migration, /primary key \(workspace_id, provider\)/);
  assert.match(migration, /references public\.workspaces\(id\)/);
  assert.doesNotMatch(migration, /shop_id|shopify_installations/);
  assert.match(migration, /provider in \('meta', 'google_ads', 'klaviyo'\)/);
  assert.doesNotMatch(migration, /'tiktok'|'pinterest'/);
  assert.match(migration, /force row level security/);
  assert.match(migration, /revoke all on table public\.workspace_provider_connections from public, anon, authenticated/);
  assert.match(migration, /grant select, insert, update, delete on table public\.workspace_provider_connections to service_role/);
  assert.doesNotMatch(migration, /insert into|select .*shopify_workspace_provider_connections/is);
});

test('OAuth completion begins as pending and writes only canonical workspace authority', async () => {
  const result = { data: { workspace_id: authority.workspace_id, provider: 'klaviyo', status: 'pending_account_selection' }, error: null };
  const { calls, store } = fixture(result);
  const written = await store.beginAccountSelection({ authority, provider: 'klaviyo', accessToken: 'access', refreshToken: 'refresh' });
  assert.equal(written.status, 'pending_account_selection');
  const insert = calls.find((call) => call[0] === 'insert');
  assert.equal(calls[0][1], TABLE);
  assert.equal(insert[1].workspace_id, authority.workspace_id);
  assert.equal(insert[1].last_authorized_via, 'shopify_verified_session');
  assert.equal(insert[1].active_account_id, null);
  assert.equal(Object.hasOwn(insert[1], 'updated_at'), false);
  assert.equal(JSON.stringify(insert[1]).includes('shop_id'), false);
});

test('new OAuth cannot silently replace an existing canonical connection', async () => {
  const { store } = fixture({ data: null, error: { code: '23505' } });
  await assert.rejects(
    store.beginAccountSelection({ authority, provider: 'meta', accessToken: 'access' }),
    /CANONICAL_CONNECTION_ALREADY_EXISTS/
  );
});

test('parked providers cannot enter canonical R4 connection store', async () => {
  assert.deepEqual([...ACTIVE_PROVIDERS], ['meta', 'google_ads', 'klaviyo']);
  const { store } = fixture();
  await assert.rejects(
    store.beginAccountSelection({ authority, provider: 'tiktok', accessToken: 'access' }),
    /PROVIDER_NOT_ACTIVE_IN_R4/
  );
  await assert.rejects(
    store.beginAccountSelection({ authority, provider: 'pinterest', accessToken: 'access' }),
    /PROVIDER_NOT_ACTIVE_IN_R4/
  );
});

test('browser-shaped authority cannot read or write canonical connections', async () => {
  const { store } = fixture();
  await assert.rejects(
    store.readStatus({ authority: { authority: 'browser', workspace_id: authority.workspace_id, source: 'body' }, provider: 'meta' }),
    /SERVER_WORKSPACE_AUTHORITY_REQUIRED/
  );
});

test('connected resolver is workspace-scoped and returns decrypted token only server-side', async () => {
  const encrypted = (value) => ({ version: 1, keyId: 'k', iv: 'i', tag: 't', ciphertext: `enc:${value}` });
  const { calls, store } = fixture({
    data: {
      status: 'connected', active_account_id: 'account', source_currency: 'USD',
      access_token_envelope: encrypted('access'), refresh_token_envelope: encrypted('refresh'),
      access_token_expires_at: '2026-09-23T10:00:00.000Z', refresh_token_expires_at: null,
      granted_scopes: ['accounts:read'], connection_version: 4
    },
    error: null
  });
  const resolved = await store.resolveConnected({ authority, provider: 'meta' });
  assert.equal(resolved.accessToken, 'access');
  assert.equal(resolved.version, 4);
  assert.equal(resolved.accessTokenExpiresAt, '2026-09-23T10:00:00.000Z');
  assert.deepEqual(resolved.grantedScopes, ['accounts:read']);
  assert.deepEqual(calls.filter((call) => call[0] === 'eq'), [
    ['eq', 'workspace_id', authority.workspace_id],
    ['eq', 'provider', 'meta'],
    ['eq', 'status', 'connected']
  ]);
});

test('connected Klaviyo refresh rotates encrypted tokens with optimistic version protection', async () => {
  const calls = [];
  const query = {
    update(row) { calls.push(['update', row]); return query; },
    eq(field, value) { calls.push(['eq', field, value]); return query; },
    select(columns) { calls.push(['select', columns]); return query; },
    async maybeSingle() { return { data: { connection_version: 5 }, error: null }; },
  };
  const client = { from(table) { calls.push(['from', table]); return query; } };
  const vault = {
    encrypt(value, context) { return { ciphertext: `enc:${value}`, context }; },
    decrypt(envelope) { return envelope.ciphertext.replace(/^enc:/, ''); },
  };
  const store = createCanonicalWorkspaceProviderConnectionStore({ client, vault, now: () => new Date('2026-09-23T09:00:00.000Z') });
  await store.refreshConnectedKlaviyo({
    authority, version: 4, accessToken: 'new-access', refreshToken: 'new-refresh',
    expiresAt: '2026-09-23T10:00:00.000Z', scopes: ['accounts:read', 'campaigns:read'],
  });
  const update = calls.find(([name]) => name === 'update')[1];
  assert.equal(update.access_token_envelope.ciphertext, 'enc:new-access');
  assert.equal(update.refresh_token_envelope.ciphertext, 'enc:new-refresh');
  assert.equal(update.access_token_expires_at, '2026-09-23T10:00:00.000Z');
  assert.deepEqual(update.granted_scopes, ['accounts:read', 'campaigns:read']);
  assert.deepEqual(calls.filter(([name]) => name === 'eq'), [
    ['eq', 'workspace_id', authority.workspace_id],
    ['eq', 'provider', 'klaviyo'],
    ['eq', 'connection_version', 4],
    ['eq', 'status', 'connected'],
  ]);
});

test('R4 security scripts are fail-closed and rollback refuses populated authority', () => {
  const preflight = read(contract.production_gate.preflight);
  const postcheck = read(contract.production_gate.postcheck);
  const rollback = read(contract.production_gate.rollback);
  assert.match(preflight, /legacy_plaintext_token_rows = 0/);
  assert.match(preflight, /BLOCK_R4B_APPLY/);
  assert.match(postcheck, /canonical_table_empty/);
  assert.match(postcheck, /browser_roles_denied/);
  assert.match(rollback, /R4_ROLLBACK_BLOCKED_CANONICAL_CONNECTION_ROWS_EXIST/);
});

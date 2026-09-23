'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  FROZEN_LEGACY_PLATFORMS,
  isLegacyProviderRuntimeFrozen,
  isStandaloneOAuthRouteFrozen,
  assertLegacyProviderWriteAllowed
} = require('../src/providers/legacy-provider-freeze');
const { registerOAuthProviderRoutes } = require('../src/oauth/provider-routes');
const { createRefreshJobBoundary } = require('../src/jobs/refresh-job-boundary');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');

test('R4-C freezes active and parked legacy provider authorities but leaves non-provider legacy utilities alone', () => {
  assert.deepEqual([...FROZEN_LEGACY_PLATFORMS], ['meta', 'google', 'klaviyo', 'tiktok', 'pinterest']);
  for (const provider of FROZEN_LEGACY_PLATFORMS) {
    assert.equal(isLegacyProviderRuntimeFrozen(provider), true);
    assert.equal(isStandaloneOAuthRouteFrozen(provider), true);
    assert.throws(() => assertLegacyProviderWriteAllowed(provider), /frozen by R4-C/);
  }
  assert.equal(isLegacyProviderRuntimeFrozen('organic'), false);
  assert.equal(isLegacyProviderRuntimeFrozen('google_sheets'), false);
});

test('standalone OAuth start and callback fail before either legacy handler runs', () => {
  const routes = new Map();
  const app = { get(route, handler) { routes.set(route, handler); } };
  let called = 0;
  registerOAuthProviderRoutes({ app, provider: 'klaviyo', startHandler: () => called++, callbackHandler: () => called++, frozen: true });
  const response = { statusCode: null, body: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
  routes.get('/auth/klaviyo')({}, response);
  assert.equal(response.statusCode, 409);
  assert.equal(response.body.code, 'LEGACY_PROVIDER_RUNTIME_FROZEN');
  routes.get('/auth/klaviyo/callback')({}, response);
  assert.equal(called, 0);
});

test('legacy refresh boundary rejects before reading or writing snapshot jobs', async () => {
  let clientRequested = false;
  const boundary = createRefreshJobBoundary({
    getClient: () => { clientRequested = true; return {}; },
    lifecycleVersion: 'test',
    isPlatformFrozen: isLegacyProviderRuntimeFrozen
  });
  await assert.rejects(
    boundary.create({ userId: 'user', platform: 'google', platformAccountId: 'account' }),
    error => error.code === 'LEGACY_PROVIDER_RUNTIME_FROZEN' && error.status === 409
  );
  assert.equal(clientRequested, false);
});

test('migration freezes writes, stops provider schedules, closes open jobs, and preserves historical rows', () => {
  const migration = read('supabase/migrations/20260923093756_r4c_freeze_legacy_provider_writes.sql');
  assert.match(migration, /update public\.snapshot_schedules[\s\S]*active = false/);
  assert.match(migration, /update public\.snapshot_jobs[\s\S]*status = 'failed'/);
  assert.match(migration, /new\.surface is distinct from 'shopify_embedded'/);
  assert.match(migration, /before insert or update or delete on public\.platform_connections/);
  assert.match(migration, /before insert or update or delete on public\.platform_connection_tokens/);
  assert.match(migration, /before insert or update or delete on public\.platform_account_ownerships/);
  assert.match(migration, /before insert or update or delete on public\.snapshot_schedules/);
  assert.match(migration, /before insert or update or delete on public\.snapshot_jobs/);
  assert.doesNotMatch(migration, /delete from|truncate|\/oauth\/revoke/i);
});

test('cron excludes frozen providers and queued backfill fails closed before provider work', () => {
  const server = read('server.js');
  assert.match(server, /if\(isLegacyProviderRuntimeFrozen\(job\.platform\)\)[\s\S]*legacy_provider_runtime_frozen/);
  assert.match(server, /from\("snapshot_schedules"\)[\s\S]*\.eq\("active",true\)[\s\S]*\.eq\("platform","organic"\)/);
  assert.match(server, /async function saveConnection\(userId,platform,payload\)\{\s*assertLegacyProviderWriteAllowed\(platform\)/);
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createKlaviyoTokenLifecycle } = require('../src/providers/klaviyo/token-lifecycle');

const authority = Object.freeze({
  authority: 'server_resolved_workspace',
  workspace_id: '11111111-1111-4111-8111-111111111111',
  source: 'shopify_verified_session',
});

function response(payload, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => payload };
}

function connected(overrides = {}) {
  return Object.freeze({
    provider: 'klaviyo', status: 'connected', version: 1,
    accessToken: 'access-token', refreshToken: 'refresh-token',
    accessTokenExpiresAt: null, refreshTokenExpiresAt: null,
    grantedScopes: ['accounts:read'],
    ...overrides,
  });
}

function fixture({ expiresAt = null, fetchImpl } = {}) {
  const calls = [];
  let connection = Object.freeze({
    provider: 'klaviyo', status: 'connected', version: 4,
    accessToken: 'old-access', refreshToken: 'old-refresh',
    accessTokenExpiresAt: expiresAt, refreshTokenExpiresAt: null,
    grantedScopes: ['accounts:read'],
  });
  const store = {
    resolveConnected: async input => (calls.push(['resolve', input]), connection),
    refreshConnectedKlaviyo: async input => {
      calls.push(['persist', input]);
      connection = Object.freeze({
        ...connection,
        version: input.version + 1,
        accessToken: input.accessToken,
        refreshToken: input.refreshToken,
        accessTokenExpiresAt: input.expiresAt,
        grantedScopes: input.scopes,
      });
    },
  };
  const lifecycle = createKlaviyoTokenLifecycle({
    connectionStore: store,
    fetchImpl: fetchImpl || (async () => response({ access_token: 'new-access', refresh_token: 'new-refresh', expires_in: 3600, scope: 'accounts:read campaigns:read' })),
    clientId: 'client', clientSecret: 'secret',
    now: () => new Date('2026-09-25T12:00:00.000Z'),
  });
  return { calls, lifecycle };
}

test('connected Klaviyo token with unknown expiry is refreshed and rotated without merchant OAuth', async () => {
  const { calls, lifecycle } = fixture();
  const result = await lifecycle.run({ authority, operation: async connection => connection.accessToken });
  assert.equal(result.value, 'new-access');
  assert.equal(result.connection.refreshToken, 'new-refresh');
  const persisted = calls.find(([name]) => name === 'persist')[1];
  assert.equal(persisted.version, 4);
  assert.equal(persisted.expiresAt, '2026-09-25T13:00:00.000Z');
  assert.deepEqual(persisted.scopes, ['accounts:read', 'campaigns:read']);
});

test('fresh connected token does not call refresh endpoint', async () => {
  let refreshCalls = 0;
  const { lifecycle } = fixture({
    expiresAt: '2026-09-25T13:00:00.000Z',
    fetchImpl: async () => { refreshCalls += 1; return response({}); },
  });
  const result = await lifecycle.run({ authority, operation: async connection => connection.accessToken });
  assert.equal(result.value, 'old-access');
  assert.equal(refreshCalls, 0);
});

test('unexpected 401 refreshes once and retries the provider operation once', async () => {
  const { calls, lifecycle } = fixture({ expiresAt: '2026-09-25T13:00:00.000Z' });
  let operations = 0;
  const result = await lifecycle.run({ authority, operation: async connection => {
    operations += 1;
    if (connection.accessToken === 'old-access') throw Object.assign(new Error('KLAVIYO_ACCESS_TOKEN_INVALID'), { code: 'KLAVIYO_ACCESS_TOKEN_INVALID' });
    return connection.accessToken;
  } });
  assert.equal(result.value, 'new-access');
  assert.equal(operations, 2);
  assert.equal(calls.filter(([name]) => name === 'persist').length, 1);
});

test('invalid refresh grant requires reauthorization and never runs provider operation', async () => {
  const { lifecycle } = fixture({ fetchImpl: async () => response({ error: 'invalid_grant' }, 400) });
  let operations = 0;
  await assert.rejects(lifecycle.run({ authority, operation: async () => { operations += 1; } }), error =>
    error.code === 'KLAVIYO_REAUTHORIZE' && error.status === 409);
  assert.equal(operations, 0);
});

test('parallel expired-token calls share one refresh request', async () => {
  let refreshCalls = 0;
  const { lifecycle } = fixture({ fetchImpl: async () => {
    refreshCalls += 1;
    await new Promise(resolve => setImmediate(resolve));
    return response({ access_token: 'new-access', refresh_token: 'new-refresh', expires_in: 3600, scope: 'accounts:read' });
  } });
  const results = await Promise.all([
    lifecycle.run({ authority, operation: async connection => connection.accessToken }),
    lifecycle.run({ authority, operation: async connection => connection.accessToken }),
  ]);
  assert.deepEqual(results.map(result => result.value), ['new-access', 'new-access']);
  assert.equal(refreshCalls, 1);
});

test('cross-instance refresh loser reuses the fresh canonical token instead of forcing merchant OAuth', async () => {
  const expired = connected({ version: 3, accessToken: 'expired-token', accessTokenExpiresAt: null });
  const winner = connected({ version: 4, accessToken: 'winner-token', accessTokenExpiresAt: '2026-09-25T14:00:00.000Z' });
  const operations = [];
  const lifecycle = createKlaviyoTokenLifecycle({
    connectionStore: {
      resolveConnected: async () => winner,
      refreshConnectedKlaviyo: async () => { throw new Error('must not persist invalid grant'); },
    },
    fetchImpl: async () => response({ error: 'invalid_grant' }, 400),
    clientId: 'client-id',
    clientSecret: 'client-secret',
    now: () => new Date('2026-09-25T12:00:00.000Z'),
  });
  const result = await lifecycle.run({
    authority,
    connection: expired,
    operation: async active => { operations.push(active.accessToken); return 'ok'; },
  });
  assert.equal(result.value, 'ok');
  assert.equal(result.connection.version, 4);
  assert.deepEqual(operations, ['winner-token']);
});

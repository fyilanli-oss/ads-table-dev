'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {ROUTES, SHARED_QUERY_IDENTITY_RISK} = require('../security/oauth-route-inventory');
const {createRequireConnectAccessForOAuth} = require('../security/oauth-access');

const SERVER_PATH = path.join(__dirname, '..', 'server.js');
const serverSource = fs.readFileSync(SERVER_PATH, 'utf8');
const dashboardSource = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard.html'), 'utf8');

function registeredAuthRoutes(source) {
  return [...source.matchAll(/app\.(?:get|post)\("(\/auth\/[^\"]+)"/g)]
    .map(match => match[1])
    .sort();
}

function responseRecorder() {
  return {
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; }
  };
}

test('OAuth inventory covers every registered /auth route', () => {
  const expected = ROUTES.flatMap(route => [route.start, route.callback]).sort();
  assert.deepEqual(registeredAuthRoutes(serverSource), expected);
});

test('every active OAuth start route uses the shared connection guard', () => {
  for (const route of ROUTES.filter(item => item.active)) {
    const start = serverSource.indexOf(`app.get("${route.start}"`);
    const callback = serverSource.indexOf(`app.get("${route.callback}"`, start);
    assert.notEqual(start, -1, `${route.provider} start route must exist`);
    assert.notEqual(callback, -1, `${route.provider} callback route must exist`);
    assert.match(
      serverSource.slice(start, callback),
      /requireConnectAccessForOAuth\(req,res\)/,
      `${route.provider} must be represented in the shared identity-risk baseline`
    );
  }
});

test('shared OAuth guard resolves identity from the verified bearer user', async () => {
  let subscriptionUserId = null;
  const guard = createRequireConnectAccessForOAuth({
    requireUser: async () => ({id: 'verified-user'}),
    getUserSubscription: async userId => { subscriptionUserId = userId; return {status: 'active'}; },
    getAccessByStatus: () => ({blocked: false, connect: true})
  });
  const result = await guard({query: {}}, responseRecorder());
  assert.equal(SHARED_QUERY_IDENTITY_RISK.status, 'resolved');
  assert.equal(SHARED_QUERY_IDENTITY_RISK.severity, 'critical');
  assert.equal(subscriptionUserId, 'verified-user');
  assert.equal(result.userId, 'verified-user');
});

test('shared OAuth guard rejects the legacy user_id query before subscription access', async () => {
  let subscriptionCalled = false;
  const guard = createRequireConnectAccessForOAuth({
    requireUser: async () => ({id: 'verified-user'}),
    getUserSubscription: async () => { subscriptionCalled = true; return {status: 'active'}; },
    getAccessByStatus: () => ({blocked: false, connect: true})
  });
  const response = responseRecorder();
  const result = await guard({query: {user_id: 'attacker-selected-user'}}, response);
  assert.equal(result, null);
  assert.equal(response.statusCode, 400);
  assert.equal(response.body.error, 'OAuth user_id query parameter is not accepted');
  assert.equal(subscriptionCalled, false);
});

test('shared OAuth guard preserves requireUser unauthenticated rejection', async () => {
  const response = responseRecorder();
  const guard = createRequireConnectAccessForOAuth({
    requireUser: async (_req, res) => { res.status(401).json({error: 'Not authenticated'}); return null; },
    getUserSubscription: async () => { throw new Error('must not run'); },
    getAccessByStatus: () => ({blocked: false, connect: true})
  });
  assert.equal(await guard({query: {}}, response), null);
  assert.equal(response.statusCode, 401);
});

test('shared OAuth guard rejects an authenticated user without connect access', async () => {
  const response = responseRecorder();
  const guard = createRequireConnectAccessForOAuth({
    requireUser: async () => ({id: 'verified-user'}),
    getUserSubscription: async () => ({status: 'expired'}),
    getAccessByStatus: () => ({blocked: false, connect: false})
  });
  assert.equal(await guard({query: {}}, response), null);
  assert.equal(response.statusCode, 403);
  assert.deepEqual(response.body, {error: 'Subscription inactive', status: 'expired'});
});

test('dashboard starts OAuth with a bearer-authenticated JSON handshake', () => {
  assert.match(dashboardSource, /startAuthenticatedOAuth/);
  assert.match(dashboardSource, /response_mode=json/);
  assert.doesNotMatch(dashboardSource, /\/auth\/(?:meta|google|klaviyo|tiktok|organic|google-sheets)\?user_id=/);
});

test('active callbacks characterize their current session-bound state and user fields', () => {
  for (const route of ROUTES.filter(item => item.active)) {
    const callback = serverSource.indexOf(`app.get("${route.callback}"`);
    assert.notEqual(callback, -1, `${route.provider} callback route must exist`);
    const nextRoute = serverSource.indexOf('\napp.', callback + 1);
    const body = serverSource.slice(callback, nextRoute === -1 ? serverSource.length : nextRoute);
    assert.match(body, new RegExp(`req\\.session\\.${route.state}`), `${route.provider} callback must check its current state field`);
    assert.match(body, new RegExp(`req\\.session\\.${route.user}`), `${route.provider} callback must read its current user field`);
    if (route.pkce) assert.match(body, new RegExp(`req\\.session\\.${route.verifier}`));
  }
});

test('production session configuration characterizes the known fallback and MemoryStore debt', () => {
  assert.match(serverSource, /process\.env\.SESSION_SECRET\|\|"dev_secret_change_me"/);
  assert.doesNotMatch(serverSource, /app\.use\(session\(\{[^}]*store\s*:/s);
});

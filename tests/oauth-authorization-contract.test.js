'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const ROUTE_DECLARATION = /app\.(?:get|post|put|patch|delete)\("[^"]+"/g;
const CALLER_IDENTITY = /req\.(?:body|query)(?:\?|)\.(?:user_id|owner_user_id)|req\.(?:body|query)\[['"](?:user_id|owner_user_id)['"]\]/;

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function functionBody(source, name, nextName) {
  const declaration = `async function ${name}(`;
  const boundary = `async function ${nextName}(`;
  const start = source.indexOf(declaration);
  assert.notEqual(start, -1, `${name} start function must exist`);
  assert.equal(source.indexOf(declaration, start + declaration.length), -1, `${name} must have one declaration`);
  const end = source.indexOf(boundary, start + declaration.length);
  assert.notEqual(end, -1, `${nextName} function boundary must exist after ${name}`);
  assert.ok(end > start, `${nextName} boundary must follow ${name}`);
  return source.slice(start, end);
}

function routeBody(source, method, route) {
  const declaration = `app.${method}("${route}"`;
  const starts = [...source.matchAll(new RegExp(escapeRegex(declaration), 'g'))].map(match => match.index);
  assert.equal(starts.length, 1, `${method.toUpperCase()} ${route} must have exactly one declaration`);
  ROUTE_DECLARATION.lastIndex = starts[0] + declaration.length;
  const next = ROUTE_DECLARATION.exec(source);
  ROUTE_DECLARATION.lastIndex = 0;
  assert.ok(next, `${method.toUpperCase()} ${route} must have a following route boundary`);
  assert.ok(next.index > starts[0], `${method.toUpperCase()} ${route} boundary must follow its declaration`);
  return source.slice(starts[0], next.index);
}

function assertAuthenticatedUserScope(route, table) {
  const auth = route.indexOf('requireUser(req,res)');
  const scope = route.search(new RegExp(`from\\("${escapeRegex(table)}"\\)[\\s\\S]*?\\.eq\\("user_id",user\\.id\\)`));
  assert.notEqual(auth, -1, 'route must authenticate with requireUser');
  assert.notEqual(scope, -1, `route must scope ${table} by user.id`);
  assert.ok(auth < scope, 'authentication must happen before the user-scoped query');
  assert.doesNotMatch(route, CALLER_IDENTITY);
}

function assertDisconnectRoute(route) {
  const auth = route.indexOf('requireUser(req,res)');
  const disconnect = route.indexOf('disconnectPlatformLifecycle(user.id,');
  assert.notEqual(auth, -1, 'disconnect route must authenticate');
  assert.notEqual(disconnect, -1, 'disconnect route must use authenticated user.id');
  assert.ok(auth < disconnect, 'authentication must happen before disconnect');
  assert.doesNotMatch(route, CALLER_IDENTITY);
}

test('provider ownership binds active ownership to authenticated user', () => {
  const body = functionBody(serverSource, 'requireActiveOwnership', 'disconnectPlatformLifecycle');
  assert.match(body, /ownership\.owner_user_id!==userId/);
  assert.match(body, /activeOwnershipStatuses\(\)\.includes\(ownership\.status\)/);
});

test('disconnect lifecycle is exactly function-bounded and authenticated-user scoped', () => {
  const body = functionBody(serverSource, 'disconnectPlatformLifecycle', 'createRefreshJob');
  assert.match(body, /from\("platform_connections"\)[\s\S]*?\.eq\("user_id",userId\)/);
  assert.match(body, /from\("platform_account_ownerships"\)[\s\S]*?\.eq\("owner_user_id",userId\)/);
  assert.match(body, /providerTokenStore\.remove\(\{userId,platform\}\)/);
  assert.doesNotMatch(body, /req\.(?:body|query)|owner_user_id\s*:\s*req|user_id\s*:\s*req/);
});

test('snapshot job list route uses authenticated user scope', () => {
  assertAuthenticatedUserScope(routeBody(serverSource, 'get', '/api/refresh/status'), 'snapshot_jobs');
});

test('dashboard snapshot read route uses authenticated user scope', () => {
  assertAuthenticatedUserScope(routeBody(serverSource, 'get', '/api/snapshots/meta/latest'), 'dashboard_snapshots');
});

test('connection debug route uses authenticated user scope', () => {
  assertAuthenticatedUserScope(routeBody(serverSource, 'get', '/api/debug/connections'), 'platform_connections');
});

test('platform account list route uses authenticated user scope', () => {
  assertAuthenticatedUserScope(routeBody(serverSource, 'get', '/api/accounts'), 'platform_ad_accounts');
});

test('every disconnect route is bounded and authenticates before server-side identity use', () => {
  const routes = [
    ['/api/disconnect/:platform', 'post'],
    ['/api/connections/:platform/disconnect', 'post'],
    ['/api/platform/meta/disconnect', 'post'],
    ['/api/platform/google/disconnect', 'post'],
    ['/api/platform/organic/disconnect', 'post'],
    ['/api/platform/klaviyo/disconnect', 'post']
  ];
  for (const [route, method] of routes) assertDisconnectRoute(routeBody(serverSource, method, route));
});

test('function extractor fails closed for missing starts and boundaries', () => {
  const fixture = 'async function first(){}\nasync function second(){}';
  assert.throws(() => functionBody(fixture, 'missing', 'second'), /start function must exist/);
  assert.throws(() => functionBody(fixture, 'first', 'missing'), /function boundary must exist/);
  assert.throws(() => functionBody(fixture, 'second', 'first'), /function boundary must exist/);
});

test('route extractor fails closed for missing, duplicate, and unbounded routes', () => {
  assert.throws(() => routeBody('app.get("/present",handler);\napp.post("/next",handler);', 'get', '/missing'), /exactly one declaration/);
  assert.throws(() => routeBody('app.get("/same",a);\napp.get("/same",b);\napp.get("/next",c);', 'get', '/same'), /exactly one declaration/);
  assert.throws(() => routeBody('app.get("/last",handler);', 'get', '/last'), /following route boundary/);
});

test('user scope outside the route boundary cannot create a false positive', () => {
  const fixture = 'app.get("/unsafe",async(req,res)=>{await requireUser(req,res);});\n' +
    'app.get("/safe",async(req,res)=>{await requireUser(req,res);db.from("snapshot_jobs").eq("user_id",user.id);});\n' +
    'app.post("/end",handler);';
  assert.throws(() => assertAuthenticatedUserScope(routeBody(fixture, 'get', '/unsafe'), 'snapshot_jobs'), /scope snapshot_jobs/);
});

test('caller-controlled identity inside a route is rejected', () => {
  const fixture = 'app.post("/disconnect",async(req,res)=>{const user=await requireUser(req,res);const user_id=req.body.user_id;await disconnectPlatformLifecycle(user.id,"meta");});\n' +
    'app.get("/end",handler);';
  assert.throws(() => assertDisconnectRoute(routeBody(fixture, 'post', '/disconnect')), /req\.body/);
});

'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { createTikTokSandboxConnectHandler } = require('../src/providers/tiktok/sandbox-connect-handler');

test('ready sandbox Connect bypasses OAuth and creates an unselected sandbox connection', async () => {
  const saves = [], redirects = [];
  const handler = createTikTokSandboxConnectHandler({ readiness: () => ({ ready: true }), requireConnectAccess: async () => ({ userId: 'user' }), saveConnection: async (...args) => saves.push(args), accessToken: 'server-only', sandboxBase: 'https://sandbox-ads.tiktok.com/open_api', fallback: async () => assert.fail('OAuth fallback called') });
  await handler({}, { redirect: value => redirects.push(value) });
  assert.equal(saves.length, 1);
  assert.equal(saves[0][0], 'user');
  assert.equal(saves[0][1], 'tiktok');
  assert.equal(saves[0][2].metadata.accountSelectionRequired, true);
  assert.equal(saves[0][2].metadata.reportBase, 'https://sandbox-ads.tiktok.com/open_api');
  assert.deepEqual(redirects, ['/dashboard?tiktok_connected=1&account_selection_required=1']);
});

test('non-ready sandbox preserves the normal OAuth start handler', async () => {
  let fallbackCalls = 0;
  const handler = createTikTokSandboxConnectHandler({ readiness: () => ({ ready: false }), requireConnectAccess: async () => assert.fail(), saveConnection: async () => assert.fail(), accessToken: null, sandboxBase: null, fallback: async () => { fallbackCalls += 1; return 'oauth'; } });
  assert.equal(await handler({}, {}), 'oauth');
  assert.equal(fallbackCalls, 1);
});

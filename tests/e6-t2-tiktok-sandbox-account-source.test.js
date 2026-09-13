'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { sandboxAdvertiser } = require('../src/providers/tiktok/sandbox-account-source');

const enabled = Object.freeze({ production: false, tiktokSandboxEnabled: true, tiktokForceSandboxReports: true });

test('approved non-production sandbox exposes one server-configured advertiser without a token', () => {
  const account = sandboxAdvertiser({ productionConfig: enabled, advertiserId: 'sandbox-advertiser', advertiserName: 'Sandbox', sandboxBase: 'https://sandbox-ads.tiktok.com/open_api' });
  assert.deepEqual(account, { advertiser_id: 'sandbox-advertiser', advertiser_name: 'Sandbox', status: 'active', currency: null, sandbox: true, reportBase: 'https://sandbox-ads.tiktok.com/open_api', tokenSource: 'server_sandbox_access_token' });
  assert.equal(Object.hasOwn(account, 'access_token'), false);
  assert.equal(Object.hasOwn(account, 'token'), false);
});

test('sandbox account source is impossible in production or without both explicit switches', () => {
  assert.equal(sandboxAdvertiser({ productionConfig: { ...enabled, production: true }, advertiserId: 'x' }), null);
  assert.equal(sandboxAdvertiser({ productionConfig: { ...enabled, tiktokForceSandboxReports: false }, advertiserId: 'x' }), null);
  assert.throws(() => sandboxAdvertiser({ productionConfig: enabled }), /advertiser id is required/);
});

test('selection persists sandbox routing metadata and normal OAuth discovery remains available', () => {
  const server = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(server, /advertiser_source:"non_production_sandbox"/);
  assert.match(server, /reportBase:primary\?\.reportBase\|\|null,tokenSource:primary\?\.tokenSource\|\|null,sandbox:primary\?\.sandbox===true/);
  assert.match(server, /endpoint:"\/v1\.3\/oauth2\/advertiser\/get\/"/);
});

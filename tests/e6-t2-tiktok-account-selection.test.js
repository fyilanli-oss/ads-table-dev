'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const dashboards = ['public/dashboard.html', 'public/dashboard-patch17H-fixed.html', 'public/dashboard-patch17H-fixed-v2.html'];

test('OAuth advertiser discovery uses the official connected-token endpoint, not the sandbox host', () => {
  const route = server.slice(server.indexOf('app.get("/api/tiktok/advertisers"'), server.indexOf('app.get("/api/tiktok/campaigns"'));
  assert.match(route, /base:TIKTOK_API_BASE/);
  assert.match(route, /endpoint:"\/v1\.3\/oauth2\/advertiser\/get\/"/);
  assert.match(route, /headers:\{"Access-Token":conn\.access_token\}/);
  assert.doesNotMatch(route, /TIKTOK_SANDBOX_API_BASE|TIKTOK_SANDBOX_ACCESS_TOKEN|TIKTOK_TEST_ACCESS_TOKEN/);
});

test('an empty account result consumes reconnect parameters before Close', () => {
  for (const file of dashboards) {
    const html = fs.readFileSync(path.join(root, file), 'utf8');
    assert.match(html, /if\(!accounts\.length\)\{\s*cleanReconnectUrl\(\);\s*if\(desc\)desc\.textContent="No accessible account was found\.";/, file);
  }
});

'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { CUSTOMER_CLIENT_QUERY, discoverGoogleCustomers } = require('../src/providers/google/customer-discovery');

test('manager-accessible root expands to selectable ad accounts with its login customer id', async () => {
  const calls = [];
  const result = await discoverGoogleCustomers({
    resourceNames: ['customers/100'],
    search: async request => {
      calls.push(request);
      return { results: [
        { customerClient: { id: '100', clientCustomer: 'customers/100', descriptiveName: 'Test manager', manager: true, level: '0', testAccount: true } },
        { customerClient: { id: '200', clientCustomer: 'customers/200', descriptiveName: 'Test ad account', manager: false, level: '1', testAccount: true, currencyCode: 'USD', timeZone: 'America/Los_Angeles', status: 'ENABLED' } }
      ] };
    }
  });
  assert.deepEqual(calls, [{ customerId: '100', query: CUSTOMER_CLIENT_QUERY }]);
  assert.equal(result.customers.length, 1);
  assert.equal(result.customers[0].customerId, '200');
  assert.equal(result.customers[0].loginCustomerId, '100');
  assert.equal(result.managers[0].customerId, '100');
  assert.deepEqual(result.discovery, { accessible_root_count: 1, queried_root_count: 1, failed_root_count: 0 });
});

test('directly accessible ad account remains selectable and uses itself as login context', async () => {
  const result = await discoverGoogleCustomers({ resourceNames: ['customers/300'], search: async () => ({ results: [{ customerClient: { id: '300', manager: false, level: 0 } }] }) });
  assert.equal(result.customers[0].customerId, '300');
  assert.equal(result.customers[0].login_customer_id, '300');
});

test('malformed provider shapes fail closed instead of returning a manager as an ad account', async () => {
  await assert.rejects(discoverGoogleCustomers({ resourceNames: ['customers/not-an-id'], search: async () => ({}) }), /resource name/);
  await assert.rejects(discoverGoogleCustomers({ resourceNames: ['customers/100'], search: async () => ({ results: [{}] }) }), /row is invalid/);
});

test('one inaccessible root cannot hide a valid test manager hierarchy', async () => {
  const result = await discoverGoogleCustomers({
    resourceNames: ['customers/100', 'customers/200'],
    search: async ({ customerId }) => {
      if (customerId === '100') throw new Error('redacted provider failure');
      return { results: [
        { customerClient: { id: '200', manager: true, level: 0 } },
        { customerClient: { id: '300', manager: false, level: 1 } }
      ] };
    }
  });
  assert.equal(result.customers[0].customerId, '300');
  assert.equal(result.customers[0].loginCustomerId, '200');
  assert.deepEqual(result.discovery, { accessible_root_count: 2, queried_root_count: 1, failed_root_count: 1 });
});

test('all inaccessible roots fail with a redacted discovery error', async () => {
  await assert.rejects(discoverGoogleCustomers({ resourceNames: ['customers/100'], search: async () => { throw new Error('raw provider body'); } }), error => error.message === 'Google Ads customer hierarchy discovery failed' && !error.message.includes('raw provider body'));
});

test('account-selection discovery failure consumes reconnect URL before Close reloads', () => {
  for (const file of ['dashboard.html', 'dashboard-patch17H-fixed.html', 'dashboard-patch17H-fixed-v2.html']) {
    const source = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
    assert.match(source, /catch\(error\)\{\s*cleanReconnectUrl\(\);\s*openSelection\(platform,\[\]\);/);
  }
});

test('OAuth token without completed account selection is not shown as connected', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(source, /selectionRequired=r\?\.metadata\?\.accountSelectionRequired===true/);
  assert.match(source, /refresh_token\)&&!selectionRequired/);
});

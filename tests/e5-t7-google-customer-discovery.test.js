'use strict';
const assert = require('node:assert/strict');
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
  assert.deepEqual(calls, [{ customerId: '100', loginCustomerId: '100', query: CUSTOMER_CLIENT_QUERY }]);
  assert.equal(result.customers.length, 1);
  assert.equal(result.customers[0].customerId, '200');
  assert.equal(result.customers[0].loginCustomerId, '100');
  assert.equal(result.managers[0].customerId, '100');
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

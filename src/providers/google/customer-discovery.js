'use strict';

const CUSTOMER_CLIENT_QUERY = `
  SELECT
    customer_client.client_customer,
    customer_client.id,
    customer_client.descriptive_name,
    customer_client.currency_code,
    customer_client.time_zone,
    customer_client.manager,
    customer_client.level
  FROM customer_client
  WHERE customer_client.level <= 1
`;

function customerId(resourceName) {
  const match = String(resourceName || '').match(/^customers\/(\d+)$/);
  if (!match) throw new Error('Google accessible customer resource name is invalid');
  return match[1];
}

function mapClient(row, loginCustomerId) {
  const client = row?.customerClient || row?.customer_client;
  if (!client || typeof client !== 'object') throw new Error('Google customer_client row is invalid');
  const id = String(client.id || customerId(client.clientCustomer || client.client_customer));
  return Object.freeze({
    resourceName: `customers/${id}`,
    customerId: id,
    platform_account_id: id,
    loginCustomerId,
    login_customer_id: loginCustomerId,
    account_name: client.descriptiveName || client.descriptive_name || `Google Ads ${id}`,
    currency: client.currencyCode || client.currency_code || null,
    timezone: client.timeZone || client.time_zone || null,
    manager: Boolean(client.manager),
    level: Number(client.level || 0),
    status: client.status || null,
    test_account: Boolean(client.testAccount ?? client.test_account)
  });
}

async function discoverGoogleCustomers({ resourceNames, search } = {}) {
  if (!Array.isArray(resourceNames)) throw new TypeError('Google accessible customer resource names must be an array');
  if (typeof search !== 'function') throw new TypeError('Google customer hierarchy search is required');
  const accounts = new Map();
  const managers = [];
  let queriedRootCount = 0;
  let failedRootCount = 0;
  for (const resourceName of resourceNames) {
    const loginCustomerId = customerId(resourceName);
    // The root is directly accessible by definition. Query it without a
    // login-customer-id header; the root becomes the login context only for
    // requests made against discovered child accounts.
    let response;
    try {
      response = await search({ customerId: loginCustomerId, query: CUSTOMER_CLIENT_QUERY });
      queriedRootCount += 1;
    } catch {
      failedRootCount += 1;
      continue;
    }
    const rows = Array.isArray(response?.results) ? response.results : [];
    for (const row of rows) {
      const client = mapClient(row, loginCustomerId);
      if (client.manager) managers.push(client);
      else if (!accounts.has(client.customerId)) accounts.set(client.customerId, client);
    }
  }
  if (resourceNames.length && queriedRootCount === 0) throw new Error('Google Ads customer hierarchy discovery failed');
  return Object.freeze({
    customers: Object.freeze([...accounts.values()]),
    managers: Object.freeze(managers),
    discovery: Object.freeze({ accessible_root_count: resourceNames.length, queried_root_count: queriedRootCount, failed_root_count: failedRootCount })
  });
}

module.exports = Object.freeze({ CUSTOMER_CLIENT_QUERY, discoverGoogleCustomers });

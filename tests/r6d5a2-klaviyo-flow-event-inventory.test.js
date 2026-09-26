'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createKlaviyoFlowEventInventory } = require('../src/providers/klaviyo/flow-event-inventory');
const { registerShopifyKlaviyoAccountRoutes } = require('../src/routes/shopify-klaviyo-account-routes');

const WORKSPACE = '11111111-1111-4111-8111-111111111111';
const authority = { authority: 'server_resolved_workspace', workspace_id: WORKSPACE, source: 'shopify_verified_session' };
const connection = {
  provider: 'klaviyo',
  status: 'connected',
  accessToken: 'secret-token',
  selectedAccounts: [{ id: 'account-secret', name: 'Private account', currency: 'USD' }],
};

test('R6-D5-A2 returns only aggregate Flow/Event inventory and never writes Dataset V2', async () => {
  const service = createKlaviyoFlowEventInventory({
    connectionStore: {
      resolveConnected: async input => {
        assert.deepEqual(input, { authority, provider: 'klaviyo' });
        return connection;
      },
    },
    providerClient: {
      fetchAccount: async ({ accessToken, accountId }) => {
        assert.equal(accessToken, 'secret-token');
        assert.equal(accountId, 'account-secret');
        return { id: accountId, timezone: 'UTC', currency: 'USD' };
      },
      fetchFlowEventInventory: async ({ accessToken, timeZone }) => {
        assert.equal(accessToken, 'secret-token');
        assert.equal(timeZone, 'UTC');
        return {
          flow_count: 2,
          flow_status_counts: { live: 1, manual: 0, draft: 1, other: 0 },
          scanned_event_count: 5,
          event_counts: { received_email: 2, placed_order: 1, other: 2 },
          attributed_event_count: 2,
          earliest_event_date: '2024-01-01',
          latest_event_date: '2024-01-03',
          event_scan_truncated: false,
        };
      },
    },
  });
  const result = await service.execute(authority);
  assert.deepEqual(result, {
    status: 'PASS_R6_D5_A2_KLAVIYO_FLOW_EVENT_INVENTORY',
    flow_count: 2,
    flow_status_counts: { live: 1, manual: 0, draft: 1, other: 0 },
    scanned_event_count: 5,
    event_counts: { received_email: 2, placed_order: 1, other: 2 },
    attributed_event_count: 2,
    earliest_event_date: '2024-01-01',
    latest_event_date: '2024-01-03',
    event_scan_truncated: false,
    account_api_verified: true,
    flow_inventory_verified: true,
    event_inventory_verified: true,
    dataset_v2_write: false,
    production_activation: false,
  });
  assert.doesNotMatch(JSON.stringify(result), /secret|Private account/i);
});

test('R6-D5-A2 route is Shopify-session-bound and ignores caller tenant/date fields', async () => {
  const routes = {};
  const app = { get() {}, post: (path, handler) => { routes[path] = handler; } };
  let received;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async ({ session_token }) => {
      assert.equal(session_token, 'session');
      return authority;
    },
    selection: {},
    flowEventInventory: {
      execute: async input => {
        received = input;
        return { status: 'PASS_R6_D5_A2_KLAVIYO_FLOW_EVENT_INVENTORY' };
      },
    },
  });
  const res = {
    set() {},
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return body; },
  };
  await routes['/api/shopify/providers/klaviyo/runtime/flow-event-inventory']({
    get: () => 'Bearer session',
    body: { workspace_id: 'attacker', provider_date: '2099-01-01' },
  }, res);
  assert.deepEqual(received, authority);
  assert.equal(res.code, 200);
});

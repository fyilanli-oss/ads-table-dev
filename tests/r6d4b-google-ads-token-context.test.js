'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {createEmbeddedProviderTokenExchanges, GOOGLE_ADS_SCOPE} = require('../src/shopify/embedded-provider-token-exchange');
const {createGoogleAdsTokenLifecycle, tokenExpiresSoon} = require('../src/providers/google/token-lifecycle');
const {createAdAccountSelection} = require('../src/shopify/ad-account-selection');
const {selectedAccounts} = require('../src/providers/workspace-provider-connection-store');

const authority = {authority: 'server_resolved_workspace', workspace_id: '11111111-1111-4111-8111-111111111111', source: 'shopify_verified_session'};
const response = (status, body) => ({status, ok: status >= 200 && status < 300, json: async () => body});

function exchanges(body) {
  return createEmbeddedProviderTokenExchanges({fetchImpl: async () => response(200, body)}).google_ads;
}

test('Google OAuth token exchange requires refresh token, expiry and adwords scope before persistence', async () => {
  const input = {code: 'code', redirectUri: 'https://app/callback', clientId: 'client', clientSecret: 'secret'};
  await assert.rejects(() => exchanges({access_token: 'access', expires_in: 3600, scope: GOOGLE_ADS_SCOPE})(input), error => error.code === 'GOOGLE_REFRESH_TOKEN_REQUIRED');
  await assert.rejects(() => exchanges({access_token: 'access', refresh_token: 'refresh', scope: GOOGLE_ADS_SCOPE})(input), error => error.code === 'GOOGLE_TOKEN_EXPIRY_REQUIRED');
  await assert.rejects(() => exchanges({access_token: 'access', refresh_token: 'refresh', expires_in: 3600, scope: 'openid'})(input), error => error.code === 'GOOGLE_ADS_SCOPE_MISSING');
  assert.deepEqual(await exchanges({access_token: 'access', refresh_token: 'refresh', expires_in: 3600, scope: GOOGLE_ADS_SCOPE})(input), {
    accessToken: 'access', refreshToken: 'refresh', expiresIn: 3600, scopes: [GOOGLE_ADS_SCOPE],
  });
});

test('Google canonical account shape requires verified manager context', () => {
  assert.deepEqual(selectedAccounts('google_ads', [{id: '456', name: 'Child', currency: 'EUR', loginCustomerId: '123'}]), [
    {id: '456', name: 'Child', currency: 'EUR', login_customer_id: '123'},
  ]);
  assert.throws(() => selectedAccounts('google_ads', [{id: '456', name: 'Child', currency: 'EUR'}]), /account.login_customer_id is required/);
  assert.throws(() => selectedAccounts('google_ads', [{id: '456', name: 'Child', currency: 'EUR', loginCustomerId: 'invalid'}]), /INVALID_GOOGLE_LOGIN_CUSTOMER_ID/);
});

test('Google lifecycle refreshes an expiring pending token, rotates refresh token and returns the winning version', async () => {
  const writes = [];
  let current = {
    status: 'pending_account_selection', connection_version: 3, accessToken: 'old-access', refreshToken: 'old-refresh',
    accessTokenExpiresAt: '2026-09-26T10:00:00.000Z', grantedScopes: [GOOGLE_ADS_SCOPE],
  };
  const store = {
    resolveConnected: async () => null,
    readPendingProvider: async () => current,
    refreshGoogle: async input => {
      writes.push(input);
      current = {...current, connection_version: 4, accessToken: input.accessToken, refreshToken: input.refreshToken, accessTokenExpiresAt: input.expiresAt, grantedScopes: input.scopes};
    },
  };
  const lifecycle = createGoogleAdsTokenLifecycle({
    connectionStore: store, clientId: 'client', clientSecret: 'secret', now: () => new Date('2026-09-26T12:00:00.000Z'),
    fetchImpl: async (_url, options) => {
      assert.match(options.body, /grant_type=refresh_token/);
      assert.doesNotMatch(options.body, /old-access/);
      return response(200, {access_token: 'new-access', refresh_token: 'new-refresh', expires_in: 3600, scope: GOOGLE_ADS_SCOPE});
    },
  });
  const result = await lifecycle.run({authority, connection: current, operation: connection => connection.accessToken});
  assert.equal(result.value, 'new-access');
  assert.equal(result.connection.connection_version, 4);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].version, 3);
  assert.equal(writes[0].status, 'pending_account_selection');
  assert.equal(writes[0].refreshToken, 'new-refresh');
});

test('Google lifecycle performs one refresh and one retry after an unexpected auth failure', async () => {
  let operations = 0;
  let refreshes = 0;
  let current = {
    status: 'connected', version: 7, accessToken: 'current', refreshToken: 'refresh',
    accessTokenExpiresAt: '2026-09-26T14:00:00.000Z', grantedScopes: [GOOGLE_ADS_SCOPE],
  };
  const store = {
    readPendingProvider: async () => null,
    resolveConnected: async () => current,
    refreshGoogle: async input => {
      refreshes += 1;
      current = {...current, version: 8, accessToken: input.accessToken, refreshToken: input.refreshToken, accessTokenExpiresAt: input.expiresAt};
    },
  };
  const lifecycle = createGoogleAdsTokenLifecycle({
    connectionStore: store, clientId: 'client', clientSecret: 'secret', now: () => new Date('2026-09-26T12:00:00.000Z'),
    fetchImpl: async () => response(200, {access_token: 'renewed', expires_in: 3600, scope: GOOGLE_ADS_SCOPE}),
  });
  const result = await lifecycle.run({authority, connection: current, operation: connection => {
    operations += 1;
    if (operations === 1) throw Object.assign(new Error('PROVIDER_REAUTHORIZE'), {code: 'PROVIDER_REAUTHORIZE'});
    return connection.accessToken;
  }});
  assert.equal(result.value, 'renewed');
  assert.equal(refreshes, 1);
  assert.equal(operations, 2);
});

test('Google invalid_grant requires reauthorization and does not invoke the operation', async () => {
  let operations = 0;
  const current = {
    status: 'connected', version: 2, accessToken: 'expired', refreshToken: 'invalid',
    accessTokenExpiresAt: '2026-09-26T10:00:00.000Z', grantedScopes: [GOOGLE_ADS_SCOPE],
  };
  const store = {
    readPendingProvider: async () => null,
    resolveConnected: async () => current,
    refreshGoogle: async () => assert.fail('invalid grant must not be persisted'),
  };
  const lifecycle = createGoogleAdsTokenLifecycle({
    connectionStore: store, clientId: 'client', clientSecret: 'secret', now: () => new Date('2026-09-26T12:00:00.000Z'),
    fetchImpl: async () => response(400, {error: 'invalid_grant'}),
  });
  await assert.rejects(() => lifecycle.run({authority, connection: current, operation: () => {operations += 1;}}), error => error.code === 'GOOGLE_REAUTHORIZE' && error.status === 409);
  assert.equal(operations, 0);
});

test('account selection uses the refreshed connection version and persists verified login context', async () => {
  const writes = [];
  const pending = {status: 'pending_account_selection', connection_version: 4, accessToken: 'old'};
  const renewed = {...pending, connection_version: 5, accessToken: 'new'};
  const account = {id: '456', name: 'Verified Google', currency: 'EUR', loginCustomerId: '123'};
  const selection = createAdAccountSelection({
    store: {
      readPendingProvider: async () => pending,
      completeAccountSelection: async input => writes.push(input),
    },
    discoverByProvider: {meta: async () => [], google_ads: async token => {
      assert.equal(token, 'new');
      return [account];
    }},
    tokenLifecycleByProvider: {google_ads: {run: async ({operation}) => ({value: await operation(renewed), connection: renewed})}},
  });
  const result = await selection.complete(authority, 'google_ads', {account_ids: ['456']});
  assert.deepEqual(result.accounts, [account]);
  assert.equal(writes[0].version, 5);
  assert.deepEqual(writes[0].accounts, [account]);
});

test('token expiry guard refreshes missing, invalid and near-expiry access tokens only', () => {
  const now = new Date('2026-09-26T12:00:00.000Z');
  assert.equal(tokenExpiresSoon({}, now), true);
  assert.equal(tokenExpiresSoon({accessToken: 'a', accessTokenExpiresAt: 'invalid'}, now), true);
  assert.equal(tokenExpiresSoon({accessToken: 'a', accessTokenExpiresAt: '2026-09-26T12:01:00.000Z'}, now), true);
  assert.equal(tokenExpiresSoon({accessToken: 'a', accessTokenExpiresAt: '2026-09-26T13:00:00.000Z'}, now), false);
});

test('R6-D4-B contract keeps production and completed-data gates closed', () => {
  const contract = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../contracts/r6d4b-google-ads-token-context-v1.json'), 'utf8'));
  assert.equal(contract.database_migration_required, false);
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.equal(contract.preserved_boundaries.E5_redevelopment, false);
  assert.ok(contract.excluded.includes('disconnect_or_provider_revoke'));
});


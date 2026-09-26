'use strict';

const CONFIRMATION = 'DISCONNECT_GOOGLE_ADS';
const GOOGLE_REVOKE_URL = 'https://oauth2.googleapis.com/revoke';

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}
function result(providerGrantRevoked = false) {
  return Object.freeze({status: 'not_connected', historical_data_preserved: true, provider_grant_revoked: providerGrantRevoked, local_credentials_removed: true});
}
async function revokeGoogleGrant(connection, fetchImpl) {
  const token = connection.refreshToken || connection.accessToken;
  if (!token) throw failure('GOOGLE_REVOKE_FAILED');
  let response;
  try {
    response = await fetchImpl(GOOGLE_REVOKE_URL, {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: new URLSearchParams({token}).toString(),
    });
  } catch {
    throw failure('GOOGLE_REVOKE_FAILED');
  }
  if (response.ok) return;
  const body = await response.text().catch(() => '');
  if (response.status === 400 && /invalid_token/i.test(body)) return;
  throw failure('GOOGLE_REVOKE_FAILED');
}
function createGoogleDisconnect({store, fetchImpl = globalThis.fetch} = {}) {
  if (!store || typeof store.resolveConnected !== 'function' || typeof store.disconnectGoogle !== 'function') throw new TypeError('Google Ads disconnect store is required');
  if (typeof fetchImpl !== 'function') throw new TypeError('Google revoke transport is required');
  return Object.freeze({
    async execute(authority, confirmation) {
      if (confirmation !== CONFIRMATION) throw failure('GOOGLE_DISCONNECT_CONFIRMATION_REQUIRED', 409);
      const connection = await store.resolveConnected({authority, provider: 'google_ads'});
      if (!connection) return result();
      await revokeGoogleGrant(connection, fetchImpl);
      await store.disconnectGoogle({authority, version: connection.version});
      return result(true);
    },
  });
}
module.exports = Object.freeze({CONFIRMATION, GOOGLE_REVOKE_URL, createGoogleDisconnect});

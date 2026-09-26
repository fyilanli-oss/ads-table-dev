'use strict';

const CONFIRMATION = 'DISCONNECT_GOOGLE_ADS';

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}

function result() {
  return Object.freeze({
    status: 'not_connected',
    historical_data_preserved: true,
    provider_grant_revoked: false,
    local_credentials_removed: true,
  });
}

function createGoogleDisconnect({store} = {}) {
  if (!store || typeof store.resolveConnected !== 'function' || typeof store.disconnectGoogle !== 'function') {
    throw new TypeError('Google Ads disconnect store is required');
  }

  return Object.freeze({
    async execute(authority, confirmation) {
      if (confirmation !== CONFIRMATION) throw failure('GOOGLE_DISCONNECT_CONFIRMATION_REQUIRED', 409);
      const connection = await store.resolveConnected({authority, provider: 'google_ads'});
      if (!connection) return result();
      await store.disconnectGoogle({authority, version: connection.version});
      return result();
    },
  });
}

module.exports = Object.freeze({CONFIRMATION, createGoogleDisconnect});

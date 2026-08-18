'use strict';

const SHARED_QUERY_IDENTITY_RISK = Object.freeze({
  id: 'OAUTH-IDENTITY-001',
  status: 'resolved',
  severity: 'critical',
  description: 'Active OAuth start routes use a verified bearer user and reject the legacy user_id query parameter.'
});

const ROUTES = Object.freeze([
  Object.freeze({provider: 'meta', start: '/auth/meta', callback: '/auth/meta/callback', active: true, transaction: true, pkce: false}),
  Object.freeze({provider: 'google_ads', start: '/auth/google', callback: '/auth/google/callback', active: true, transaction: true, pkce: false}),
  Object.freeze({provider: 'google_sheets', start: '/auth/google-sheets', callback: '/auth/google-sheets/callback', active: true, transaction: true, pkce: false}),
  Object.freeze({provider: 'ga4_organic', start: '/auth/organic', callback: '/auth/organic/callback', active: true, transaction: true, pkce: false}),
  Object.freeze({provider: 'pinterest', start: '/auth/pinterest', callback: '/auth/pinterest/callback', active: false, transaction: false, pkce: false}),
  Object.freeze({provider: 'klaviyo', start: '/auth/klaviyo', callback: '/auth/klaviyo/callback', active: true, transaction: true, pkce: true}),
  Object.freeze({provider: 'tiktok', start: '/auth/tiktok', callback: '/auth/tiktok/callback', active: true, transaction: true, pkce: false})
]);

module.exports = Object.freeze({ROUTES, SHARED_QUERY_IDENTITY_RISK});

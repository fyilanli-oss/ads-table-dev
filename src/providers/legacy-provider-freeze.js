'use strict';

const FROZEN_LEGACY_PLATFORMS = Object.freeze(['meta', 'google', 'klaviyo', 'tiktok', 'pinterest']);
const FROZEN_STANDALONE_OAUTH_ROUTES = Object.freeze(['meta', 'google', 'klaviyo', 'tiktok', 'pinterest']);

function normalize(value) {
  return String(value || '').trim().toLowerCase();
}

function isLegacyProviderRuntimeFrozen(platform) {
  return FROZEN_LEGACY_PLATFORMS.includes(normalize(platform));
}

function isStandaloneOAuthRouteFrozen(provider) {
  return FROZEN_STANDALONE_OAUTH_ROUTES.includes(normalize(provider));
}

function legacyProviderFrozenError(platform, operation = 'write') {
  const error = new Error(`Legacy ${normalize(platform) || 'provider'} ${operation} is frozen by R4-C`);
  error.code = 'LEGACY_PROVIDER_RUNTIME_FROZEN';
  error.status = 409;
  return error;
}

function assertLegacyProviderWriteAllowed(platform) {
  if (isLegacyProviderRuntimeFrozen(platform)) throw legacyProviderFrozenError(platform, 'write');
}

module.exports = {
  FROZEN_LEGACY_PLATFORMS,
  FROZEN_STANDALONE_OAUTH_ROUTES,
  isLegacyProviderRuntimeFrozen,
  isStandaloneOAuthRouteFrozen,
  legacyProviderFrozenError,
  assertLegacyProviderWriteAllowed
};

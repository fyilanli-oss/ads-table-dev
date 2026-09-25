'use strict';

const TOKEN_URL = 'https://a.klaviyo.com/oauth/token';
const DEFAULT_EXPIRY_SKEW_MS = 120000;

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

function tokenExpiresSoon(connection, now, skewMs = DEFAULT_EXPIRY_SKEW_MS) {
  if (!connection?.accessToken) return true;
  if (!connection.accessTokenExpiresAt) return true;
  const expiresAt = new Date(connection.accessTokenExpiresAt).getTime();
  return !Number.isFinite(expiresAt) || expiresAt <= now.getTime() + skewMs;
}

function isAccessTokenError(error) {
  return ['KLAVIYO_ACCESS_TOKEN_INVALID', 'KLAVIYO_REAUTHORIZE'].includes(String(error?.code || error?.message || ''));
}

function parseScopes(value, fallback = []) {
  const raw = Array.isArray(value) ? value : (typeof value === 'string' ? value.split(/[\s,]+/) : fallback);
  return [...new Set(raw.map(scope => typeof scope === 'string' ? scope.trim() : '').filter(Boolean))];
}

function createKlaviyoTokenLifecycle({
  connectionStore,
  fetchImpl = fetch,
  clientId,
  clientSecret,
  now = () => new Date(),
  expirySkewMs = DEFAULT_EXPIRY_SKEW_MS,
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function' ||
    typeof connectionStore.refreshConnectedKlaviyo !== 'function') {
    throw new TypeError('canonical connection store with connected Klaviyo refresh is required');
  }
  if (typeof fetchImpl !== 'function') throw new TypeError('fetchImpl is required');
  const id = required(clientId, 'Klaviyo client id');
  const secret = required(clientSecret, 'Klaviyo client secret');
  const inFlight = new Map();

  async function readConnected(authority) {
    const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
    if (!connection) throw codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED', 409);
    return connection;
  }

  async function performRefresh(authority, connection) {
    if (typeof connection?.refreshToken !== 'string' || !connection.refreshToken.trim()) {
      throw codedError('KLAVIYO_REAUTHORIZE', 409);
    }
    const refreshToken = connection.refreshToken.trim();
    const response = await fetchImpl(TOKEN_URL, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${Buffer.from(`${id}:${secret}`).toString('base64')}`,
        'content-type': 'application/x-www-form-urlencoded',
        accept: 'application/json',
      },
      body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }).toString(),
      redirect: 'error',
      signal: AbortSignal.timeout(20000),
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const providerCode = String(payload?.error || '');
      if (response.status === 400 && providerCode === 'invalid_grant') {
        const winner = await readConnected(authority);
        const connectionChanged = String(winner.version) !== String(connection.version);
        if (connectionChanged && !tokenExpiresSoon(winner, now(), expirySkewMs)) return winner;
        throw codedError('KLAVIYO_REAUTHORIZE', 409);
      }
      if (response.status === 429) throw codedError('KLAVIYO_TOKEN_REFRESH_RATE_LIMITED', 503);
      if (response.status >= 500) throw codedError('KLAVIYO_TOKEN_REFRESH_UNAVAILABLE', 503);
      if (response.status === 401) throw codedError('KLAVIYO_TOKEN_REFRESH_CONFIGURATION_FAILED', 503);
      throw codedError('KLAVIYO_TOKEN_REFRESH_FAILED', 503);
    }
    const accessToken = required(payload?.access_token, 'Klaviyo refreshed access token');
    const expiresIn = Number(payload?.expires_in);
    if (!Number.isSafeInteger(expiresIn) || expiresIn <= 0) throw codedError('KLAVIYO_TOKEN_REFRESH_RESPONSE_INVALID', 503);
    const refreshedAt = now();
    const expiresAt = new Date(refreshedAt.getTime() + (expiresIn * 1000)).toISOString();
    const nextRefreshToken = typeof payload?.refresh_token === 'string' && payload.refresh_token.trim()
      ? payload.refresh_token.trim() : refreshToken;
    const scopes = parseScopes(payload?.scope, connection.grantedScopes);
    try {
      await connectionStore.refreshConnectedKlaviyo({
        authority,
        version: connection.version,
        accessToken,
        refreshToken: nextRefreshToken,
        expiresAt,
        refreshExpiresAt: connection.refreshTokenExpiresAt || null,
        scopes,
      });
    } catch (error) {
      if (String(error?.code || error?.message || '') !== 'CONNECTION_CHANGED') throw error;
      const winner = await readConnected(authority);
      if (tokenExpiresSoon(winner, now(), expirySkewMs)) throw error;
      return winner;
    }
    return readConnected(authority);
  }

  async function refresh(authority, connection) {
    const key = required(authority?.workspace_id, 'authority.workspace_id');
    if (inFlight.has(key)) return inFlight.get(key);
    const promise = performRefresh(authority, connection).finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
    return promise;
  }

  async function run({ authority, connection: connectionInput = null, operation } = {}) {
    if (typeof operation !== 'function') throw new TypeError('operation is required');
    let connection = connectionInput || await readConnected(authority);
    let refreshed = false;
    if (tokenExpiresSoon(connection, now(), expirySkewMs)) {
      connection = await refresh(authority, connection);
      refreshed = true;
    }
    try {
      return Object.freeze({ value: await operation(connection), connection });
    } catch (error) {
      if (!isAccessTokenError(error)) throw error;
      if (refreshed) throw codedError('KLAVIYO_REAUTHORIZE', 409);
      connection = await refresh(authority, connection);
      try {
        return Object.freeze({ value: await operation(connection), connection });
      } catch (retryError) {
        if (isAccessTokenError(retryError)) throw codedError('KLAVIYO_REAUTHORIZE', 409);
        throw retryError;
      }
    }
  }

  return Object.freeze({ run });
}

module.exports = Object.freeze({ TOKEN_URL, DEFAULT_EXPIRY_SKEW_MS, tokenExpiresSoon, createKlaviyoTokenLifecycle });

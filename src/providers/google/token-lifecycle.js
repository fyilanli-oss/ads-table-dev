'use strict';

const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_ADS_SCOPE = 'https://www.googleapis.com/auth/adwords';
const DEFAULT_EXPIRY_SKEW_MS = 120000;

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function codedError(code, status) {
  return Object.assign(new Error(code), {code, status});
}

function versionOf(connection) {
  const version = connection?.version ?? connection?.connection_version;
  if (!Number.isInteger(version) || version < 1) throw codedError('CONNECTION_CHANGED', 409);
  return version;
}

function tokenExpiresSoon(connection, now, skewMs = DEFAULT_EXPIRY_SKEW_MS) {
  if (!connection?.accessToken || !connection?.accessTokenExpiresAt) return true;
  const expiresAt = new Date(connection.accessTokenExpiresAt).getTime();
  return !Number.isFinite(expiresAt) || expiresAt <= now.getTime() + skewMs;
}

function isAccessTokenError(error) {
  return ['PROVIDER_REAUTHORIZE', 'GOOGLE_ACCESS_TOKEN_INVALID', 'GOOGLE_REAUTHORIZE'].includes(String(error?.code || error?.message || ''));
}

function reauthorizeFrom(error) {
  const result = codedError('GOOGLE_REAUTHORIZE', 409);
  result.providerStage = error?.providerStage || 'token_lifecycle';
  result.upstreamStatus = error?.upstreamStatus ?? null;
  result.upstreamCode = error?.upstreamCode || null;
  result.upstreamRequestId = error?.upstreamRequestId || null;
  return result;
}

function scopes(value, fallback = []) {
  const source = Array.isArray(value) ? value : (typeof value === 'string' ? value.split(/[\s,]+/) : fallback);
  return [...new Set(source.map(item => typeof item === 'string' ? item.trim() : '').filter(Boolean))];
}

function createGoogleAdsTokenLifecycle({
  connectionStore,
  fetchImpl = fetch,
  clientId,
  clientSecret,
  now = () => new Date(),
  expirySkewMs = DEFAULT_EXPIRY_SKEW_MS,
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function' ||
    typeof connectionStore.readPendingProvider !== 'function' || typeof connectionStore.refreshGoogle !== 'function') {
    throw new TypeError('canonical connection store with Google refresh is required');
  }
  if (typeof fetchImpl !== 'function') throw new TypeError('fetchImpl is required');
  const id = required(clientId, 'Google client id');
  const secret = required(clientSecret, 'Google client secret');
  const inFlight = new Map();

  async function readCurrent(authority, status) {
    const connection = status === 'pending_account_selection'
      ? await connectionStore.readPendingProvider({authority, provider: 'google_ads'})
      : await connectionStore.resolveConnected({authority, provider: 'google_ads'});
    if (!connection || connection.status !== status) throw codedError('GOOGLE_REAUTHORIZE', 409);
    return connection;
  }

  async function performRefresh(authority, connection) {
    const status = required(connection?.status, 'connection.status');
    if (!['pending_account_selection', 'connected'].includes(status)) throw codedError('GOOGLE_REAUTHORIZE', 409);
    const refreshToken = typeof connection?.refreshToken === 'string' ? connection.refreshToken.trim() : '';
    if (!refreshToken) throw codedError('GOOGLE_REAUTHORIZE', 409);
    const response = await fetchImpl(TOKEN_URL, {
      method: 'POST',
      headers: {'content-type': 'application/x-www-form-urlencoded', accept: 'application/json'},
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_id: id,
        client_secret: secret,
      }).toString(),
      redirect: 'error',
      signal: AbortSignal.timeout(20000),
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      if (response.status === 400 && String(payload?.error || '') === 'invalid_grant') {
        const winner = await readCurrent(authority, status);
        if (versionOf(winner) !== versionOf(connection) && !tokenExpiresSoon(winner, now(), expirySkewMs)) return winner;
        throw codedError('GOOGLE_REAUTHORIZE', 409);
      }
      if (response.status === 429) throw codedError('GOOGLE_TOKEN_REFRESH_RATE_LIMITED', 503);
      if (response.status >= 500) throw codedError('GOOGLE_TOKEN_REFRESH_UNAVAILABLE', 503);
      throw codedError('GOOGLE_TOKEN_REFRESH_FAILED', 503);
    }
    const accessToken = required(payload?.access_token, 'Google refreshed access token');
    const expiresIn = Number(payload?.expires_in);
    if (!Number.isSafeInteger(expiresIn) || expiresIn <= 0) throw codedError('GOOGLE_TOKEN_REFRESH_RESPONSE_INVALID', 503);
    const nextScopes = scopes(payload?.scope, connection.grantedScopes);
    if (!nextScopes.includes(GOOGLE_ADS_SCOPE)) throw codedError('GOOGLE_ADS_SCOPE_MISSING', 409);
    const nextRefreshToken = typeof payload?.refresh_token === 'string' && payload.refresh_token.trim()
      ? payload.refresh_token.trim() : refreshToken;
    const expiresAt = new Date(now().getTime() + (expiresIn * 1000)).toISOString();
    try {
      await connectionStore.refreshGoogle({
        authority,
        version: versionOf(connection),
        status,
        accessToken,
        refreshToken: nextRefreshToken,
        expiresAt,
        scopes: nextScopes,
      });
    } catch (error) {
      if (String(error?.code || error?.message || '') !== 'CONNECTION_CHANGED') throw error;
      const winner = await readCurrent(authority, status);
      if (tokenExpiresSoon(winner, now(), expirySkewMs)) throw error;
      return winner;
    }
    return readCurrent(authority, status);
  }

  async function refresh(authority, connection) {
    const key = `${required(authority?.workspace_id, 'authority.workspace_id')}:${required(connection?.status, 'connection.status')}`;
    if (inFlight.has(key)) return inFlight.get(key);
    const promise = performRefresh(authority, connection).finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
    return promise;
  }

  async function run({authority, connection: connectionInput, operation} = {}) {
    if (!connectionInput) throw new TypeError('connection is required');
    if (typeof operation !== 'function') throw new TypeError('operation is required');
    let connection = connectionInput;
    let refreshed = false;
    if (tokenExpiresSoon(connection, now(), expirySkewMs)) {
      connection = await refresh(authority, connection);
      refreshed = true;
    }
    try {
      return Object.freeze({value: await operation(connection), connection});
    } catch (error) {
      if (!isAccessTokenError(error)) throw error;
      if (refreshed) throw reauthorizeFrom(error);
      connection = await refresh(authority, connection);
      try {
        return Object.freeze({value: await operation(connection), connection});
      } catch (retryError) {
        if (isAccessTokenError(retryError)) throw reauthorizeFrom(retryError);
        throw retryError;
      }
    }
  }

  return Object.freeze({run});
}

module.exports = Object.freeze({TOKEN_URL, GOOGLE_ADS_SCOPE, DEFAULT_EXPIRY_SKEW_MS, tokenExpiresSoon, createGoogleAdsTokenLifecycle});


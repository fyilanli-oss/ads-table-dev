'use strict';

const SAFE_STATUSES = new Set([200, 201]);

function shapeMetadata(value) {
  return Object.freeze({
    kind: Array.isArray(value) ? 'array' : value === null ? 'null' : typeof value,
    keys: value && typeof value === 'object' && !Array.isArray(value)
      ? Object.keys(value).sort().filter((key) => /^[a-z_]{1,40}$/i.test(key)).slice(0, 20) : [],
    arrayLength: Array.isArray(value) ? value.length : null
  });
}

function fail(message, value, safeCode = 'MANAGEMENT_RESPONSE_INVALID') {
  const error = new Error(message);
  error.safeMetadata = shapeMetadata(value);
  error.safeCode = safeCode;
  throw error;
}

function normalizeResponse(status, body) {
  if (!SAFE_STATUSES.has(status)) {
    const safeCode = status === 401 || status === 403
      ? 'MANAGEMENT_AUTH_REJECTED'
      : status >= 400 && status < 500
        ? 'MANAGEMENT_QUERY_REJECTED'
        : 'MANAGEMENT_SERVICE_UNAVAILABLE';
    fail(`Management API rejected request (HTTP ${status})`, null, safeCode);
  }
  let parsed;
  try { parsed = typeof body === 'string' ? JSON.parse(body) : body; }
  catch { fail('Management API returned malformed JSON', null); }
  const wrapped = parsed && typeof parsed === 'object' && !Array.isArray(parsed)
    && Object.keys(parsed).length === 1 && Array.isArray(parsed.result) ? parsed.result : null;
  const candidate = Array.isArray(parsed) ? parsed : wrapped;
  if (candidate && candidate.every((item) => item && typeof item === 'object' && !Array.isArray(item)
    && !Object.hasOwn(item, 'rows') && !Object.hasOwn(item, 'result') && !Object.hasOwn(item, 'command'))) {
    if (candidate.length === 0) fail('Management API response has no result sets', candidate);
    return Object.freeze({ rows: candidate, resultSetCount: 1 });
  }
  const sets = candidate;
  if (!sets) fail('Unknown Management API response wrapper', parsed);
  if (sets.length === 0) fail('Management API response has no result sets', sets);
  const final = sets.at(-1);
  if (!final || typeof final !== 'object' || Array.isArray(final)) fail('Final result set is malformed', final);
  const rows = Array.isArray(final.rows) ? final.rows : Array.isArray(final.result) ? final.result : null;
  if (!rows) fail('Final result set is not SELECT output', final);
  if (rows.length === 0) fail('Final SELECT returned no rows', rows);
  return Object.freeze({ rows, resultSetCount: sets.length });
}

function createManagementClient({ token, projectRef, timeoutMs = 30000, transport = globalThis.fetch }) {
  if (!token || !projectRef || typeof transport !== 'function') throw new Error('Management API configuration is incomplete');
  let requestCount = 0;
  return Object.freeze({
    get requestCount() { return requestCount; },
    async query(sql) {
      requestCount += 1;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await transport(`https://api.supabase.com/v1/projects/${projectRef}/database/query`, {
          method: 'POST', signal: controller.signal,
          headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
          body: JSON.stringify({ query: sql })
        });
        return normalizeResponse(response.status, await response.text());
      } catch (error) {
        if (error && error.name === 'AbortError') {
          const timeout = new Error('Management API request timed out');
          timeout.safeCode = 'MANAGEMENT_TIMEOUT';
          throw timeout;
        }
        if (error && error.safeMetadata) throw error;
        const transportError = new Error('Management API transport failed');
        transportError.safeCode = 'MANAGEMENT_TRANSPORT_FAILED';
        throw transportError;
      } finally { clearTimeout(timer); }
    }
  });
}

module.exports = Object.freeze({ createManagementClient, normalizeResponse, shapeMetadata });

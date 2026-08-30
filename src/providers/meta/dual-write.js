'use strict';

const SAFE_FAILURE_CODES = new Set(['META_TRANSPORT_FAILED', 'META_SERVICE_UNAVAILABLE', 'META_RATE_LIMITED', 'META_AUTH_REJECTED', 'META_REQUEST_REJECTED', 'META_REFRESH_FAILED']);

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function pairKey(userId, accountId) {
  return JSON.stringify([required(userId, 'userId'), required(accountId, 'accountId')]);
}

function buildAllowlist(entries) {
  if (!Array.isArray(entries)) throw new TypeError('Meta dual-write allowlist must be an array');
  const keys = new Set();
  for (const entry of entries) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new TypeError('Meta dual-write allowlist entry must be an object');
    const fields = Object.keys(entry).sort();
    if (fields.join(',') !== 'accountId,userId') throw new Error('Meta dual-write allowlist entry fields are invalid');
    const key = pairKey(entry.userId, entry.accountId);
    if (keys.has(key)) throw new Error('Meta dual-write allowlist contains a duplicate pair');
    keys.add(key);
  }
  return keys;
}

function createMetaDualWriteCoordinator({ enabled = false, allowlist = [], v2Refresh, telemetry = () => {} } = {}) {
  if (enabled !== true && enabled !== false) throw new TypeError('Meta dual-write enabled must be boolean');
  const allowed = buildAllowlist(allowlist);
  if (!v2Refresh || typeof v2Refresh.run !== 'function') throw new TypeError('Meta V2 refresh runner is required');
  if (typeof telemetry !== 'function') throw new TypeError('telemetry must be a function');
  const emit = event => { try { telemetry(Object.freeze(event)); } catch {} };

  return Object.freeze({
    isAllowed(userId, accountId) { return enabled && allowed.has(pairKey(userId, accountId)); },
    async run({ userId, accountId, legacyWrite, v2Input } = {}) {
      const key = pairKey(userId, accountId);
      if (typeof legacyWrite !== 'function') throw new TypeError('legacyWrite is required');
      const legacyResult = await legacyWrite();
      if (!enabled) { emit({ event: 'meta_dual_write_skipped', reason: 'disabled' }); return legacyResult; }
      if (!allowed.has(key)) { emit({ event: 'meta_dual_write_skipped', reason: 'not_allowlisted' }); return legacyResult; }
      if (v2Input?.context?.userId !== userId || v2Input?.accountId !== accountId) {
        emit({ event: 'meta_dual_write_skipped', reason: 'ownership_mismatch' }); return legacyResult;
      }
      try {
        const result = await v2Refresh.run(v2Input);
        emit({ event: 'meta_dual_write_completed', attempts: result?.result?.attempts ?? null, rows_written: result?.result?.write?.persisted ?? null });
      } catch (error) {
        emit({ event: 'meta_dual_write_failed', safe_code: SAFE_FAILURE_CODES.has(error?.safeCode) ? error.safeCode : 'META_DUAL_WRITE_FAILED' });
      }
      return legacyResult;
    }
  });
}

module.exports = Object.freeze({ SAFE_FAILURE_CODES, buildAllowlist, createMetaDualWriteCoordinator, pairKey });

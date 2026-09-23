'use strict';

const { buildEntityKey, validateEntityHierarchy } = require('../../funnel-core/entity-hierarchy');
const { validateWorkspaceCanonicalRow } = require('../../funnel-core/workspace-canonical-contract');

function required(value, field) {
  const clean = typeof value === 'string' ? value.trim() : '';
  if (!clean) throw new TypeError(`${field} is required`);
  return clean;
}

function rowKey(row) {
  return `${row.identity.traffic_type}:${buildEntityKey(row.identity, row.entity)}`;
}

function assertInScope(row, expected) {
  if (
    row.identity.workspace_id !== expected.workspace_id
    || row.identity.platform !== expected.platform
    || row.identity.platform_account_id !== expected.platform_account_id
    || row.time.business_date !== expected.business_date
  ) throw new Error('canonical row is outside workspace checkpoint scope');
}

function createWorkspaceIdempotentBackfillBatchWriter({ writeBoundary } = {}) {
  if (!writeBoundary || typeof writeBoundary.write !== 'function') throw new TypeError('canonical write boundary is required');
  return Object.freeze({
    async write({ checkpoint, rows } = {}) {
      if (!checkpoint || typeof checkpoint !== 'object' || Array.isArray(checkpoint)) throw new TypeError('checkpoint is required');
      if (!Array.isArray(rows)) throw new TypeError('rows must be an array');
      const expected = Object.freeze({
        workspace_id: required(checkpoint.workspace_id, 'checkpoint.workspace_id'),
        platform: required(checkpoint.platform, 'checkpoint.platform').toLowerCase(),
        platform_account_id: required(checkpoint.platform_account_id, 'checkpoint.platform_account_id'),
        business_date: required(checkpoint.business_date, 'checkpoint.business_date')
      });
      if (checkpoint.status !== 'running') throw new Error('checkpoint must be claimed before canonical write');

      const keys = new Set();
      for (const row of rows) {
        validateWorkspaceCanonicalRow(row);
        validateEntityHierarchy(row.identity, row.entity);
        assertInScope(row, expected);
        const key = rowKey(row);
        if (keys.has(key)) throw new Error('duplicate canonical identity inside workspace backfill batch');
        keys.add(key);
      }

      const persisted = await writeBoundary.write(rows);
      if (!Array.isArray(persisted) || persisted.length !== rows.length) throw new Error('canonical workspace backfill write cardinality mismatch');
      const persistedKeys = new Set();
      for (const row of persisted) {
        validateWorkspaceCanonicalRow(row);
        validateEntityHierarchy(row.identity, row.entity);
        assertInScope(row, expected);
        const key = rowKey(row);
        if (persistedKeys.has(key) || !keys.has(key)) throw new Error('canonical workspace backfill write identity mismatch');
        persistedKeys.add(key);
      }
      if (persistedKeys.size !== keys.size) throw new Error('canonical workspace backfill write identity mismatch');
      return Object.freeze({
        checkpoint_identity: expected,
        attempted: rows.length,
        persisted: persisted.length,
        idempotency_conflict: 'workspace_id,platform,platform_account_id,business_date,traffic_type,entity_key'
      });
    }
  });
}

module.exports = Object.freeze({ createWorkspaceIdempotentBackfillBatchWriter, rowKey });

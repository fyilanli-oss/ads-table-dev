'use strict';

const { validateCanonicalRow } = require('../../../funnel-core/canonical-contract');
const { buildEntityKey, validateEntityHierarchy } = require('../../../funnel-core/entity-hierarchy');

function requiredText(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function createMetaDatasetWriter({ adapter, writeBoundary } = {}) {
  if (!adapter || typeof adapter.fetchCanonicalRows !== 'function') throw new TypeError('Meta adapter is required');
  if (!writeBoundary || typeof writeBoundary.write !== 'function') throw new TypeError('canonical write boundary is required');

  return Object.freeze({
    async ingest(input) {
      const userId = requiredText(input?.context?.userId, 'context.userId');
      const accountId = requiredText(input?.accountId, 'accountId');
      if (input?.context?.account?.id !== accountId) throw new Error('Meta write account ownership mismatch');
      const mapped = await adapter.fetchCanonicalRows(input);
      if (!Array.isArray(mapped)) throw new Error('Meta adapter result must be an array');
      const rows = mapped.map((result) => {
        const row = result?.row;
        validateCanonicalRow(row);
        validateEntityHierarchy(row.identity, row.entity);
        if (row.identity.user_id !== userId || row.identity.platform_account_id !== accountId || row.identity.platform !== 'meta') {
          throw new Error('Meta canonical row ownership mismatch');
        }
        const entityKey = buildEntityKey(row.identity, row.entity);
        if (result.entityKey !== entityKey) throw new Error('Meta canonical entity key mismatch');
        return row;
      });
      const persisted = await writeBoundary.write(rows);
      if (!Array.isArray(persisted) || persisted.length !== rows.length) throw new Error('Dataset V2 write result cardinality mismatch');
      return Object.freeze({ attempted: rows.length, persisted: persisted.length, rows: persisted });
    }
  });
}

module.exports = Object.freeze({ createMetaDatasetWriter });

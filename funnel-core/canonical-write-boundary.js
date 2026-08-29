'use strict';

const { validateCanonicalRow } = require('./canonical-contract');
const { validateEntityHierarchy } = require('./entity-hierarchy');

class CanonicalWriteBoundary {
  constructor({ repository }) {
    if (!repository || typeof repository.upsertCanonicalRawFacts !== 'function') throw new TypeError('canonical repository is required');
    this.repository = repository;
  }

  async write(rows) {
    if (!Array.isArray(rows)) throw new TypeError('canonical rows must be an array');
    for (const row of rows) {
      validateCanonicalRow(row);
      validateEntityHierarchy(row.identity, row.entity);
    }
    return this.repository.upsertCanonicalRawFacts(rows);
  }
}

module.exports = { CanonicalWriteBoundary };

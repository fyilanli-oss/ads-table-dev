'use strict';

const { validateEntityHierarchy } = require('./entity-hierarchy');
const {
  validateWorkspaceCanonicalRow,
  cloneWorkspaceCanonicalRow
} = require('./workspace-canonical-contract');

class WorkspaceCanonicalWriteBoundary {
  constructor({ repository } = {}) {
    if (!repository || typeof repository.upsertCanonicalRawFacts !== 'function') {
      throw new Error('WorkspaceCanonicalWriteBoundary requires a dataset repository');
    }
    this.repository = repository;
  }

  async write(rows) {
    if (!Array.isArray(rows)) throw new Error('rows must be an array');
    const validated = rows.map((row) => {
      validateWorkspaceCanonicalRow(row);
      validateEntityHierarchy(row.identity, row.entity);
      return cloneWorkspaceCanonicalRow(row);
    });
    return this.repository.upsertCanonicalRawFacts(validated);
  }
}

module.exports = Object.freeze({ WorkspaceCanonicalWriteBoundary });

'use strict';

const { DatasetRepository } = require('./dataset-repository');
const { buildEntityKey, validateEntityHierarchy } = require('./entity-hierarchy');
const {
  validateWorkspaceCanonicalRow,
  cloneWorkspaceCanonicalRow
} = require('./workspace-canonical-contract');

function workspaceCanonicalUniqueKey(row) {
  validateWorkspaceCanonicalRow(row);
  return [
    row.identity.workspace_id,
    row.identity.platform,
    row.identity.platform_account_id,
    row.time.business_date,
    row.identity.traffic_type,
    buildEntityKey(row.identity, row.entity)
  ].join('|');
}

class WorkspaceInMemoryDatasetRepository extends DatasetRepository {
  constructor(initialRows = []) {
    super();
    this.rows = new Map();
    for (const row of initialRows) this._upsertSync(row);
  }

  _upsertSync(row) {
    validateWorkspaceCanonicalRow(row);
    validateEntityHierarchy(row.identity, row.entity);
    const copy = cloneWorkspaceCanonicalRow(row);
    copy.entity_key = buildEntityKey(copy.identity, copy.entity);
    this.rows.set(workspaceCanonicalUniqueKey(copy), copy);
    return cloneWorkspaceCanonicalRow(copy);
  }

  async upsertCanonicalRawFacts(rows) {
    if (!Array.isArray(rows)) throw new Error('rows must be an array');
    return rows.map((row) => this._upsertSync(row));
  }

  async readCanonicalRawFacts({ workspace_id, from, to, platform = null, platform_account_id = null, traffic_type = null, entity_key = null } = {}) {
    if (!workspace_id) throw new Error('workspace_id is required');
    if (!from || !to) throw new Error('from and to are required');

    return [...this.rows.values()]
      .filter((row) => row.identity.workspace_id === workspace_id)
      .filter((row) => row.time.business_date >= from && row.time.business_date <= to)
      .filter((row) => !platform || row.identity.platform === platform)
      .filter((row) => !platform_account_id || row.identity.platform_account_id === platform_account_id)
      .filter((row) => !traffic_type || row.identity.traffic_type === traffic_type)
      .filter((row) => !entity_key || row.entity_key === entity_key)
      .map(cloneWorkspaceCanonicalRow);
  }
}

module.exports = Object.freeze({
  workspaceCanonicalUniqueKey,
  WorkspaceInMemoryDatasetRepository
});

'use strict';

const { validateCanonicalRow, cloneCanonicalRow } = require('./canonical-contract');
const { validateEntityHierarchy, buildEntityKey } = require('./entity-hierarchy');

class DatasetRepository {
  async upsertCanonicalRawFacts() {
    throw new Error('DatasetRepository.upsertCanonicalRawFacts must be implemented by a persistence adapter');
  }

  async readCanonicalRawFacts() {
    throw new Error('DatasetRepository.readCanonicalRawFacts must be implemented by a persistence adapter');
  }
}

function canonicalUniqueKey(row) {
  const businessDate = row.time.business_date;
  const entityKey = buildEntityKey(row.identity, row.entity);
  return [
    row.identity.user_id,
    row.identity.platform,
    row.identity.platform_account_id,
    businessDate,
    row.identity.traffic_type,
    entityKey
  ].join('|');
}

class InMemoryDatasetRepository extends DatasetRepository {
  constructor(initialRows = []) {
    super();
    this.rows = new Map();
    for (const row of initialRows) this._upsertSync(row);
  }

  _upsertSync(row) {
    validateCanonicalRow(row);
    validateEntityHierarchy(row.identity, row.entity);
    const copy = cloneCanonicalRow(row);
    copy.entity_key = buildEntityKey(copy.identity, copy.entity);
    this.rows.set(canonicalUniqueKey(copy), copy);
    return cloneCanonicalRow(copy);
  }

  async upsertCanonicalRawFacts(rows) {
    if (!Array.isArray(rows)) throw new Error('rows must be an array');
    return rows.map((row) => this._upsertSync(row));
  }

  async readCanonicalRawFacts({ user_id, from, to, platform = null, platform_account_id = null, traffic_type = null, entity_key = null } = {}) {
    if (!user_id) throw new Error('user_id is required');
    if (!from || !to) throw new Error('from and to are required');

    return [...this.rows.values()]
      .filter((row) => row.identity.user_id === user_id)
      .filter((row) => row.time.business_date >= from && row.time.business_date <= to)
      .filter((row) => !platform || row.identity.platform === platform)
      .filter((row) => !platform_account_id || row.identity.platform_account_id === platform_account_id)
      .filter((row) => !traffic_type || row.identity.traffic_type === traffic_type)
      .filter((row) => !entity_key || row.entity_key === entity_key)
      .map(cloneCanonicalRow);
  }
}

module.exports = Object.freeze({
  DatasetRepository,
  InMemoryDatasetRepository,
  canonicalUniqueKey
});

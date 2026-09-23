'use strict';

const { DatasetRepository } = require('./dataset-repository');
const {
  canonicalToDbRow,
  dbToCanonicalRow
} = require('./supabase-dataset-repository');
const {
  WORKSPACE_CANONICAL_CONTRACT_VERSION,
  validateWorkspaceCanonicalRow,
  cloneWorkspaceCanonicalRow
} = require('./workspace-canonical-contract');

const TABLE = 'performance_dataset_rows_v2';
const WORKSPACE_UPSERT_CONFLICT = 'workspace_id,platform,platform_account_id,business_date,traffic_type,entity_key';

function workspaceCanonicalToDbRow(row) {
  validateWorkspaceCanonicalRow(row);
  return {
    ...canonicalToDbRow(row, { requireUserId: false }),
    user_id: row.identity.user_id ?? null,
    workspace_id: row.identity.workspace_id,
    canonical_contract_version: WORKSPACE_CANONICAL_CONTRACT_VERSION
  };
}

function workspaceDbToCanonicalRow(db) {
  if (!db?.workspace_id) throw new Error('Dataset V2 workspace_id is required');
  const row = dbToCanonicalRow(db, { requireUserId: false });
  row.identity.workspace_id = db.workspace_id;
  row.canonical_contract_version = WORKSPACE_CANONICAL_CONTRACT_VERSION;
  validateWorkspaceCanonicalRow(row);
  return row;
}

class WorkspaceSupabaseDatasetRepository extends DatasetRepository {
  constructor(supabaseClient) {
    super();
    if (!supabaseClient || typeof supabaseClient.from !== 'function') {
      throw new Error('A server-side Supabase client is required');
    }
    this.supabase = supabaseClient;
  }

  async upsertCanonicalRawFacts(rows) {
    if (!Array.isArray(rows)) throw new Error('rows must be an array');
    if (rows.length === 0) return [];

    const payload = rows.map(workspaceCanonicalToDbRow);
    const { data, error } = await this.supabase
      .from(TABLE)
      .upsert(payload, { onConflict: WORKSPACE_UPSERT_CONFLICT })
      .select('*');

    if (error) throw new Error(`Dataset V2 workspace upsert failed: ${error.message || error}`);
    return (data || []).map(workspaceDbToCanonicalRow).map(cloneWorkspaceCanonicalRow);
  }

  async readCanonicalRawFacts({ workspace_id, from, to, platform = null, platform_account_id = null, traffic_type = null, entity_key = null } = {}) {
    if (!workspace_id) throw new Error('workspace_id is required');
    if (!from || !to) throw new Error('from and to are required');

    let query = this.supabase
      .from(TABLE)
      .select('*')
      .eq('workspace_id', workspace_id)
      .gte('business_date', from)
      .lte('business_date', to);

    if (platform) query = query.eq('platform', platform);
    if (platform_account_id) query = query.eq('platform_account_id', platform_account_id);
    if (traffic_type) query = query.eq('traffic_type', traffic_type);
    if (entity_key) query = query.eq('entity_key', entity_key);

    const { data, error } = await query.order('business_date', { ascending: true }).order('entity_key', { ascending: true });
    if (error) throw new Error(`Dataset V2 workspace read failed: ${error.message || error}`);
    const rows = (data || []).map(workspaceDbToCanonicalRow);
    if (rows.some((row) => row.identity.workspace_id !== workspace_id)) {
      throw new Error('Dataset V2 cross-workspace read rejected');
    }
    return rows.map(cloneWorkspaceCanonicalRow);
  }
}

module.exports = Object.freeze({
  TABLE,
  WORKSPACE_UPSERT_CONFLICT,
  workspaceCanonicalToDbRow,
  workspaceDbToCanonicalRow,
  WorkspaceSupabaseDatasetRepository
});

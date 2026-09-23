'use strict';

const { WorkspaceCanonicalWriteBoundary } = require('./workspace-canonical-write-boundary');
const { WorkspaceFunnelQueryService } = require('./funnel-query-service');

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CALLER_TENANT_FIELDS = Object.freeze(['workspace_id', 'user_id', 'shop_id']);

function requireServerWorkspaceAuthority(value) {
  if (!value || value.authority !== 'server_resolved_workspace') {
    throw new Error('SERVER_WORKSPACE_AUTHORITY_REQUIRED');
  }
  if (typeof value.workspace_id !== 'string' || !UUID.test(value.workspace_id)) {
    throw new Error('SERVER_WORKSPACE_ID_INVALID');
  }
  if (typeof value.source !== 'string' || value.source.trim() === '') {
    throw new Error('SERVER_WORKSPACE_AUTHORITY_SOURCE_REQUIRED');
  }
  return Object.freeze({
    authority: 'server_resolved_workspace',
    workspace_id: value.workspace_id,
    source: value.source
  });
}

function rejectCallerTenantFields(value, label) {
  if (!value || typeof value !== 'object') return;
  for (const field of CALLER_TENANT_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(value, field)) {
      throw new Error(`${label}.${field} is not tenant authority`);
    }
  }
}

function bindWorkspace(rows, authority) {
  if (!Array.isArray(rows)) throw new Error('rows must be an array');
  return rows.map((row) => {
    if (!row || typeof row !== 'object' || !row.identity || typeof row.identity !== 'object') {
      throw new Error('canonical row identity is required');
    }
    const claimed = row.identity.workspace_id;
    if (claimed !== null && claimed !== undefined && claimed !== authority.workspace_id) {
      throw new Error('CROSS_WORKSPACE_WRITE_REJECTED');
    }
    return {
      ...row,
      identity: { ...row.identity, workspace_id: authority.workspace_id }
    };
  });
}

class WorkspaceDatasetRuntime {
  constructor({ resolveAuthority, repository } = {}) {
    if (typeof resolveAuthority !== 'function') throw new Error('resolveAuthority is required');
    this.resolveAuthority = resolveAuthority;
    this.writeBoundary = new WorkspaceCanonicalWriteBoundary({ repository });
    this.queryService = new WorkspaceFunnelQueryService({ repository });
  }

  async authority(authorityInput) {
    rejectCallerTenantFields(authorityInput, 'authority_input');
    return requireServerWorkspaceAuthority(await this.resolveAuthority(authorityInput));
  }

  async write({ authority_input, rows } = {}) {
    const authority = await this.authority(authority_input);
    return this.writeBoundary.write(bindWorkspace(rows, authority));
  }

  async query({ authority_input, filters = {} } = {}) {
    rejectCallerTenantFields(filters, 'filters');
    const authority = await this.authority(authority_input);
    return this.queryService.query({ ...filters, workspace_id: authority.workspace_id });
  }
}

module.exports = Object.freeze({
  CALLER_TENANT_FIELDS,
  requireServerWorkspaceAuthority,
  rejectCallerTenantFields,
  bindWorkspace,
  WorkspaceDatasetRuntime
});

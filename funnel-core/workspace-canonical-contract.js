'use strict';

const {
  validateCanonicalRow,
  cloneCanonicalRow
} = require('./canonical-contract');

const WORKSPACE_CANONICAL_CONTRACT_VERSION = 'v2';

function requireNonEmpty(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(`${field} must be a non-empty string`);
  return value.trim();
}

function requireUuid(value, field) {
  const clean = requireNonEmpty(value, field);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(clean)) {
    throw new Error(`${field} must be a UUID`);
  }
  return clean;
}

function validateWorkspaceCanonicalRow(row, options = {}) {
  validateCanonicalRow(row, { ...options, requireUserId: false });
  requireUuid(row.identity.workspace_id, 'identity.workspace_id');
  return row;
}

function cloneWorkspaceCanonicalRow(row) {
  validateWorkspaceCanonicalRow(row);
  return cloneCanonicalRow(row);
}

module.exports = Object.freeze({
  WORKSPACE_CANONICAL_CONTRACT_VERSION,
  validateWorkspaceCanonicalRow,
  cloneWorkspaceCanonicalRow
});

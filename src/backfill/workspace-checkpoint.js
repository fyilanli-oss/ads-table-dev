'use strict';

const { STATUSES, transitionCheckpoint, shouldResume } = require('./checkpoint');

function required(value, field) {
  const clean = typeof value === 'string' ? value.trim() : '';
  if (!clean) throw new TypeError(`${field} is required`);
  return clean;
}

function workspaceCheckpointIdentity({ workspace_id, platform, platform_account_id, business_date, date_key } = {}) {
  return Object.freeze({
    workspace_id: required(workspace_id, 'workspace_id'),
    platform: required(platform, 'platform').toLowerCase(),
    platform_account_id: required(platform_account_id, 'platform_account_id'),
    business_date: required(business_date, 'business_date'),
    date_key: required(date_key, 'date_key')
  });
}

function createWorkspaceCheckpoint(unit) {
  const identity = workspaceCheckpointIdentity(unit);
  return Object.freeze({
    ...identity,
    finality: required(unit.finality, 'finality'),
    priority: Number(unit.priority),
    status: 'queued',
    cursor: null,
    attempt_count: 0,
    last_error_code: null
  });
}

module.exports = Object.freeze({
  STATUSES,
  workspaceCheckpointIdentity,
  createWorkspaceCheckpoint,
  transitionCheckpoint,
  shouldResume
});

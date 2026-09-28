'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  WORKSPACE_UPSERT_CONFLICT
} = require('../funnel-core/workspace-supabase-dataset-repository');

const root = path.resolve(__dirname, '..');
const migration = fs.readFileSync(path.join(
  root,
  'supabase/migrations/20260928160500_fix_dataset_v2_workspace_upsert_index.sql'
), 'utf8');

const canonicalColumns = [
  'workspace_id',
  'platform',
  'platform_account_id',
  'business_date',
  'traffic_type',
  'entity_key'
];

test('Dataset V2 workspace upsert key matches the unconditional unique index', () => {
  assert.equal(WORKSPACE_UPSERT_CONFLICT, canonicalColumns.join(','));
  assert.match(
    migration,
    /create unique index performance_dataset_rows_v2_workspace_canonical_uidx\s+on public\.performance_dataset_rows_v2\s+\(workspace_id, platform, platform_account_id, business_date, traffic_type, entity_key\)\s*;/i
  );
  assert.doesNotMatch(migration, /where\s+workspace_id\s+is\s+not\s+null/i);
});

test('Dataset V2 workspace upsert index repair changes no data', () => {
  assert.match(
    migration,
    /drop index if exists public\.performance_dataset_rows_v2_workspace_canonical_uidx/i
  );
  assert.doesNotMatch(migration, /\b(?:insert|update|delete|truncate)\b/i);
  assert.doesNotMatch(migration, /\balter\s+table\b/i);
});

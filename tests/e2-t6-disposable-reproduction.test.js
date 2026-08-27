'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { FIXTURES } = require('../security/e2-t6-static-root-cause');
const { buildEntityKey } = require('../funnel-core/entity-hierarchy');

const root = path.join(__dirname, '..');
const read = (file) => fs.readFileSync(path.join(root, file), 'utf8');
const transaction = read('docs/security/sql/E2_T6_RLS_V3_DISPOSABLE_TRANSACTION.sql');
const evidence = require('../artifacts/dataset-v2-acceptance/e2-t6-rls/disposable-reproduction-v1.json');

test('V3 disposable transaction corrects both canonical fixture keys and uses a new namespace', () => {
  assert.match(transaction, /'operation_code','e2_t6_rls_v3'/);
  assert.doesNotMatch(transaction, /e2_t6_rls_v2:[ab]/);
  for (const fixture of FIXTURES) {
    const canonical = buildEntityKey(
      { ...fixture.identity, platform_account_id: fixture.identity.platform_account_id.replace('_v2_', '_v3_') },
      Object.fromEntries(Object.entries(fixture.entity).map(([key, value]) => [key, typeof value === 'string' ? value.replace(/_v2_/g, '_v3_') : value]))
    );
    assert.match(transaction, new RegExp(canonical));
  }
  assert.match(transaction, /raw->>'fixture_namespace'='e2_t6_rls_v3'/);
});

test('V3 payload aggregate has balanced construction and an explicit evidence source', () => {
  assert.match(transaction, /'cases',\(select jsonb_agg\([\s\S]+from pg_temp\.e2_t6_rls_evidence\n\)\) evidence from summary/);
  assert.match(transaction, /select evidence from payload;\nrollback;\s*$/);
});

test('disposable reproduction evidence is redacted and production-independent', () => {
  const runner = read('scripts/e2-t6-disposable-reproduction.js');
  assert.match(runner, /E2_T6_DISPOSABLE_DATABASE_URL/);
  assert.doesNotMatch(runner, /SUPABASE_|VERCEL_|service_role_key|access_token|refresh_token/);
  assert.equal(evidence.production_operation_run, false);
  assert.equal(evidence.corrected_v3_contract.passed_cases, 16);
  assert.equal(evidence.corrected_v3_contract.overall_passed, true);
  assert.equal(evidence.corrected_v3_contract.outer_rollback, true);
});
